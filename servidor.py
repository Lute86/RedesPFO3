"""
servidor.py — Servidor distribuido con RabbitMQ y PostgreSQL
PFO 3: Rediseño como Sistema Distribuido

Arquitectura:
  - Acepta conexiones TCP de múltiples clientes simultáneamente
  - Publica cada mensaje en RabbitMQ (cola 'tareas')
  - Pool de workers consume la cola y persiste en PostgreSQL
  - Soporta el comando /historial para consultar la BD

Dependencias (ver requirements.txt):
  pika       — cliente AMQP para RabbitMQ
  psycopg2   — driver PostgreSQL

Servicios externos requeridos:
  RabbitMQ   corriendo en localhost:5672
  PostgreSQL corriendo en localhost:5432

Uso:
  python3 servidor.py [puerto]     # por defecto 5000
"""

import socket
import sys
import signal
import threading
import json
from datetime import datetime

import pika
import psycopg2
from psycopg2 import pool as pg_pool

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
HOST       = "127.0.0.1"
PORT       = 5000
NUM_WORKERS = 4
QUEUE_NAME  = "tareas"

# RabbitMQ
RABBIT_HOST = "localhost"
RABBIT_PORT = 5672
RABBIT_USER = "guest"
RABBIT_PASS = "guest"

# PostgreSQL
PG_DSN = "host=localhost port=5432 dbname=pfo3 user=postgres password=postgres"

# ──────────────────────────────────────────────
# Argumentos
# ──────────────────────────────────────────────
if len(sys.argv) > 2:
    print("Uso: python3 servidor.py [puerto]", file=sys.stderr)
    sys.exit(1)

if len(sys.argv) == 2:
    try:
        PORT = int(sys.argv[1])
        if not (0 <= PORT <= 65535):
            raise ValueError
    except ValueError:
        print("Puerto inválido (debe ser un entero entre 0 y 65535)", file=sys.stderr)
        sys.exit(1)

# ──────────────────────────────────────────────
# PostgreSQL — connection pool
# ──────────────────────────────────────────────
def init_db() -> pg_pool.ThreadedConnectionPool:
    """
    Crea la tabla 'mensajes' si no existe y devuelve un pool
    de conexiones PostgreSQL (mínimo 1, máximo NUM_WORKERS+2).
    """
    try:
        db_pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=NUM_WORKERS + 2,
            dsn=PG_DSN,
        )
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mensajes (
                    id          SERIAL PRIMARY KEY,
                    contenido   TEXT        NOT NULL,
                    fecha_envio TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ip_cliente  TEXT        NOT NULL,
                    worker_id   INTEGER     NOT NULL
                )
            """)
        conn.commit()
        db_pool.putconn(conn)
        print("PostgreSQL: tabla 'mensajes' lista.")
        return db_pool
    except psycopg2.Error as e:
        print(f"Error PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)


def save_message(db_pool: pg_pool.ThreadedConnectionPool,
                 contenido: str,
                 ip: str,
                 worker_id: int) -> str:
    """Inserta un mensaje y devuelve la fecha de inserción."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mensajes (contenido, ip_cliente, worker_id) "
                "VALUES (%s, %s, %s) RETURNING fecha_envio",
                (contenido, ip, worker_id),
            )
            fecha = cur.fetchone()[0].isoformat(timespec="seconds")
        conn.commit()
        return fecha
    finally:
        db_pool.putconn(conn)


def get_history(db_pool: pg_pool.ThreadedConnectionPool,
                limit: int = 10) -> list[tuple]:
    """Devuelve los últimos `limit` mensajes de PostgreSQL."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, contenido, fecha_envio, ip_cliente, worker_id "
                "FROM mensajes ORDER BY id DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()
    finally:
        db_pool.putconn(conn)

# ──────────────────────────────────────────────
# RabbitMQ — publicar una tarea
# ──────────────────────────────────────────────
def make_rabbit_connection() -> pika.BlockingConnection:
    """Abre una conexión AMQP a RabbitMQ."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def publish_task(channel: pika.adapters.blocking_connection.BlockingChannel,
                 msg: str,
                 ip: str,
                 sock: socket.socket) -> None:
    """
    Publica un mensaje JSON en la cola RabbitMQ.
    'sock' se serializa como fileno para recuperarlo en el worker.
    """
    body = json.dumps({"msg": msg, "ip": ip, "fd": sock.fileno()})
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=body.encode(),
        properties=pika.BasicProperties(delivery_mode=2),   # persistente
    )

# ──────────────────────────────────────────────
# Worker — consume la cola RabbitMQ
# ──────────────────────────────────────────────
# Mapa fd → socket para que los workers puedan responder al cliente
fd_to_sock: dict[int, socket.socket] = {}
fd_lock = threading.Lock()


def process_task(body: bytes,
                 db_pool: pg_pool.ThreadedConnectionPool,
                 worker_id: int) -> None:
    """
    Decodifica un mensaje de RabbitMQ, lo procesa, persiste en
    PostgreSQL y envía la respuesta al socket del cliente.
    """
    task = json.loads(body)
    msg = task["msg"]
    ip  = task["ip"]
    fd  = task["fd"]

    # Recuperar el socket original
    with fd_lock:
        sock = fd_to_sock.get(fd)

    if msg.strip().lower() == "/historial":
        rows = get_history(db_pool)
        if not rows:
            respuesta = "Historial vacío.\n"
        else:
            lines = [f"  [{r[0]}] {r[2]}  {r[3]}  w{r[4]}: {r[1]}" for r in rows]
            respuesta = "Últimos mensajes:\n" + "\n".join(lines) + "\n"
    else:
        fecha = save_message(db_pool, msg, ip, worker_id)
        respuesta = f"[Worker-{worker_id}] Guardado en PostgreSQL {fecha}: {msg}\n"

    if sock:
        try:
            sock.sendall(respuesta.encode("utf-8"))
        except OSError:
            pass


def worker_loop(worker_id: int,
                db_pool: pg_pool.ThreadedConnectionPool) -> None:
    """
    Cada worker abre su propia conexión a RabbitMQ y consume
    mensajes de la cola 'tareas' con acknowledgment manual.
    """
    print(f"  Worker-{worker_id} iniciado.")
    conn_rmq = make_rabbit_connection()
    channel  = conn_rmq.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)    # un mensaje por vez por worker

    def on_message(ch, method, _props, body):
        process_task(body, db_pool, worker_id)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
    try:
        channel.start_consuming()
    except Exception:
        pass
    finally:
        try:
            conn_rmq.close()
        except Exception:
            pass

# ──────────────────────────────────────────────
# Receptor de cliente — publica en RabbitMQ
# ──────────────────────────────────────────────
def handle_client(sock: socket.socket,
                  addr: tuple,
                  pub_channel) -> None:
    """
    Lee líneas del cliente y las publica en RabbitMQ.
    Registra el socket en fd_to_sock para que los workers respondan.
    """
    ip = addr[0]
    fd = sock.fileno()
    print(f"Cliente conectado: {ip}:{addr[1]}")

    with fd_lock:
        fd_to_sock[fd] = sock

    with sock, sock.makefile("r", encoding="utf-8") as reader:
        for line in reader:
            msg = line.rstrip("\n")
            if not msg:
                continue
            try:
                publish_task(pub_channel, msg, ip, sock)
            except Exception as e:
                print(f"Error publicando en RabbitMQ: {e}", file=sys.stderr)

    with fd_lock:
        fd_to_sock.pop(fd, None)
    print(f"Cliente desconectado: {ip}:{addr[1]}")

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def ask_port() -> int:
    while True:
        raw = input("Nuevo puerto (0-65535): ").strip()
        try:
            port = int(raw)
            if 0 <= port <= 65535:
                return port
            raise ValueError
        except ValueError:
            print(f"  '{raw}' no es válido.")


def init_socket(port: int) -> tuple[socket.socket, int]:
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, port))
            s.listen()
            print(f"Servidor escuchando en {HOST}:{port}  ({NUM_WORKERS} workers activos)")
            return s, port
        except OSError as e:
            print(f"\nNo se pudo iniciar en puerto {port}: {e}")
            print("  1. Intentar con otro puerto")
            print("  2. Cerrar")
            choice = input("Opción: ").strip()
            if choice == "1":
                port = ask_port()
            elif choice == "2":
                sys.exit(0)

# ──────────────────────────────────────────────
# Cierre limpio
# ──────────────────────────────────────────────
server_socket: socket.socket | None = None
pub_conn_global = None

def shutdown_server(signum, frame) -> None:
    print("\nCerrando servidor...")
    if server_socket:
        server_socket.close()
    if pub_conn_global:
        try:
            pub_conn_global.close()
        except Exception:
            pass
    sys.exit(0)

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_server)

    # 1. Inicializar PostgreSQL
    db_pool = init_db()

    # 2. Conexión publicadora (hilo principal → RabbitMQ)
    pub_conn_global = make_rabbit_connection()
    pub_channel = pub_conn_global.channel()
    pub_channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"RabbitMQ: cola '{QUEUE_NAME}' lista.")

    # 3. Arrancar workers (cada uno con su propia conexión a RabbitMQ)
    for wid in range(1, NUM_WORKERS + 1):
        t = threading.Thread(target=worker_loop, args=(wid, db_pool), daemon=True)
        t.start()

    # 4. Arrancar socket TCP
    server_socket, PORT = init_socket(PORT)

    # 5. Aceptar clientes
    while True:
        try:
            client_sock, addr = server_socket.accept()
        except OSError:
            break
        t = threading.Thread(
            target=handle_client,
            args=(client_sock, addr, pub_channel),
            daemon=True,
        )
        t.start()
