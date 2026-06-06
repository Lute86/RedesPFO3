"""
server.py — Servidor HTTP distribuido (Flask + RabbitMQ + PostgreSQL)
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)

Arquitectura:
  - Flask expone el cliente web (static/client.html) y la API JSON.
  - POST /api/tasks   recibe una tarea, genera task_id, publica en RabbitMQ
                      y devuelve 202 inmediatamente.
  - Pool de workers   (hilos daemon) consume la cola 'tareas', ejecuta la
                      tarea matemática y persiste el resultado en
                      PostgreSQL (tabla task_results).
  - GET  /api/tasks/<id>  devuelve el estado/resultado de una tarea.
                          El cliente hace polling hasta status="done".
  - GET  /api/historial   últimos 10 mensajes de la tabla 'mensajes'.
  - GET  /api/help        describe los comandos disponibles.

Tareas soportadas:
  calc:<expr>     ej: calc:2+2   calc:10*(3+4)   calc:2**8
  factorial:<n>   ej: factorial:10
  sqrt:<n>        ej: sqrt:144
  primes:<n>      ej: primes:50
  fib:<n>         ej: fib:10
  /historial      últimos 10 mensajes (se procesa como una tarea más)

Dependencias (ver requirements.txt):
  flask            — framework HTTP
  pika             — cliente AMQP para RabbitMQ
  psycopg2-binary  — driver PostgreSQL

Servicios externos requeridos:
  RabbitMQ    corriendo en localhost:5672  (credenciales guest/guest)
  PostgreSQL  corriendo en localhost:5432  (db 'pfo3', user/pass postgres/postgres)

Uso:
  python3 server.py [puerto]     # por defecto 5000
"""

import json
import math
import os
import signal
import sys
import threading
import time
import uuid

import pika
import psycopg2
from flask import Flask, jsonify, request, send_from_directory
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor


# ──────────────────────────────────────────────
# Configuración — variables de entorno con fallback
# ──────────────────────────────────────────────
HOST        = os.environ.get("SERVER_HOST", "0.0.0.0")
PORT        = int(os.environ.get("SERVER_PORT", "5000"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "4"))
QUEUE_NAME  = os.environ.get("QUEUE_NAME", "tareas")

RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.environ.get("RABBIT_PORT", "5672"))
RABBIT_USER = os.environ.get("RABBIT_USER", "guest")
RABBIT_PASS = os.environ.get("RABBIT_PASS", "guest")

PG_DSN = (
    f"host={os.environ.get('PG_HOST', 'localhost')} "
    f"port={os.environ.get('PG_PORT', '5432')} "
    f"dbname={os.environ.get('PG_DB', 'pfo3')} "
    f"user={os.environ.get('PG_USER', 'postgres')} "
    f"password={os.environ.get('PG_PASS', 'postgres')}"
)


# ──────────────────────────────────────────────
# Argumentos CLI
# ──────────────────────────────────────────────
if len(sys.argv) > 2:
    print("Uso: python3 server.py [puerto]", file=sys.stderr)
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
# PostgreSQL — connection pool y esquema
# ──────────────────────────────────────────────
def init_db(retries: int = 10, delay: float = 3.0) -> pg_pool.ThreadedConnectionPool:
    """
    Crea las tablas si no existen y devuelve un pool de conexiones.
    Reintenta hasta `retries` veces para tolerar el arranque de Docker.

    Tablas:
      mensajes     — log de todas las tareas (auditoría + /historial)
      task_results — resultado por task_id (correlación cliente ↔ worker)
    """
    for attempt in range(1, retries + 1):
        try:
            pool = pg_pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=NUM_WORKERS + 2,
                dsn=PG_DSN,
            )
            conn = pool.getconn()
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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS task_results (
                        task_id     TEXT PRIMARY KEY,
                        contenido   TEXT        NOT NULL,
                        resultado   TEXT,
                        fecha_envio TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        ip_cliente  TEXT        NOT NULL,
                        worker_id   INTEGER
                    )
                """)
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS task_results_fecha_idx "
                    "ON task_results (fecha_envio DESC)"
                )
            conn.commit()
            pool.putconn(conn)
            print("PostgreSQL: tablas 'mensajes' y 'task_results' listas.")
            return pool
        except psycopg2.Error as e:
            print(f"PostgreSQL no disponible (intento {attempt}/{retries}): {e}",
                  file=sys.stderr)
            if attempt == retries:
                sys.exit(1)
            time.sleep(delay)


def save_result(db_pool,
                task_id: str,
                contenido: str,
                resultado: str,
                ip: str,
                worker_id: int) -> str:
    """
    Inserta (o actualiza) el resultado de una tarea y la entrada de auditoría.
    Devuelve la fecha de procesamiento.
    """
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_results
                    (task_id, contenido, resultado, ip_cliente, worker_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE
                  SET resultado = EXCLUDED.resultado,
                      worker_id = EXCLUDED.worker_id
                """,
                (task_id, contenido, resultado, ip, worker_id),
            )
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


def get_task(db_pool, task_id: str) -> dict | None:
    """Devuelve el resultado de una tarea por task_id, o None si no existe aún."""
    conn = db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT task_id, contenido, resultado, fecha_envio, "
                "ip_cliente, worker_id "
                "FROM task_results WHERE task_id = %s",
                (task_id,),
            )
            return cur.fetchone()
    finally:
        db_pool.putconn(conn)


def get_history(db_pool, limit: int = 10) -> list[tuple]:
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
# RabbitMQ — publicador y consumidores
# ──────────────────────────────────────────────
def make_rabbit_connection(retries: int = 10, delay: float = 3.0):
    """Abre una conexión AMQP a RabbitMQ con reintentos."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=credentials,
        heartbeat=0,
    )
    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            print(f"RabbitMQ no disponible (intento {attempt}/{retries}): {e}",
                  file=sys.stderr)
            if attempt == retries:
                raise
            time.sleep(delay)


# Lock que protege el canal publicador (pika.BlockingConnection NO es
# thread-safe — Flask sirve cada request en su propio hilo).
publisher_lock = threading.Lock()
publisher_channel = None


def publish_task(task_id: str, msg: str, ip: str) -> None:
    """Publica una tarea en la cola 'tareas'."""
    body = json.dumps({"task_id": task_id, "msg": msg, "ip": ip}).encode()
    with publisher_lock:
        publisher_channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),  # persistente
        )


# ──────────────────────────────────────────────
# Motor de tareas matemáticas
# ──────────────────────────────────────────────
def execute_task(msg: str) -> str:
    """
    Interpreta y ejecuta una tarea matemática.

    Tareas soportadas:
      calc:<expr>     ej: calc:2+2   calc:10*(3+4)   calc:2**8
      factorial:<n>   ej: factorial:10
      sqrt:<n>        ej: sqrt:144
      primes:<n>      ej: primes:50
      fib:<n>         ej: fib:10
    """
    msg = msg.strip()
    parts = msg.split(":", 1)

    if len(parts) != 2:
        return (
            f"Tarea desconocida: '{msg}'\n"
            "Formatos: calc:<expr> | factorial:<n> | sqrt:<n> | "
            "primes:<n> | fib:<n>\n"
        )

    cmd, arg = parts[0].strip().lower(), parts[1].strip()

    try:
        if cmd == "calc":
            allowed = set("0123456789+-*/.() ")
            if not all(c in allowed for c in arg):
                return f"Error: expresión no permitida '{arg}'\n"
            resultado = eval(arg, {"__builtins__": {}}, {})
            if isinstance(resultado, float) and resultado.is_integer():
                resultado = int(resultado)
            return f"{arg} = {resultado}\n"

        elif cmd == "factorial":
            n = int(arg)
            if n < 0:    return "Error: factorial no definido para negativos\n"
            if n > 1000: return "Error: n demasiado grande (máx 1000)\n"
            return f"{n}! = {math.factorial(n)}\n"

        elif cmd == "sqrt":
            n = float(arg)
            if n < 0: return "Error: sqrt de número negativo\n"
            return f"√{arg} = {math.sqrt(n)}\n"

        elif cmd == "primes":
            n = int(arg)
            if n < 2:      return "No hay primos menores a 2\n"
            if n > 10_000: return "Error: n demasiado grande (máx 10000)\n"
            sieve = [True] * (n + 1)
            sieve[0] = sieve[1] = False
            for i in range(2, int(n**0.5) + 1):
                if sieve[i]:
                    for j in range(i * i, n + 1, i):
                        sieve[j] = False
            primos = [str(i) for i, v in enumerate(sieve) if v]
            return (f"Primos hasta {n} ({len(primos)} encontrados): "
                    f"{', '.join(primos)}\n")

        elif cmd == "fib":
            n = int(arg)
            if n < 0:    return "Error: índice negativo\n"
            if n > 1000: return "Error: n demasiado grande (máx 1000)\n"
            a, b = 0, 1
            for _ in range(n):
                a, b = b, a + b
            return f"fib({n}) = {a}\n"

        else:
            return (
                f"Operación desconocida: '{cmd}'\n"
                "Disponibles: calc | factorial | sqrt | primes | fib\n"
            )
    except (ValueError, ZeroDivisionError, SyntaxError) as e:
        return f"Error al procesar '{msg}': {e}\n"


# ──────────────────────────────────────────────
# Worker — consume la cola RabbitMQ
# ──────────────────────────────────────────────
def process_task(body: bytes, db_pool, worker_id: int) -> None:
    """
    Decodifica un mensaje de RabbitMQ, ejecuta la tarea y persiste
    el resultado en PostgreSQL.
    """
    task = json.loads(body)
    task_id = task["task_id"]
    msg     = task["msg"]
    ip      = task["ip"]

    if msg.strip().lower() == "/historial":
        rows = get_history(db_pool)
        if not rows:
            respuesta = "Historial vacío.\n"
        else:
            lines = [f"  [{r[0]}] {r[2]}  {r[3]}  w{r[4]}: {r[1]}"
                     for r in rows]
            respuesta = "Últimos mensajes:\n" + "\n".join(lines) + "\n"
    else:
        resultado = execute_task(msg)
        respuesta = f"[Worker-{worker_id}] {resultado}"

    save_result(db_pool, task_id, msg, respuesta, ip, worker_id)


def worker_loop(worker_id: int, db_pool) -> None:
    """
    Cada worker abre su propia conexión a RabbitMQ y consume
    mensajes de la cola 'tareas' con acknowledgment manual.
    """
    print(f"  Worker-{worker_id} iniciado.")
    conn_rmq = make_rabbit_connection()
    channel  = conn_rmq.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)   # un mensaje por vez por worker

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
# Flask — aplicación HTTP
# ──────────────────────────────────────────────
app = Flask(__name__, static_folder="static", static_url_path="/static")
db_pool = None


@app.route("/")
def index():
    """Sirve el cliente web (HTML + CSS + JS)."""
    return send_from_directory("static", "client.html")


@app.route("/client.css")
def client_css():
    """Sirve el CSS del cliente. Path relativo en el HTML para que
    `client.html` también funcione si se abre directamente con
    file:// en el navegador."""
    return send_from_directory("static", "client.css",
                                mimetype="text/css")


@app.route("/client.js")
def client_js():
    """Sirve el JS del cliente. Path relativo en el HTML para que
    `client.html` también funcione si se abre directamente con
    file:// en el navegador."""
    return send_from_directory("static", "client.js",
                                mimetype="application/javascript")


@app.route("/api/health")
def health():
    return jsonify({
        "status":   "ok",
        "workers":  NUM_WORKERS,
        "queue":    QUEUE_NAME,
    })


@app.route("/api/help")
def help_endpoint():
    return jsonify({
        "tareas": {
            "calc:<expr>":    "evalúa una expresión (ej: calc:2+2)",
            "factorial:<n>":  "factorial de n (ej: factorial:10)",
            "sqrt:<n>":       "raíz cuadrada (ej: sqrt:144)",
            "primes:<n>":     "primos hasta n (ej: primes:50)",
            "fib:<n>":        "n-ésimo Fibonacci (ej: fib:10)",
        },
        "especiales": {
            "/historial": "últimos 10 mensajes (se procesa como tarea)",
        },
        "flujo": (
            "POST /api/tasks {msg} → task_id → "
            "GET /api/tasks/<id> hasta status='done'"
        ),
    })


@app.route("/api/tasks", methods=["POST"])
def submit_task():
    """
    Recibe una tarea, la publica en RabbitMQ y devuelve
    inmediatamente el task_id (status 202 Accepted).
    """
    data = request.get_json(silent=True) or {}
    msg  = (data.get("msg") or "").strip()
    if not msg:
        return jsonify({"error": "campo 'msg' requerido"}), 400

    task_id = str(uuid.uuid4())
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "?")
    publish_task(task_id, msg, ip)
    return jsonify({"task_id": task_id, "status": "pending"}), 202


@app.route("/api/tasks/<task_id>")
def get_task_endpoint(task_id):
    """
    Devuelve el estado actual de una tarea. El cliente hace polling
    de este endpoint hasta que status='done'.
    """
    row = get_task(db_pool, task_id)
    if not row:
        # No publicada todavía, o en cola en RabbitMQ
        return jsonify({"task_id": task_id, "status": "pending"}), 404
    payload = {
        "task_id":   row["task_id"],
        "contenido": row["contenido"],
        "status":    "done" if row["resultado"] is not None else "pending",
        "resultado": row["resultado"],
        "worker":    row["worker_id"],
        "fecha":     row["fecha_envio"].isoformat(timespec="seconds")
                    if row["fecha_envio"] else None,
        "ip":        row["ip_cliente"],
    }
    return jsonify(payload)


@app.route("/api/historial")
def historial():
    rows = get_history(db_pool, limit=10)
    return jsonify([
        {
            "id":     r[0],
            "msg":    r[1],
            "fecha":  r[2].isoformat(timespec="seconds"),
            "ip":     r[3],
            "worker": r[4],
        } for r in rows
    ])


# ──────────────────────────────────────────────
# Cierre limpio
# ──────────────────────────────────────────────
def shutdown_server(signum, frame) -> None:
    print("\nCerrando servidor...")
    if publisher_channel:
        try:
            publisher_channel.connection.close()
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

    # 2. Conexión publicadora (compartida entre los request threads de Flask)
    pub_conn = make_rabbit_connection()
    publisher_channel = pub_conn.channel()
    publisher_channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"RabbitMQ: cola '{QUEUE_NAME}' lista.")

    # 3. Arrancar workers (cada uno con su propia conexión a RabbitMQ)
    for wid in range(1, NUM_WORKERS + 1):
        t = threading.Thread(target=worker_loop, args=(wid, db_pool), daemon=True)
        t.start()

    # 4. Arrancar Flask
    print(f"Servidor Flask escuchando en {HOST}:{PORT}  "
          f"({NUM_WORKERS} workers activos)")
    print(f"Cliente web: http://localhost:{PORT}/")
    print(f"Comandos: calc:<expr> | factorial:<n> | sqrt:<n> | "
          f"primes:<n> | fib:<n> | /historial")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
