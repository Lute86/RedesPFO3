"""
servidor.py — Servidor distribuido con workers y cola de tareas
PFO 3: Rediseño como Sistema Distribuido

Arquitectura:
  - Acepta conexiones TCP de múltiples clientes simultáneamente
  - Distribuye cada tarea a un pool de threads (workers)
  - Cola interna (queue.Queue) simula la capa de mensajería
  - Los workers ejecutan la tarea matemática y devuelven el resultado
  - Persiste mensajes en SQLite (representa capa de BD)
  - Soporta un "comando" especial para consultar el historial

Tareas soportadas:
  calc:<expr>     ej: calc:2+2   calc:10*(3+4)   calc:2**8
  factorial:<n>   ej: factorial:10
  sqrt:<n>        ej: sqrt:144
  primes:<n>      ej: primes:50
  fib:<n>         ej: fib:10

Uso:
  python3 servidor.py [puerto]          # por defecto 5000
"""

import math
import socket
import sqlite3
import sys
import signal
import threading
import queue
from datetime import datetime

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 5000
NUM_WORKERS = 4          # cantidad de threads worker en el pool
TASK_QUEUE = queue.Queue()   # cola central de tareas (simula RabbitMQ)

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
# Base de datos (capa de almacenamiento)
# ──────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    """Inicializa SQLite y devuelve la conexión."""
    try:
        conn = sqlite3.connect("mensajes.db", check_same_thread=False)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mensajes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                contenido   TEXT    NOT NULL,
                fecha_envio TEXT    NOT NULL,
                ip_cliente  TEXT    NOT NULL,
                worker_id   INTEGER NOT NULL
            )
        """)
        conn.commit()
        return conn
    except sqlite3.Error as e:
        print(f"Error DB: {e}", file=sys.stderr)
        sys.exit(1)

db_lock = threading.Lock()

def save_message(conn: sqlite3.Connection,
                 contenido: str,
                 ip: str,
                 worker_id: int) -> str:
    """Guarda un mensaje y devuelve la fecha de inserción."""
    fecha = datetime.now().isoformat(timespec="seconds")
    with db_lock:
        conn.execute(
            "INSERT INTO mensajes (contenido, fecha_envio, ip_cliente, worker_id) "
            "VALUES (?, ?, ?, ?)",
            (contenido, fecha, ip, worker_id),
        )
        conn.commit()
    return fecha

def get_history(conn: sqlite3.Connection, limit: int = 10) -> list[tuple]:
    """Devuelve los últimos `limit` mensajes almacenados."""
    with db_lock:
        cur = conn.execute(
            "SELECT id, contenido, fecha_envio, ip_cliente, worker_id "
            "FROM mensajes ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()

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
# Procesamiento de una tarea por parte de un worker
# ──────────────────────────────────────────────
def process_task(task: dict, conn: sqlite3.Connection, worker_id: int) -> str:
    """
    Recibe una tarea (dict con claves 'msg' e 'ip') y la procesa.
    Devuelve la respuesta que se enviará al cliente.
    """
    msg = task["msg"]
    ip  = task["ip"]

    # Comando especial: historial
    if msg.strip().lower() == "/historial":
        rows = get_history(conn)
        if not rows:
            return "Historial vacío.\n"
        lines = [f"  [{r[0]}] {r[2]}  {r[3]}  w{r[4]}: {r[1]}" for r in rows]
        return "Últimos mensajes:\n" + "\n".join(lines) + "\n"

    # Tarea normal: ejecutar y guardar
    resultado = execute_task(msg)
    fecha = save_message(conn, msg, ip, worker_id)
    return f"[Worker-{worker_id} | {fecha}] {resultado}"

# ──────────────────────────────────────────────
# Worker — consume tareas de la cola
# ──────────────────────────────────────────────
def worker_loop(worker_id: int, conn: sqlite3.Connection) -> None:
    """Loop del worker: toma tareas de TASK_QUEUE y responde al cliente."""
    print(f"  Worker-{worker_id} iniciado.")
    while True:
        task = TASK_QUEUE.get()          # bloquea hasta haber tarea
        if task is None:                 # señal de cierre
            TASK_QUEUE.task_done()
            break
        try:
            respuesta = process_task(task, conn, worker_id)
            sock: socket.socket = task["sock"]
            sock.sendall(respuesta.encode("utf-8"))
        except OSError:
            pass                         # cliente ya desconectado
        finally:
            TASK_QUEUE.task_done()

# ──────────────────────────────────────────────
# Receptor de cliente — encola sus mensajes
# ──────────────────────────────────────────────
def handle_client(sock: socket.socket, addr: tuple) -> None:
    """
    Lee líneas del cliente y las encola como tareas para los workers.
    Cada cliente corre en su propio hilo de I/O.
    """
    ip = addr[0]
    print(f"Cliente conectado: {ip}:{addr[1]}")
    with sock, sock.makefile("r", encoding="utf-8") as reader:
        for line in reader:
            msg = line.rstrip("\n")
            if not msg:
                continue
            TASK_QUEUE.put({"msg": msg, "ip": ip, "sock": sock})
    print(f"Cliente desconectado: {ip}:{addr[1]}")

# ──────────────────────────────────────────────
# Validación de puerto interactiva
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
            print(f"  '{raw}' no es un puerto válido.")

# ──────────────────────────────────────────────
# Inicio del socket servidor
# ──────────────────────────────────────────────
def init_socket(port: int) -> tuple[socket.socket, int]:
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, port))
            s.listen()
            print(f"Servidor escuchando en {HOST}:{port}  ({NUM_WORKERS} workers activos)")
            print("Comandos: calc:<expr> | factorial:<n> | sqrt:<n> | "
                  "primes:<n> | fib:<n> | /historial")
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

def shutdown_server(signum, frame) -> None:
    print("\nCerrando servidor...")
    # Señal de cierre para cada worker
    for _ in range(NUM_WORKERS):
        TASK_QUEUE.put(None)
    if server_socket:
        server_socket.close()
    sys.exit(0)

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_server)

    db_conn = init_db()

    # Arrancar el pool de workers
    for wid in range(1, NUM_WORKERS + 1):
        t = threading.Thread(target=worker_loop, args=(wid, db_conn), daemon=True)
        t.start()

    server_socket, PORT = init_socket(PORT)

    # Bucle principal: aceptar clientes
    while True:
        try:
            client_sock, addr = server_socket.accept()
        except OSError:
            break   # servidor cerrado por señal

        # Hilo de I/O por cliente (liviano: solo encola mensajes)
        t = threading.Thread(target=handle_client, args=(client_sock, addr), daemon=True)
        t.start()
