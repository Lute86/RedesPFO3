"""
cliente.py — Cliente para el servidor distribuido
PFO 3: Rediseño como Sistema Distribuido

El cliente no interactúa directamente con RabbitMQ ni PostgreSQL;
se comunica exclusivamente con el servidor vía socket TCP.

Uso:
  python3 cliente.py                        # conecta a 127.0.0.1:5000
  python3 cliente.py <host> <puerto>
"""

import socket
import sys
import threading
import ipaddress

HOST = "127.0.0.1"
PORT = 5000

# ──────────────────────────────────────────────
# Argumentos
# ──────────────────────────────────────────────
if len(sys.argv) == 1:
    pass

elif len(sys.argv) == 3:
    host_input = sys.argv[1]
    try:
        ipaddress.ip_address(host_input)
        HOST = host_input
    except ValueError:
        try:
            HOST = socket.gethostbyname(host_input)
        except socket.gaierror:
            print("Host inválido", file=sys.stderr)
            sys.exit(1)

    try:
        PORT = int(sys.argv[2])
        if not (0 <= PORT <= 65535):
            raise ValueError
    except ValueError:
        print("Puerto inválido", file=sys.stderr)
        sys.exit(1)

else:
    print("Uso: python3 cliente.py  o  python3 cliente.py <host> <puerto>",
          file=sys.stderr)
    sys.exit(1)

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
            print(f"  '{raw}' no es un puerto válido.")


def connect(host: str, port: int) -> tuple[socket.socket, int]:
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            return s, port
        except Exception as e:
            print(f"\nNo se pudo conectar a {host}:{port} — {e}")
            print("  1. Intentar con otro puerto")
            print("  2. Cerrar")
            choice = input("Opción: ").strip()
            if choice == "1":
                port = ask_port()
            elif choice == "2":
                sys.exit(0)


def print_help() -> None:
    print(
        "\nComandos disponibles:\n"
        "  /historial   — muestra los últimos mensajes guardados en PostgreSQL\n"
        "  éxito        — cierra la conexión y sale\n"
        "  Ctrl+C       — cierre forzado\n"
        "  (cualquier otro texto se publica en RabbitMQ via el servidor)\n"
    )

# ──────────────────────────────────────────────
# Conexión
# ──────────────────────────────────────────────
sock, PORT = connect(HOST, PORT)
print(f"Conectado a {HOST}:{PORT}.")
print_help()

disconnected = threading.Event()

# ──────────────────────────────────────────────
# Hilo receptor
# ──────────────────────────────────────────────
def receive() -> None:
    with sock.makefile("r", encoding="utf-8") as reader:
        for line in reader:
            print(line, end="", flush=True)
    print("\nServidor desconectado.")
    disconnected.set()

threading.Thread(target=receive, daemon=True).start()

# ──────────────────────────────────────────────
# Loop de envío
# ──────────────────────────────────────────────
while not disconnected.is_set():
    try:
        msg = input()
    except (EOFError, KeyboardInterrupt):
        print("\nSaliendo...")
        break

    if disconnected.is_set():
        break

    if msg.strip().lower() == "éxito":
        try:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except OSError:
            pass
        break

    try:
        sock.sendall((msg + "\n").encode("utf-8"))
    except OSError:
        break
