"""
cliente.py — Cliente para el servidor distribuido
PFO 3: Rediseño como Sistema Distribuido

Funcionalidades:
  - Envía tareas al servidor y muestra las respuestas de los workers
  - Soporta el comando /historial para consultar mensajes previos
  - Menú de reintento si la conexión falla
  - Salida limpia con 'éxito' o Ctrl+C

Uso:
  python3 cliente.py                        # conecta a 127.0.0.1:5000
  python3 cliente.py <host> <puerto>        # host puede ser IP o nombre
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
    pass  # usa HOST y PORT por defecto

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
    """Solicita un puerto válido por teclado, reintentando si es inválido."""
    while True:
        raw = input("Nuevo puerto (0-65535): ").strip()
        try:
            port = int(raw)
            if 0 <= port <= 65535:
                return port
            raise ValueError
        except ValueError:
            print(f"  '{raw}' no es un puerto válido, ingresá un entero entre 0 y 65535.")


def connect(host: str, port: int) -> tuple[socket.socket, int]:
    """Intenta conectar; ofrece menú de reintento si falla."""
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
        "  /historial   — muestra los últimos mensajes procesados\n"
        "  éxito        — cierra la conexión y sale\n"
        "  Ctrl+C       — cierre forzado\n"
        "  (cualquier otro texto se envía como tarea al servidor)\n"
    )

# ──────────────────────────────────────────────
# Conexión
# ──────────────────────────────────────────────
sock, PORT = connect(HOST, PORT)
print(f"Conectado a {HOST}:{PORT}.")
print_help()

# Evento que indica que el servidor cerró la conexión
disconnected = threading.Event()

# ──────────────────────────────────────────────
# Hilo receptor — muestra respuestas de los workers
# ──────────────────────────────────────────────
def receive() -> None:
    """Lee líneas del servidor e imprime la respuesta del worker."""
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

    # Cierre limpio por palabra clave
    if msg.strip().lower() == "éxito":
        try:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except OSError:
            pass
        break

    # Enviar tarea (incluye comandos como /historial)
    try:
        sock.sendall((msg + "\n").encode("utf-8"))
    except OSError:
        break
