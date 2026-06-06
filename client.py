"""
client.py — Cliente Python alternativo para el servidor Flask
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)

Funcionalidades:
  - Envía tareas vía HTTP al endpoint POST /api/tasks
  - Hace polling de GET /api/tasks/<id> hasta obtener el resultado
  - Soporta /historial para ver los últimos 10 mensajes
  - Cierre limpio con 'éxito' o Ctrl+C

Tareas soportadas (mismas que el servidor):
  calc:<expr>     ej: calc:2+2   calc:10*(3+4)   calc:2**8
  factorial:<n>   ej: factorial:10
  sqrt:<n>        ej: sqrt:144
  primes:<n>      ej: primes:50
  fib:<n>         ej: fib:10

Dependencias:
  requests   — cliente HTTP

Uso:
  python3 client.py                        # conecta a http://127.0.0.1:5000
  python3 client.py <host> <puerto>        # ej: python3 client.py mi-host 5000
"""

import sys
import time

import requests


# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 5000
BASE_URL = f"http://{HOST}:{PORT}"
POLL_INTERVAL = 0.5
POLL_TIMEOUT  = 30.0


# ──────────────────────────────────────────────
# Argumentos
# ──────────────────────────────────────────────
if len(sys.argv) == 1:
    pass

elif len(sys.argv) == 3:
    HOST = sys.argv[1]
    try:
        PORT = int(sys.argv[2])
        if not (0 <= PORT <= 65535):
            raise ValueError
    except ValueError:
        print("Puerto inválido", file=sys.stderr)
        sys.exit(1)
    BASE_URL = f"http://{HOST}:{PORT}"

else:
    print("Uso: python3 client.py  o  python3 client.py <host> <puerto>",
          file=sys.stderr)
    sys.exit(1)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def print_help() -> None:
    print(
        "\nComandos disponibles:\n"
        "  calc:<expr>     — evalúa una expresión (ej: calc:2+2)\n"
        "  factorial:<n>   — factorial (ej: factorial:10)\n"
        "  sqrt:<n>        — raíz cuadrada (ej: sqrt:144)\n"
        "  primes:<n>      — primos hasta n (ej: primes:50)\n"
        "  fib:<n>         — n-ésimo Fibonacci (ej: fib:10)\n"
        "  /historial      — últimos 10 mensajes\n"
        "  éxito           — cierra y sale\n"
        "  Ctrl+C          — cierre forzado\n"
    )


def connect(base_url: str) -> str:
    """Comprueba que el servidor esté vivo. Reintenta hasta conectar."""
    while True:
        try:
            r = requests.get(f"{base_url}/api/health", timeout=3)
            r.raise_for_status()
            data = r.json()
            print(f"Conectado a {base_url}  "
                  f"({data.get('workers')} workers · cola '{data.get('queue')}').")
            return base_url
        except Exception as e:
            print(f"\nNo se pudo conectar a {base_url} — {e}")
            print("  1. Reintentar")
            print("  2. Cerrar")
            choice = input("Opción: ").strip()
            if choice == "2":
                sys.exit(0)


def submit_task(base_url: str, msg: str) -> str:
    """Envía una tarea y devuelve el task_id."""
    r = requests.post(
        f"{base_url}/api/tasks",
        json={"msg": msg},
        timeout=5,
    )
    r.raise_for_status()
    return r.json()["task_id"]


def wait_for_result(base_url: str, task_id: str) -> dict:
    """Hace polling hasta que la tarea esté 'done'. Lanza si pasa el timeout."""
    started = time.monotonic()
    short_id = task_id[:8]
    while True:
        r = requests.get(f"{base_url}/api/tasks/{task_id}", timeout=5)
        if r.status_code == 404:
            # Aún en cola
            pass
        elif r.ok:
            data = r.json()
            if data.get("status") == "done":
                return data
        else:
            r.raise_for_status()

        if time.monotonic() - started > POLL_TIMEOUT:
            raise TimeoutError(f"timeout esperando resultado de {short_id}…")
        time.sleep(POLL_INTERVAL)


def print_task_result(data: dict) -> None:
    meta = f"Worker-{data.get('worker')} | {data.get('fecha')}"
    print(f"[{meta}] {data.get('resultado', '').rstrip()}")


def show_history(base_url: str) -> None:
    """Procesa /historial como una tarea normal (sigue el flujo distribuido)."""
    task_id = submit_task(base_url, "/historial")
    data = wait_for_result(base_url, task_id)
    print()
    print(data.get("resultado", "").rstrip())


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main() -> None:
    base_url = connect(BASE_URL)
    print_help()

    while True:
        try:
            msg = input()
        except (EOFError, KeyboardInterrupt):
            print("\nSaliendo...")
            break

        msg = msg.strip()
        if not msg:
            continue
        if msg.lower() == "éxito":
            print("Chau.")
            break

        try:
            if msg == "/historial":
                show_history(base_url)
            else:
                task_id = submit_task(base_url, msg)
                data = wait_for_result(base_url, task_id)
                print_task_result(data)
        except requests.RequestException as e:
            print(f"[error de red] {e}")
        except TimeoutError as e:
            print(f"[timeout] {e}")
        except Exception as e:
            print(f"[error] {e}")


if __name__ == "__main__":
    main()
