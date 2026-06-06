#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# start.sh — Arranca el sistema PFO3 sin Docker (Linux / macOS)
#
# Qué hace:
#   1. Verifica que Python3, RabbitMQ y PostgreSQL estén disponibles
#   2. Crea el virtualenv e instala dependencias si hace falta
#   3. Crea la base de datos 'pfo3' en PostgreSQL si no existe
#   4. Lanza dos instancias de servidor.py en background (puertos 5001 y 5002)
#   5. Muestra cómo conectar un cliente
#
# Uso:
#   chmod +x start.sh
#   ./start.sh            # arranca todo
#   ./start.sh stop       # detiene los servidores lanzados por este script
# ──────────────────────────────────────────────────────────────

set -euo pipefail

# ── Colores ───────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}  ✓${RESET} $*"; }
warn() { echo -e "${YELLOW}  ⚠${RESET} $*"; }
err()  { echo -e "${RED}  ✗${RESET} $*"; }
info() { echo -e "${CYAN}  →${RESET} $*"; }

# ── Configuración ─────────────────────────────────────────────
VENV_DIR=".venv"
PID_FILE=".pfo3_pids"
LOG_DIR="logs"

PG_USER="${PG_USER:-postgres}"
PG_PASS="${PG_PASS:-postgres}"
PG_DB="${PG_DB:-pfo3}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"

RABBIT_HOST="${RABBIT_HOST:-localhost}"
RABBIT_PORT="${RABBIT_PORT:-5672}"
RABBIT_USER="${RABBIT_USER:-guest}"
RABBIT_PASS="${RABBIT_PASS:-guest}"

SERVER_PORT_1=5001
SERVER_PORT_2=5002

# ── Subcomando: stop ──────────────────────────────────────────
if [[ "${1:-}" == "stop" ]]; then
    if [[ ! -f "$PID_FILE" ]]; then
        warn "No se encontró $PID_FILE — ¿ya están detenidos?"
        exit 0
    fi
    echo -e "\n${BOLD}Deteniendo servidores...${RESET}"
    while IFS= read -r pid; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && ok "PID $pid detenido"
        else
            warn "PID $pid ya no estaba corriendo"
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
    echo -e "\n${GREEN}Sistema detenido.${RESET}\n"
    exit 0
fi

# ── Banner ────────────────────────────────────────────────────
echo -e "\n${BOLD}══════════════════════════════════════════${RESET}"
echo -e "${BOLD}   PFO 3 — Inicio manual (sin Docker)${RESET}"
echo -e "${BOLD}══════════════════════════════════════════${RESET}\n"

# ── 1. Verificar Python3 ──────────────────────────────────────
echo -e "${BOLD}[1/5] Verificando Python3...${RESET}"
if ! command -v python3 &>/dev/null; then
    err "python3 no encontrado. Instalalo desde https://python.org"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
ok "$PYTHON_VERSION"

# ── 2. Verificar RabbitMQ ─────────────────────────────────────
echo -e "\n${BOLD}[2/5] Verificando RabbitMQ...${RESET}"
if command -v rabbitmq-diagnostics &>/dev/null && rabbitmq-diagnostics ping &>/dev/null; then
    ok "RabbitMQ corriendo en $RABBIT_HOST:$RABBIT_PORT"
else
    warn "RabbitMQ no responde. Intentando iniciarlo..."
    # Linux (systemd)
    if command -v systemctl &>/dev/null; then
        sudo systemctl start rabbitmq-server 2>/dev/null \
            && ok "RabbitMQ iniciado (systemctl)" \
            || { err "No se pudo iniciar RabbitMQ. Ejecutá: sudo systemctl start rabbitmq-server"; exit 1; }
    # macOS (homebrew)
    elif command -v brew &>/dev/null; then
        brew services start rabbitmq 2>/dev/null \
            && ok "RabbitMQ iniciado (brew)" \
            || { err "No se pudo iniciar RabbitMQ. Ejecutá: brew services start rabbitmq"; exit 1; }
    else
        err "No se pudo iniciar RabbitMQ automáticamente."
        err "Inicialo manualmente y volvé a correr este script."
        exit 1
    fi
    # Esperar a que acepte conexiones
    info "Esperando que RabbitMQ esté listo..."
    for i in $(seq 1 10); do
        rabbitmq-diagnostics ping &>/dev/null && break
        sleep 2
    done
    rabbitmq-diagnostics ping &>/dev/null && ok "RabbitMQ listo" \
        || { err "RabbitMQ no respondió tras 20s"; exit 1; }
fi

# ── 3. Verificar PostgreSQL ───────────────────────────────────
echo -e "\n${BOLD}[3/5] Verificando PostgreSQL...${RESET}"
if pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" &>/dev/null; then
    ok "PostgreSQL corriendo en $PG_HOST:$PG_PORT"
else
    warn "PostgreSQL no responde. Intentando iniciarlo..."
    if command -v systemctl &>/dev/null; then
        sudo systemctl start postgresql 2>/dev/null \
            && ok "PostgreSQL iniciado (systemctl)" \
            || { err "No se pudo iniciar PostgreSQL. Ejecutá: sudo systemctl start postgresql"; exit 1; }
    elif command -v brew &>/dev/null; then
        brew services start postgresql@15 2>/dev/null \
            || brew services start postgresql 2>/dev/null \
            && ok "PostgreSQL iniciado (brew)" \
            || { err "No se pudo iniciar PostgreSQL. Ejecutá: brew services start postgresql"; exit 1; }
    else
        err "No se pudo iniciar PostgreSQL automáticamente."
        err "Inicialo manualmente y volvé a correr este script."
        exit 1
    fi
    info "Esperando que PostgreSQL esté listo..."
    for i in $(seq 1 10); do
        pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" &>/dev/null && break
        sleep 2
    done
    pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" &>/dev/null && ok "PostgreSQL listo" \
        || { err "PostgreSQL no respondió tras 20s"; exit 1; }
fi

# Crear la base de datos si no existe
info "Verificando base de datos '$PG_DB'..."
if PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
        -lqt 2>/dev/null | cut -d'|' -f1 | grep -qw "$PG_DB"; then
    ok "Base de datos '$PG_DB' ya existe"
else
    PGPASSWORD="$PG_PASS" createdb -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" "$PG_DB" 2>/dev/null \
        && ok "Base de datos '$PG_DB' creada" \
        || { err "No se pudo crear la base de datos '$PG_DB'"; exit 1; }
fi

# ── 4. Virtualenv y dependencias ─────────────────────────────
echo -e "\n${BOLD}[4/5] Preparando entorno Python...${RESET}"
if [[ ! -d "$VENV_DIR" ]]; then
    info "Creando virtualenv en $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
    ok "Virtualenv creado"
fi

# Activar venv para este script
source "$VENV_DIR/bin/activate"

# Instalar/actualizar dependencias solo si hace falta
if ! python3 -c "import pika, psycopg2" &>/dev/null; then
    info "Instalando dependencias desde requirements.txt ..."
    pip install --quiet -r requirements.txt
    ok "Dependencias instaladas"
else
    ok "Dependencias ya instaladas"
fi

# ── 5. Lanzar servidores ──────────────────────────────────────
echo -e "\n${BOLD}[5/5] Lanzando servidores...${RESET}"
mkdir -p "$LOG_DIR"
rm -f "$PID_FILE"

export RABBIT_HOST RABBIT_PORT RABBIT_USER RABBIT_PASS
export PG_HOST PG_PORT PG_DB PG_USER PG_PASS

for PORT in $SERVER_PORT_1 $SERVER_PORT_2; do
    LOG_FILE="$LOG_DIR/servidor_$PORT.log"
    SERVER_PORT=$PORT python3 servidor.py > "$LOG_FILE" 2>&1 &
    PID=$!
    echo "$PID" >> "$PID_FILE"
    ok "Servidor arrancado en puerto $PORT  (PID $PID, log: $LOG_FILE)"
done

# ── Resumen ────────────────────────────────────────────────────
echo -e "\n${BOLD}══════════════════════════════════════════${RESET}"
echo -e "${GREEN}${BOLD}  Sistema PFO3 en ejecución${RESET}"
echo -e "${BOLD}══════════════════════════════════════════${RESET}"
echo -e "  Servidor 1  →  localhost:$SERVER_PORT_1"
echo -e "  Servidor 2  →  localhost:$SERVER_PORT_2"
echo -e "  PostgreSQL  →  $PG_HOST:$PG_PORT  (db: $PG_DB)"
echo -e "  RabbitMQ    →  $RABBIT_HOST:$RABBIT_PORT"
echo ""
echo -e "  ${YELLOW}⚠  Los servidores pueden tardar ~10s en conectarse${RESET}"
echo -e "  ${YELLOW}   a RabbitMQ/PostgreSQL. Es normal ver reintentos en los logs.${RESET}"
echo ""
echo -e "  Conectar cliente:  ${CYAN}source $VENV_DIR/bin/activate && python3 cliente.py 127.0.0.1 $SERVER_PORT_1${RESET}"
echo -e "  Ver logs:          ${CYAN}tail -f $LOG_DIR/servidor_$SERVER_PORT_1.log${RESET}"
echo -e "  Detener todo:      ${CYAN}./start.sh stop${RESET}"
echo -e "${BOLD}══════════════════════════════════════════${RESET}\n"
