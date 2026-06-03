# PFO 3 — Sistema Distribuido Cliente-Servidor

## Arquitectura

```
Clientes (móvil / web / CLI)
        │  TCP
        ▼
Balanceador de carga  (Nginx — stream proxy)
        │  TCP interno
   ┌────┴────┐
Worker1  Worker2  Worker3  Worker4   ← pool de threads (servidor.py)
   └────┬────┘
        │  AMQP  (publica y consume)
   Cola de mensajes  ── RabbitMQ ──
        │
        │  psycopg2
   Base de datos  ── PostgreSQL ──
```

## Archivos

| Archivo | Descripción |
|---|---|
| `servidor.py` | Servidor TCP: publica en RabbitMQ, workers consumen y persisten en PostgreSQL |
| `cliente.py` | Cliente TCP con reconexión y comandos especiales |
| `requirements.txt` | Dependencias Python del proyecto |
| `arquitectura_sistema_distribuido.svg` | Diagrama del proyecto |

---

## Requisitos previos

### 1. RabbitMQ

**Ubuntu / Debian**
```bash
sudo apt install rabbitmq-server
sudo systemctl enable --now rabbitmq-server
```

**macOS (Homebrew)**
```bash
brew install rabbitmq
brew services start rabbitmq
```

**Docker (opción más rápida)**
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

Verificar que está corriendo:
```bash
rabbitmq-diagnostics ping   # debe responder "Ping succeeded"
```

Panel web (opcional): http://localhost:15672  (usuario: `guest`, contraseña: `guest`)

---

### 2. PostgreSQL

**Ubuntu / Debian**
```bash
sudo apt install postgresql
sudo systemctl enable --now postgresql
```

**macOS (Homebrew)**
```bash
brew install postgresql@15
brew services start postgresql@15
```

**Docker**
```bash
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

Crear la base de datos que usa el servidor:
```bash
psql -U postgres -c "CREATE DATABASE pfo3;"

o

docker exec -it postgres psql -U postgres -c "CREATE DATABASE pfo3;"
```

> La tabla `mensajes` se crea automáticamente al iniciar `servidor.py`.

---

## Instalación del entorno Python

```bash
# Crear entorno virtual (recomendado)
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

---

## Cómo ejecutar

```bash
# Terminal 1 — servidor
python3 servidor.py              # escucha en 127.0.0.1:5000 por defecto
python3 servidor.py 5001         # puerto personalizado

# Terminal 2, 3, … — clientes concurrentes
python3 cliente.py
python3 cliente.py 127.0.0.1 5000
```

---

## Comandos del cliente

| Comando | Efecto |
|---|---|
| cualquier texto | Se publica en RabbitMQ; un worker lo consume y guarda en PostgreSQL |
| `/historial` | Devuelve los últimos 10 mensajes desde PostgreSQL |
| `éxito` | Cierra la conexión limpiamente |
| `Ctrl+C` | Salida forzada |

---

## Configuración

Las variables de conexión están al inicio de `servidor.py`:
(Modificar en caso de usar otros datos -puertos, usuarios, db-)

```python
# RabbitMQ
RABBIT_HOST = "localhost"
RABBIT_PORT = 5672
RABBIT_USER = "guest"
RABBIT_PASS = "guest"

# PostgreSQL
PG_DSN = "host=localhost port=5432 dbname=pfo3 user=postgres password=postgres"

# Workers
NUM_WORKERS = 4
```

---

## Nginx (balanceador de carga)

Para múltiples instancias del servidor, configurar Nginx en modo `stream` (TCP):

```nginx
# /etc/nginx/nginx.conf
stream {
    upstream workers_pool {
        least_conn;
        server 127.0.0.1:5001;
        server 127.0.0.1:5002;
        server 127.0.0.1:5003;
        server 127.0.0.1:5004;
    }
    server {
        listen 5000;
        proxy_pass workers_pool;
    }
}
```

Lanzar una instancia por puerto:
```bash
python3 servidor.py 5001 &
python3 servidor.py 5002 &
python3 servidor.py 5003 &
python3 servidor.py 5004 &
sudo nginx -c /etc/nginx/nginx.conf
```

---

## Componentes y equivalencias

| Componente en el código | Tecnología | Rol en el sistema |
|---|---|---|
| `pika.BlockingConnection` | RabbitMQ / AMQP | Cola de mensajes entre clientes y workers |
| `psycopg2.ThreadedConnectionPool` | PostgreSQL | Almacenamiento persistente de mensajes |
| `threading.Thread` por cliente | — | Hilo de I/O por cada conexión TCP |
| `basic_qos(prefetch_count=1)` | RabbitMQ | Distribuye tareas equitativamente entre workers |
| `delivery_mode=2` | RabbitMQ | Mensajes persistentes (sobreviven reinicios) |
