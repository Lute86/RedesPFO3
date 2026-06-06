# PFO 3 — Sistema Distribuido Cliente-Servidor (variante Flask)

## Introducción

Esta entrega corresponde al **PFO 3: Rediseño como Sistema Distribuido
(Cliente-Servidor)**. El objetivo es transformar un sistema cliente-servidor
convencional en una arquitectura distribuida que cumpla con las consignas
del enunciado:

- **Clientes** (móviles, web) — interfaz web servida por Flask y cliente
  Python de terminal, ambos sobre el mismo protocolo HTTP/JSON.
- **Balanceador de carga** (Nginx/HAProxy) — Nginx en modo HTTP delante de
  dos instancias de `server.py`, distribución `least_conn`.
- **Servidores workers con pool de hilos** — cada instancia de `server.py`
  levanta un pool de hilos daemon que consumen la cola de mensajes.
- **Cola de mensajes** (RabbitMQ) — desacopla la recepción HTTP de la
  ejecución; las tareas son durables (`delivery_mode=2`) y los workers
  usan `basic_qos(prefetch_count=1)` para reparto equitativo.
- **Almacenamiento distribuido** (PostgreSQL) — dos tablas:
  `mensajes` (auditoría + `/historial`) y `task_results` (correlación
  cliente↔worker por `task_id`, lo que permite que cualquier instancia
  de Flask resuelva cualquier `task_id`).

Hay dos formas equivalentes de correr el sistema:

- **Modo manual** — RabbitMQ y PostgreSQL instalados en el host, una o más
  instancias de `server.py` corriendo directamente con `python3`.
- **Modo dockerizado** — todo orquestado con Docker Compose: Postgres,
  RabbitMQ, dos instancias de Flask y Nginx como balanceador.

El diagrama completo de la arquitectura está en
[`arquitectura_sistema_distribuido.svg`](arquitectura_sistema_distribuido.svg).

---

## Arquitectura

```
Clientes (navegador / CLI)
        │  HTTP / JSON
        ▼
   Nginx :5000  (balanceador HTTP, least_conn)
        │
   ┌────┴────────────────┐
   ▼                     ▼
Flask :5000  (srv_1)  Flask :5000  (srv_2)
(server.py)          (server.py)
   │  basic_publish      │  basic_publish
   ▼                     ▼
        Cola de mensajes ── RabbitMQ ──
        │  basic_consume (4 workers por instancia, hilos daemon)
   ┌────┴────┐
  W1   W2   W3   W4   ← pool de hilos
   └────┬────┘
        │  psycopg2
   Base de datos ── PostgreSQL ──
   ├─ mensajes      (auditoría / /historial)
   └─ task_results  (correlación cliente ↔ worker por task_id)
```

El `task_id` (UUID generado en el POST inicial) es la clave de correlación:
reemplaza al descriptor de archivo del socket TCP de la versión anterior
y permite que la respuesta de un worker sea leída por **cualquier**
instancia de Flask (no necesariamente la que recibió la petición).

---

## Archivos

| Archivo | Descripción |
|---|---|
| `server.py`             | Servidor Flask: API HTTP, publica en RabbitMQ, workers consumen y persisten en PostgreSQL |
| `client.py`             | Cliente Python alternativo (mismo flujo vía `requests`) |
| `requirements.txt`      | Dependencias Python (Flask, pika, psycopg2-binary, requests) |
| `static/client.html`    | Cliente web (interfaz HTML) — también funciona si se abre directo con `file://` |
| `static/client.css`     | Estilos del cliente web |
| `static/client.js`      | Lógica del cliente web (fetch + polling + historial) |
| `Dockerfile`            | Imagen del servidor Flask (python:3.12-slim + curl para healthcheck) |
| `docker-compose.yml`    | Orquestación completa: Postgres, RabbitMQ, 2 servidores Flask, Nginx |
| `nginx.conf`            | Configuración del balanceador HTTP (modo `http`, no `stream`) |
| `Makefile`              | Atajos para operar el sistema dockerizado (`make prod/stop/logs/…`) |
| `arquitectura_sistema_distribuido.svg` | Diagrama del sistema (entregable del PFO) |
| `subject`               | Enunciado original de la consigna |
| `AGENTS.md`             | Notas internas para agentes sobre el repositorio |

---

## Tareas soportadas

| Comando | Ejemplo | Resultado |
|---|---|---|
| `calc:<expr>`     | `calc:2+2`, `calc:10*(3+4)`, `calc:2**8` | `<expr> = <resultado>` |
| `factorial:<n>`   | `factorial:10` | `10! = 3628800` |
| `sqrt:<n>`        | `sqrt:144` | `√144 = 12.0` |
| `primes:<n>`      | `primes:50` | lista de primos hasta n |
| `fib:<n>`         | `fib:10` | `fib(10) = 55` |
| `/historial`      | `/historial` | últimos 10 mensajes de PostgreSQL |

---

# 🅐 Modo manual (sin Docker)

Toda la infra corre en el host. Es la opción más liviana para desarrollar
y probar el sistema.

## A.1 Requisitos previos

### RabbitMQ

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

**Docker standalone (alternativa)**
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

Verificar: `rabbitmq-diagnostics ping` → debe decir `Ping succeeded`.
Panel web opcional: http://localhost:15672 (`guest` / `guest`).

### PostgreSQL

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

**Docker standalone (alternativa)**
```bash
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15
```

Crear la base de datos que usa el servidor:
```bash
psql -U postgres -c "CREATE DATABASE pfo3;"

o, con Docker:

docker exec -it postgres psql -U postgres -c "CREATE DATABASE pfo3;"
```

> Las tablas `mensajes` y `task_results` se crean automáticamente al
> iniciar `server.py`.

## A.2 Configuración

Las variables se leen del entorno. Si no se exportan, el servidor usa
los valores por defecto que están al inicio de `server.py`. Para una
instalación local típica, los defaults funcionan sin tocarlos:

| Variable | Default | Descripción |
|---|---|---|
| `SERVER_HOST` | `0.0.0.0`     | IP de escucha del servidor Flask |
| `SERVER_PORT` | `5000`        | Puerto HTTP |
| `NUM_WORKERS` | `4`           | Cantidad de threads workers (consumidores RabbitMQ) |
| `QUEUE_NAME`  | `tareas`      | Nombre de la cola RabbitMQ |
| `RABBIT_HOST` | `localhost`   | Host de RabbitMQ |
| `RABBIT_PORT` | `5672`        | Puerto AMQP |
| `RABBIT_USER` | `guest`       | Usuario RabbitMQ |
| `RABBIT_PASS` | `guest`       | Contraseña RabbitMQ |
| `PG_HOST`     | `localhost`   | Host PostgreSQL |
| `PG_PORT`     | `5432`        | Puerto PostgreSQL |
| `PG_DB`       | `pfo3`        | Base de datos |
| `PG_USER`     | `postgres`    | Usuario PostgreSQL |
| `PG_PASS`     | `postgres`    | Contraseña PostgreSQL |

Si querés usar otras credenciales u hosts, exportalas antes de arrancar:
```bash
export RABBIT_USER=pfo3user RABBIT_PASS=pfo3pass
export PG_USER=pfo3user PG_PASS=pfo3pass
python3 server.py
```

## A.3 Ejecución

### Instalar dependencias

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Levantar el servidor

```bash
python3 server.py              # escucha en 0.0.0.0:5000 por defecto
python3 server.py 5001         # puerto personalizado
```

Salida esperada:
```
PostgreSQL: tablas 'mensajes' y 'task_results' listas.
RabbitMQ: cola 'tareas' lista.
  Worker-1 iniciado.
  Worker-2 iniciado.
  Worker-3 iniciado.
  Worker-4 iniciado.
Servidor Flask escuchando en 0.0.0.0:5000  (4 workers activos)
Cliente web: http://localhost:5000/
```

### Múltiples instancias detrás de un balanceador (opcional)

Para correr dos instancias de Flask y poner un Nginx manual delante,
levantá cada una en un puerto distinto:
```bash
SERVER_PORT=5001 python3 server.py &
SERVER_PORT=5002 python3 server.py &
```
Y configurá Nginx con un `upstream` HTTP que apunte a `127.0.0.1:5001`
y `127.0.0.1:5002` (ver el `nginx.conf` del modo dockerizado como
referencia).

---

# 🅑 Modo dockerizado

Toda la infra (Postgres, RabbitMQ, dos instancias de Flask y Nginx) corre
en contenedores, orquestada por `docker-compose.yml`. Es la opción
recomendada para una demo reproducible.

## B.1 Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) 20.10+
- [Docker Compose](https://docs.docker.com/compose/install/) v2
  (incluido con Docker Desktop; en Linux viene como plugin `docker compose`)
- Opcional: [`make`](https://www.gnu.org/software/make/) para usar el
  `Makefile`

## B.2 Uso con `make` (recomendado)

```bash
make prod          # build + levanta todo en background
make status        # verifica que los 5 servicios estén healthy
make logs          # tail en tiempo real de todos los servicios
make web           # abre el cliente web en el browser
make client        # abre el cliente Python en la terminal
make stop          # detiene los contenedores
make clean         # stop + elimina volúmenes e imágenes
```

Tras `make prod`, esperá ~20–40 s la primera vez (Postgres y RabbitMQ
tardan en inicializar; los servidores Flask reintentan la conexión
automáticamente).

## B.3 Uso directo con `docker compose`

Si no tenés `make`, los comandos equivalentes son:

```bash
# Levantar todo
docker compose -p pfo3flask up --build -d

# Ver estado
docker compose -p pfo3flask ps

# Logs en tiempo real
docker compose -p pfo3flask logs -f

# Cliente web: abrir en el browser
xdg-open http://localhost:5000/        # Linux
open http://localhost:5000/            # macOS

# Cliente Python: se ejecuta en el host (necesita `requests`)
python3 client.py 127.0.0.1 5000

# Detener
docker compose -p pfo3flask down

# Limpieza total (volúmenes e imágenes locales)
docker compose -p pfo3flask down -v --rmi local
```

## B.4 Puertos expuestos en el host

| Servicio | Puerto host | Puerto interno | Notas |
|---|---|---|---|
| Nginx (entrada pública) | **5000** | 5000 | Balanceador HTTP, `least_conn` |
| Servidor 1 (directo)    | **5001** | 5000 | Debug sin pasar por Nginx |
| Servidor 2 (directo)    | **5002** | 5000 | Debug sin pasar por Nginx |
| PostgreSQL              | **5433** | 5432 | Evita conflicto con Postgres local |
| RabbitMQ AMQP           | **5673** | 5672 | Evita conflicto con RabbitMQ local |
| RabbitMQ UI             | **15673** | 15672 | Panel web de administración |

Credenciales de los servicios dockerizados: `pfo3user` / `pfo3pass`
(base de datos `pfo3`).

---

# 🅒 Uso del cliente

Independientemente del modo elegido, hay **dos clientes** que apuntan
al mismo servidor Flask.

## C.1 Cliente web (navegador)

Abrí `http://localhost:5000/` en el navegador. Vas a ver:

- una barra de estado con la cantidad de workers activos,
- un formulario para escribir tareas y chips con ejemplos rápidos,
- una lista de "tareas en vuelo" con polling automático (cada 500 ms)
  hasta que el worker responda,
- el historial reciente (últimos 10 mensajes).

> Truco: si querés ver la UI sin levantar el backend, podés abrir
> `static/client.html` directamente con doble click — los assets usan
> rutas relativas para que el CSS y el JS carguen desde el disco.

Desde `make`:
```bash
make web       # abre http://localhost:5000/ en el browser
```

## C.2 Cliente Python (terminal)

Pensado para uso interactivo desde la terminal o scripts:

```bash
python3 client.py                            # http://127.0.0.1:5000
python3 client.py 192.168.0.10 5000          # host y puerto personalizados
```

Desde `make` (si estás en modo dockerizado):
```bash
make client
```

Comandos disponibles (idénticos a la web):
```
calc:2+2                 →  2+2 = 4
factorial:10             →  10! = 3628800
sqrt:144                 →  √144 = 12.0
primes:50                →  Primos hasta 50: 2, 3, 5, 7, ...
fib:10                   →  fib(10) = 55
/historial               →  últimos 10 mensajes de PostgreSQL
éxito                    →  cierra y sale
Ctrl+C                   →  cierre forzado
```

El cliente implementa el mismo flujo que la web: `POST /api/tasks` →
recibe `task_id` → `GET /api/tasks/<id>` cada 500 ms hasta `status=done`.

---

# 🅓 API HTTP (referencia)

| Método | Endpoint | Body | Respuesta |
|---|---|---|---|
| `GET`  | `/`                       | —                | `client.html` (interfaz web) |
| `GET`  | `/client.css`             | —                | CSS del cliente |
| `GET`  | `/client.js`              | —                | JS del cliente |
| `GET`  | `/api/health`             | —                | `{"status":"ok","workers":4,"queue":"tareas"}` |
| `GET`  | `/api/help`               | —                | descripción de comandos |
| `POST` | `/api/tasks`              | `{"msg":"calc:2+2"}` | `202 {"task_id":"…","status":"pending"}` |
| `GET`  | `/api/tasks/<task_id>`    | —                | `{"task_id":"…","status":"done","resultado":"…","worker":1,"fecha":"…"}` |
| `GET`  | `/api/historial`          | —                | `[{"id":…,"msg":"…","fecha":"…","worker":…}]` |

**Flujo cliente:** `POST /api/tasks` → guarda `task_id` →
`GET /api/tasks/<id>` cada 500 ms hasta `status="done"`.

---

# 🅔 Componentes y equivalencias

| Componente en el código | Tecnología | Rol en el sistema |
|---|---|---|
| Flask `app.route`             | Flask / HTTP   | API JSON + cliente web (reemplaza al socket TCP) |
| `pika.BlockingConnection`     | RabbitMQ / AMQP| Cola de mensajes entre la API y los workers |
| `psycopg2.ThreadedConnectionPool` | PostgreSQL | Almacenamiento persistente (mensajes + resultados) |
| `threading.Thread` por worker | —             | Hilo de consumo de RabbitMQ; un worker por hilo |
| `basic_qos(prefetch_count=1)` | RabbitMQ      | Distribuye tareas equitativamente entre workers |
| `delivery_mode=2`             | RabbitMQ      | Mensajes persistentes (sobreviven reinicios) |
| `task_id` (UUID)              | correlación    | Asocia la respuesta de un worker con la petición del cliente (reemplaza al `fd` del socket TCP) |
| `publisher_lock`              | pika          | Protege el canal publicador compartido entre los request threads de Flask |
| Nginx `upstream` + `least_conn` | HTTP proxy | Balanceador de carga HTTP entre las dos instancias de Flask |
