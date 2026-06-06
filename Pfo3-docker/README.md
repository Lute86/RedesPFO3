# PFO 3 — Sistema Distribuido Cliente-Servidor

## Arquitectura

```
Clientes (móvil / web / CLI)
        │  TCP :5100
        ▼
  Nginx (balanceador — least_conn)
        │  TCP interno
   ┌────┴────┐
servidor_1  servidor_2        ← contenedores Docker (servidor.py)
 4 workers   4 workers        ← threads por contenedor
   └────┬────┘
        │  AMQP — RabbitMQ :5673
   Cola de mensajes
        │  psycopg2 — PostgreSQL :5433
   Base de datos
```

## Archivos del proyecto

| Archivo | Descripción |
|---|---|
| `servidor.py` | Servidor TCP: publica en RabbitMQ, workers consumen y persisten en PostgreSQL |
| `cliente.py` | Cliente TCP con reconexión y comandos especiales |
| `Dockerfile` | Imagen del servidor |
| `docker-compose.yml` | Orquestación completa del sistema |
| `nginx.conf` | Balanceador TCP (stream) para los dos servidores |
| `Makefile` | Comandos de operación |
| `requirements.txt` | Dependencias Python |

---

## Inicio rápido

### Linux / macOS (con Make)

```bash
make prod
```

### Windows (sin Make — PowerShell o CMD)

```bat
docker compose up --build -d
```

Para ver los logs en tiempo real:
```bat
docker compose logs -f
```

Para detener todo:
```bat
docker compose down
```

Para conectar un cliente (una vez que el sistema esté listo):
```bat
python cliente.py 127.0.0.1 5100
```

### Linux / macOS — conectar cliente

```bash
make client
```

---

## ⚠️ Tiempo de arranque

Al levantar el sistema por primera vez, **es normal que los servidores fallen o rechacen conexiones durante 20–40 segundos**. Esto ocurre porque Docker inicia los contenedores en paralelo y PostgreSQL/RabbitMQ necesitan unos segundos para inicializarse antes de aceptar conexiones.

El servidor tiene reintentos automáticos (hasta 10 intentos con pausa de 3 segundos), así que no hace falta intervenir. Esperar a que los logs muestren estas líneas, que indican que el sistema está completamente listo:

```
pfo3_servidor_1  | PostgreSQL: tabla 'mensajes' lista.
pfo3_servidor_1  | RabbitMQ: cola 'tareas' lista.
pfo3_servidor_1  | Servidor escuchando en 0.0.0.0:5100
```

Para monitorear el arranque: `docker compose logs -f` (o `make logs` en Linux/macOS).

---

## Puertos expuestos en el host

Todos los puertos usan valores no estándar para evitar conflictos con servicios locales.

| Servicio | Puerto host | Puerto interno | Por qué cambia |
|---|---|---|---|
| Nginx (entrada pública) | **5100** | 5100 | Puerto de entrada del cliente |
| Servidor 1 (directo) | **5101** | 5100 | Acceso de debug sin pasar por Nginx |
| Servidor 2 (directo) | **5102** | 5100 | Acceso de debug sin pasar por Nginx |
| PostgreSQL | **5433** | 5432 | Evita conflicto con PostgreSQL local |
| RabbitMQ AMQP | **5673** | 5672 | Evita conflicto con RabbitMQ local |
| RabbitMQ UI | **15673** | 15672 | Panel de administración |

---

## Comandos Makefile

| Comando | Efecto |
|---|---|
| `make prod` | Build + arranca todo el sistema en background |
| `make stop` | Detiene y elimina los contenedores |
| `make logs` | Tail en tiempo real de todos los servicios |
| `make logs-server` | Logs solo de los servidores |
| `make logs-rabbit` | Logs solo de RabbitMQ |
| `make logs-pg` | Logs solo de PostgreSQL |
| `make status` | Estado de los contenedores |
| `make clean` | Stop + elimina volúmenes e imágenes locales |
| `make client` | Conecta un cliente Python al sistema vía Nginx |
| `make rabbit-ui` | Abre el panel web de RabbitMQ en el browser |

---

## Comandos del cliente

| Comando | Efecto |
|---|---|
| cualquier texto | Se publica en RabbitMQ; un worker lo guarda en PostgreSQL |
| `/historial` | Devuelve los últimos 10 mensajes desde PostgreSQL |
| `éxito` | Cierra la conexión limpiamente |
| `Ctrl+C` | Salida forzada |

---

## Instalación local (sin Docker)

### Dependencias Python

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### RabbitMQ

```bash
# Ubuntu/Debian
sudo apt install rabbitmq-server && sudo systemctl start rabbitmq-server

# macOS
brew install rabbitmq && brew services start rabbitmq

# Docker standalone
docker run -d -p 5672:5672 rabbitmq:3-management
```

### PostgreSQL

```bash
# Ubuntu/Debian
sudo apt install postgresql && sudo systemctl start postgresql

# macOS
brew install postgresql@15 && brew services start postgresql@15

# Docker standalone
docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:15
```

Crear la base de datos:
```bash
psql -U postgres -c "CREATE DATABASE pfo3;"
```

### Ejecutar

```bash
python3 servidor.py        # escucha en 0.0.0.0:5000 por defecto
python3 cliente.py
```

---

## Variables de entorno del servidor

| Variable | Default | Descripción |
|---|---|---|
| `SERVER_HOST` | `0.0.0.0` | IP de escucha |
| `SERVER_PORT` | `5000` | Puerto TCP |
| `NUM_WORKERS` | `4` | Threads worker por instancia |
| `QUEUE_NAME` | `tareas` | Nombre de la cola RabbitMQ |
| `RABBIT_HOST` | `localhost` | Host de RabbitMQ |
| `RABBIT_PORT` | `5672` | Puerto AMQP |
| `RABBIT_USER` | `guest` | Usuario RabbitMQ |
| `RABBIT_PASS` | `guest` | Contraseña RabbitMQ |
| `PG_HOST` | `localhost` | Host PostgreSQL |
| `PG_PORT` | `5432` | Puerto PostgreSQL |
| `PG_DB` | `pfo3` | Base de datos |
| `PG_USER` | `postgres` | Usuario PostgreSQL |
| `PG_PASS` | `postgres` | Contraseña PostgreSQL |
