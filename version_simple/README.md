# PFO 3 — Sistema Distribuido Cliente-Servidor (versión simple)

## Arquitectura

```
Clientes (móvil / web / CLI)
        │
        ▼
Balanceador de carga  (Nginx / HAProxy)
        │
   ┌────┴────┐
Worker1  Worker2  Worker3  Worker4   ← pool de threads (servidor.py)
   └────┬────┘
        │
   Cola de mensajes  (queue.Queue → RabbitMQ en prod)
        │
   ┌────┴────┐
PostgreSQL    S3/MinIO               ← almacenamiento distribuido
```

## Archivos

| Archivo | Descripción |
|---|---|
| `servidor.py` | Servidor TCP con pool de N workers, cola de tareas y motor de cálculo |
| `cliente.py` | Cliente TCP con reconexión y comandos especiales |
| `mensajes.db` | SQLite — se genera la comenzar |

## Cómo ejecutar

```bash
# Terminal 1 — servidor (por defecto escucha en 127.0.0.1:5000)
python3 servidor.py

# Terminal 2, 3, … — clientes concurrentes
python3 cliente.py
python3 cliente.py 127.0.0.1 5000
```

## Tareas soportadas

Los workers ejecutan la tarea y devuelven el resultado real al cliente.

| Tarea | Ejemplo | Resultado |
|---|---|---|
| `calc:<expr>` | `calc:2+2` / `calc:10*(3+4)` / `calc:2**8` | `2+2 = 4` |
| `factorial:<n>` | `factorial:10` | `10! = 3628800` |
| `sqrt:<n>` | `sqrt:144` | `√144 = 12.0` |
| `primes:<n>` | `primes:20` | `2, 3, 5, 7, 11, 13, 17, 19` |
| `fib:<n>` | `fib:10` | `fib(10) = 55` |

`calc` usa un sandbox que solo permite dígitos y los operadores `+ - * / . ( )`.

## Comandos del cliente

| Comando | Efecto |
|---|---|
| `calc:<expr>` | Evalúa una expresión aritmética |
| `factorial:<n>` | Factorial de `n` (máx 1000) |
| `sqrt:<n>` | Raíz cuadrada de `n` |
| `primes:<n>` | Primos hasta `n` (máx 10000) |
| `fib:<n>` | n-ésimo número de Fibonacci (máx 1000) |
| `/historial` | Devuelve los últimos 10 mensajes procesados |
| `éxito` | Cierra la conexión limpiamente |
| `Ctrl+C` | Salida forzada |

## Conceptos del sistema distribuido implementados

- **Pool de workers**: `NUM_WORKERS` threads consumen tareas de `queue.Queue`
- **Cola de mensajes**: `queue.Queue` (equivalente local de RabbitMQ/AMQP)
- **Concurrencia**: cada cliente corre en un hilo de I/O independiente
- **Almacenamiento**: SQLite local (equivalente a PostgreSQL en producción)
- **Reconexión**: menú interactivo en cliente y servidor si el puerto falla
- **Procesamiento de tareas**: los workers ejecutan el motor matemático y devuelven el resultado al socket del cliente

## Escalado a producción

Para pasar a producción real reemplazá:

| Componente local | Equivalente productivo |
|---|---|
| `queue.Queue` | RabbitMQ (pika) o Redis Streams |
| SQLite | PostgreSQL con replicación |
| Socket directo | Nginx → Gunicorn / uWSGI |
| Archivos | Amazon S3 / MinIO |
