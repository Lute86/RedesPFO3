PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Enunciado
Objetivo: Transformar el sistema en una arquitectura distribuida usando sockets.
Consignas:
1.​ Diseña un diagrama que incluya:
o​ Clientes (móviles, web).
o​ Balanceador de carga (Nginx/HAProxy).
o​ Servidores workers (cada uno con su pool de hilos).
o​ Cola de mensajes (RabbitMQ) para comunicación entre servidores.
o​ Almacenamiento distribuido (PostgreSQL, S3).
2.​ Implementa en Python:
o​ Un servidor que reciba tareas por socket y las distribuya a workers.
o​ Un cliente que envíe tareas y reciba resultados.
Entregables:
●​ Diagrama del sistema.
●​ Código del servidor y cliente en repositorio de Github
