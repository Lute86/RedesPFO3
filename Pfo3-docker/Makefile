# ──────────────────────────────────────────────────────────────
# PFO 3 — Makefile
# ──────────────────────────────────────────────────────────────
# Uso rápido:
#   make prod        →  build + arranca todo en background
#   make stop        →  detiene y elimina los contenedores
#   make logs        →  tail de todos los logs
#   make status      →  estado de los contenedores
#   make clean       →  stop + elimina volúmenes e imágenes
#   make client      →  conecta un cliente local al sistema
#   make rabbit-ui   →  abre el panel de RabbitMQ en el browser
# ──────────────────────────────────────────────────────────────

COMPOSE  = docker compose
PROJECT  = pfo3

# Puertos expuestos (host) — definidos aquí como única fuente de verdad
PORT_NGINX    = 5100
PORT_PG       = 5433
PORT_RABBIT   = 5673
PORT_RMQUI    = 15673

.PHONY: prod stop logs status clean client rabbit-ui help

# ── Target principal ──────────────────────────────────────────
prod:
	@echo "════════════════════════════════════════"
	@echo "  PFO 3 — Arrancando sistema distribuido"
	@echo "════════════════════════════════════════"
	$(COMPOSE) -p $(PROJECT) up --build -d
	@echo ""
	@echo "  Nginx (entrada pública) →  localhost:$(PORT_NGINX)"
	@echo "  Servidor 1 (directo)    →  localhost:5101"
	@echo "  Servidor 2 (directo)    →  localhost:5102"
	@echo "  PostgreSQL              →  localhost:$(PORT_PG)"
	@echo "  RabbitMQ AMQP           →  localhost:$(PORT_RABBIT)"
	@echo "  RabbitMQ UI             →  http://localhost:$(PORT_RMQUI)"
	@echo ""
	@echo "  Para conectar un cliente:  make client"
	@echo "  Para ver los logs:         make logs"

# ── Detener ───────────────────────────────────────────────────
stop:
	$(COMPOSE) -p $(PROJECT) down

# ── Logs en tiempo real ───────────────────────────────────────
logs:
	$(COMPOSE) -p $(PROJECT) logs -f

logs-server:
	$(COMPOSE) -p $(PROJECT) logs -f servidor_1 servidor_2

logs-rabbit:
	$(COMPOSE) -p $(PROJECT) logs -f rabbitmq

logs-pg:
	$(COMPOSE) -p $(PROJECT) logs -f postgres

# ── Estado ────────────────────────────────────────────────────
status:
	$(COMPOSE) -p $(PROJECT) ps

# ── Limpieza total (incluye volúmenes) ────────────────────────
clean:
	$(COMPOSE) -p $(PROJECT) down -v --rmi local
	@echo "Contenedores, volúmenes e imágenes locales eliminados."

# ── Cliente interactivo ───────────────────────────────────────
client:
	@echo "Conectando cliente a localhost:$(PORT_NGINX) ..."
	@echo "  Comandos: /historial  |  éxito (para salir)"
	@echo ""
	python3 cliente.py 127.0.0.1 $(PORT_NGINX)

# ── Panel RabbitMQ ────────────────────────────────────────────
rabbit-ui:
	@echo "Abriendo RabbitMQ Management UI → http://localhost:$(PORT_RMQUI)"
	@echo "  Usuario: pfo3user   |   Contraseña: pfo3pass"
	@open "http://localhost:$(PORT_RMQUI)" 2>/dev/null || \
	 xdg-open "http://localhost:$(PORT_RMQUI)" 2>/dev/null || \
	 echo "  Abrí manualmente: http://localhost:$(PORT_RMQUI)"

# ── Ayuda ─────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Targets disponibles:"
	@echo "  make prod        Construye y arranca todo el sistema"
	@echo "  make stop        Detiene los contenedores"
	@echo "  make logs        Tail de todos los logs"
	@echo "  make logs-server Logs solo de los servidores"
	@echo "  make logs-rabbit Logs solo de RabbitMQ"
	@echo "  make logs-pg     Logs solo de PostgreSQL"
	@echo "  make status      Estado de los contenedores"
	@echo "  make clean       Elimina contenedores, volúmenes e imágenes"
	@echo "  make client      Conecta un cliente Python al sistema"
	@echo "  make rabbit-ui   Abre el panel web de RabbitMQ"
	@echo ""
