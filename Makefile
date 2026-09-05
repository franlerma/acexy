# =============================================================================
# acexy — Makefile de desarrollo local
#
#   Dos flujos:
#     (A) Compilar la solución Go a binario local    -> make build
#     (B) Levantar el stack docker-compose en vivo   -> make up
#
#   Probar un stream con tu ID de AceStream:
#       make up
#       make play id=<tu-id-acestream>
#   y abre en VLC / mpv la URL que imprime el target `play`.
# =============================================================================

SHELL := /bin/sh

# Directorio del módulo Go y nombre del binario resultante
MODULE_DIR := acexy
BIN        := acexy
BIN_PATH   := $(MODULE_DIR)/$(BIN)

# Parámetros por defecto
COMPOSE   := docker compose
HTTP_PORT := 8080

# ID de stream AceStream para el target `play`: make play id=dd1e6707...
id ?=
STREAM_URL := http://127.0.0.1:$(HTTP_PORT)/ace/getstream?id=$(id)

.DEFAULT_GOAL := help
.PHONY: help build run up down logs status health play clean

help: ## Lista los targets disponibles
	@echo "acexy — targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

# -----------------------------------------------------------------------------
# (A) Compilación de la solución
# -----------------------------------------------------------------------------

build: ## Compila el binario acexy (requiere Go instalado)
	cd $(MODULE_DIR) && go build -o $(BIN)
	@echo "Binario listo: $(BIN_PATH)"

run: build ## Compila y ejecuta el proxy en local (sin Docker). Args extra: make run ARGS="-help"
	cd $(MODULE_DIR) && ./$(BIN) $(ARGS)

# -----------------------------------------------------------------------------
# (B) Stack docker-compose (vía recomendada para probar en vivo)
# -----------------------------------------------------------------------------

up: ## Construye la imagen y levanta el stack en background
	$(COMPOSE) up -d --build

down: ## Detiene y elimina los contenedores del stack
	$(COMPOSE) down

logs: ## Sigue los logs del stack
	$(COMPOSE) logs -f

status: ## Estado agregado del proxy (/ace/status)
	@curl -sf http://127.0.0.1:$(HTTP_PORT)/ace/status >/dev/null \
		&& echo "proxy OK en :$(HTTP_PORT)" \
		|| echo "proxy no responde en :$(HTTP_PORT) — ¿hiciste make up?"; \
		$(COMPOSE) ps

health: ## Healthcheck del contenedor acexy
	$(COMPOSE) ps acexy

play: ## Imprime la URL para reproducir un stream: make play id=<tu-id-acestream>
	@test -n "$(id)" || { echo "Falta el id: make play id=<tu-id-acestream>"; exit 1; }
	@echo "Abre esta URL en VLC / mpv:"
	@echo "  $(STREAM_URL)"

clean: ## Elimina el binario local
	rm -f $(BIN_PATH)
