.DEFAULT_GOAL := help

.PHONY: $(shell perl -nle'print $$& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {print $$1}')

help: ## shows the list of commands
	@perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

build: ## builds docker image
	docker compose build

stop: ## stops all docker containers
	docker compose stop

postgres: ## runs postgres locally
	docker compose up --build -d postgres

drop-db: stop ## drop the local database content
	docker compose -f local.yml down && \
	docker volume rm open_quant_kit_postgres_data open_quant_kit_postgres_data_backups

dagster: ## runs dagster
	docker compose up --build --remove-orphans dagster
