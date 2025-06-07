.DEFAULT_GOAL := help

.PHONY: $(shell perl -nle'print $$& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {print $$1}')

help: ## shows the list of commands
	@perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

run: ## launch notebook using local venv
	./init.sh

compose: ## executes the notebook on docker
	docker compose -f local.yml up --build

test: ## run tests
	docker compose -f local.yml run --remove-orphans marimo py.test -vv --cache-clear

