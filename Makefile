.DEFAULT_GOAL := help

.PHONY: $(shell perl -nle'print $$& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {print $$1}')

help: ## shows the list of commands
	@perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

venv: ## init virtual environment locally
	python -m venv venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r requirements.txt
	source venv/bin/activate

run:  ## executes the notebook locally
	venv/bin/marimo edit notebook.py --port 8888

compose: ## executes the notebook on docker
	docker compose -f local.yml up --build

