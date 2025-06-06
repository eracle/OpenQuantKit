.PHONY: venv run compose

venv:
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

run: venv
        .venv/bin/marimo edit notebook.py --port 8888

compose:
       docker compose -f local.yml up --build
