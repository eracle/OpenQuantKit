import marimo

app = marimo.App()

@app.cell
def _():
    from importlib import util
    from pathlib import Path
    import sys
    return util, Path, sys

@app.cell
def _(util, Path, sys):
    config_path = Path(__file__).with_name("config.py")
    if not config_path.exists():
        raise FileNotFoundError(
            "config.py not found. Copy config.template.py to config.py and populate your ticker lists."
        )
    spec = util.spec_from_file_location("config", config_path)
    config = util.module_from_spec(spec)
    sys.modules["config"] = config
    spec.loader.exec_module(config)
    return config

@app.cell
def _(config):
    ticker_groups = getattr(config, "TICKER_GROUPS", None)
    if ticker_groups is None:
        raise AttributeError("TICKER_GROUPS not defined in config.py")
    print("Loaded ticker lists:")
    for name, tickers in ticker_groups.items():
        print(f"{name}: {tickers}")
    return ticker_groups

if __name__ == "__main__":
    app.run()

