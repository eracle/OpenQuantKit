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
            "config.py not found. Copy oqk/config.template.py to config.py and populate your ticker lists."
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

@app.cell
def _(ticker_groups):
    """Select tickers from predefined groups or add your own."""

    import marimo as mo

    group_select = mo.ui.multiselect(
        options=list(ticker_groups.keys()),
        label="Select ticker groups",
    )

    custom_input = mo.ui.text(
        label="Additional tickers (comma separated)",
        value="",
    )

    def _combine(groups: list[str], custom: str) -> list[str]:
        tickers = []
        for group in groups:
            tickers.extend(ticker_groups.get(group, []))
        if custom:
            tickers.extend(
                [t.strip().upper() for t in custom.split(",") if t.strip()]
            )
        seen = set()
        deduped = []
        for ticker in tickers:
            if ticker not in seen:
                deduped.append(ticker)
                seen.add(ticker)
        return deduped

    selected_tickers = mo.compute(_combine, group_select, custom_input)

    mo.vstack([
        group_select,
        custom_input,
        mo.md("**Selected tickers**"),
        selected_tickers,
    ])

    return selected_tickers

if __name__ == "__main__":
    app.run()

