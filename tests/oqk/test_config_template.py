import importlib.util
from pathlib import Path


def test_config_template_loads():
    template_path = Path(__file__).resolve().parents[2] / "oqk" / "config.template.py"
    spec = importlib.util.spec_from_file_location("config_template", template_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "TICKER_GROUPS")
    assert isinstance(module.TICKER_GROUPS, dict)
    assert module.TICKER_GROUPS  # not empty

