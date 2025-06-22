# List of assets
from .dbt import open_quant_kit_dbt_assets
from .raw.raw_price import raw_price

assets = [
    open_quant_kit_dbt_assets,
    raw_price,
]
