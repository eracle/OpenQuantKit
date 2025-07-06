# List of assets
from .dbt import open_quant_kit_dbt_assets
from .dim.dim_cik import dim_cik
from .raw.raw_filing import raw_filing
from .raw.raw_filing_index import raw_filing_index
from .raw.raw_price import raw_price

assets = [
    open_quant_kit_dbt_assets,
    raw_price,
    dim_cik,
    raw_filing_index,
    raw_filing,
]
