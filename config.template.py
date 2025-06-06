"""Configuration template for OpenQuantKit.

Copy this file to ``config.py`` and edit the lists of tickers.
Each key in ``TICKER_GROUPS`` represents a group name and maps to
an iterable of ticker symbols.
"""

TICKER_GROUPS = {
    # Example groups. Replace tickers with your own selections.
    "REIT_DATA_CENTERS": ["DLR", "EQIX"],
    "REIT_ENERGY": ["NEE", "DUK"],
    "ENERGY_COMPANIES": ["XOM", "CVX"],
    "CHIP_MANUFACTURERS": ["TSM", "INTC", "NVDA"],
}

