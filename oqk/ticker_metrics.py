# oqk/ticker_metrics.py

import pandas as pd
from datetime import timedelta
from pandas.tseries.offsets import BDay

from .ticker_db import get_safe_lag_date

def compute_ticker_metrics(df: pd.DataFrame) -> dict:
    df = df.copy()
    df = df.dropna(subset=["close"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    if df.empty:
        return {}

    date_range = pd.date_range(df["date"].min(), df["date"].max(), freq="D")
    weekday_range = date_range[date_range.weekday < 5]

    gaps = df["date"].diff().dropna().dt.days - 1
    largest_gap = gaps.max() if not gaps.empty else 0
    num_gaps_gt_3 = (gaps > 3).sum()
    num_gaps_gt_5 = (gaps > 5).sum()

    close_std = df["close"].std()
    num_zeros = (df["close"] <= 1e-5).sum()
    num_duplicates = df["date"].duplicated().sum()
    duration = (df["date"].max() - df["date"].min()).days + 1
    num_points = len(df)

    return {
        "data_duration_days": duration,
        "num_data_points": num_points,
        "completeness_ratio": round(num_points / duration, 3),
        "largest_gap_days": int(largest_gap),
        "num_gaps_gt_3_days": int(num_gaps_gt_3),
        "num_gaps_gt_5_days": int(num_gaps_gt_5),
        "std_close": round(close_std, 5) if close_std is not None else None,
        "num_zero_close": int(num_zeros),
        "first_date": df["date"].min().date(),
        "last_date": df["date"].max().date(),
        "weekday_coverage": round(len(df["date"].dt.normalize().drop_duplicates()) / len(weekday_range), 3),
        "has_recent_data": df["date"].max().date() >= get_safe_lag_date(),
        "num_duplicate_dates": int(num_duplicates),
    }
