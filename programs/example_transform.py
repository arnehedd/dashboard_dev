"""Example: reads the orders parquet, doubles the amount, writes a new file."""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

IN = Path(__file__).parent.parent / "data" / "example_orders.parquet"
OUT = Path(__file__).parent.parent / "data" / "example_orders_clean.parquet"
df = pd.read_parquet(IN)
df["amount"] = df["amount"] * 2
df["timestamp"] = datetime.now(timezone.utc)
df.to_parquet(OUT)
print(f"transformed {len(df)} rows -> {OUT}")
