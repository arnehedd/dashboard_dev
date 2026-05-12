"""Example: writes a parquet file with today's timestamp."""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

OUT = Path(__file__).parent.parent / "data" / "example_orders.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)
df = pd.DataFrame({
    "timestamp": [datetime.now(timezone.utc)],
    "order_id": [1],
    "amount": [42.0],
})
df.to_parquet(OUT)
print(f"wrote {len(df)} rows to {OUT}")
