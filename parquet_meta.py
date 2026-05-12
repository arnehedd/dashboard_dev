import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


_CACHE_TTL_SECONDS = 10.0


@dataclass
class ParquetMeta:
    latest_timestamp: datetime | None
    size_bytes: int | None
    error: str | None  # 'missing' | 'empty' | 'no column: X' | 'read error: ...'


_cache: dict[tuple[str, str], tuple[float, ParquetMeta]] = {}


def _read(path: Path, column: str) -> ParquetMeta:
    if not path.exists():
        return ParquetMeta(None, None, "missing")
    size = path.stat().st_size
    try:
        df = pd.read_parquet(path, columns=[column])
    except (KeyError, ValueError) as exc:
        msg = str(exc)
        if column in msg:
            return ParquetMeta(None, size, f"no column: {column}")
        return ParquetMeta(None, size, f"read error: {exc.__class__.__name__}")
    except Exception as exc:  # pyarrow / parquet errors
        return ParquetMeta(None, size, f"read error: {exc.__class__.__name__}")
    if df.empty:
        return ParquetMeta(None, size, "empty")
    ts = df[column].max()
    if pd.isna(ts):
        return ParquetMeta(None, size, "empty")
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    return ParquetMeta(latest_timestamp=ts, size_bytes=size, error=None)


def get_meta(path: str | Path, timestamp_column: str) -> ParquetMeta:
    key = (str(path), timestamp_column)
    now = time.monotonic()
    cached = _cache.get(key)
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    meta = _read(Path(path), timestamp_column)
    _cache[key] = (now, meta)
    return meta


def clear_cache() -> None:
    _cache.clear()
