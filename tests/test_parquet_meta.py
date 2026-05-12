from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from parquet_meta import clear_cache, get_meta


@pytest.fixture(autouse=True)
def _reset_cache():
    clear_cache()
    yield
    clear_cache()


def _write_parquet(path: Path, timestamps: list[datetime]):
    df = pd.DataFrame(
        {"timestamp": timestamps, "value": list(range(len(timestamps)))}
    )
    df.to_parquet(path)


def test_returns_latest_timestamp_and_size(tmp_path: Path):
    p = tmp_path / "x.parquet"
    _write_parquet(p, [
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 5, 12, 9, 14, tzinfo=timezone.utc),
        datetime(2026, 3, 1, tzinfo=timezone.utc),
    ])
    meta = get_meta(p, "timestamp")
    assert meta.latest_timestamp == datetime(
        2026, 5, 12, 9, 14, tzinfo=timezone.utc
    )
    assert meta.size_bytes is not None and meta.size_bytes > 0
    assert meta.error is None


def test_missing_file_returns_meta_with_error(tmp_path: Path):
    meta = get_meta(tmp_path / "does_not_exist.parquet", "timestamp")
    assert meta.latest_timestamp is None
    assert meta.size_bytes is None
    assert meta.error == "missing"


def test_missing_column_returns_error(tmp_path: Path):
    p = tmp_path / "y.parquet"
    pd.DataFrame({"other": [1, 2]}).to_parquet(p)
    meta = get_meta(p, "timestamp")
    assert meta.latest_timestamp is None
    assert meta.error and "timestamp" in meta.error


def test_empty_file_returns_no_timestamp(tmp_path: Path):
    p = tmp_path / "z.parquet"
    pd.DataFrame(
        {"timestamp": pd.Series([], dtype="datetime64[ns, UTC]")}
    ).to_parquet(p)
    meta = get_meta(p, "timestamp")
    assert meta.latest_timestamp is None
    assert meta.size_bytes is not None  # file does exist
    assert meta.error == "empty"


def test_cache_returns_same_object_within_window(tmp_path: Path):
    p = tmp_path / "c.parquet"
    _write_parquet(p, [datetime(2026, 1, 1, tzinfo=timezone.utc)])
    m1 = get_meta(p, "timestamp")
    # Overwrite with newer timestamp
    _write_parquet(p, [datetime(2026, 6, 1, tzinfo=timezone.utc)])
    m2 = get_meta(p, "timestamp")
    # Within 10s cache window, should still report old value
    assert m2.latest_timestamp == m1.latest_timestamp
