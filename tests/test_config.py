from pathlib import Path

import pytest

from config import ConfigError, load_config


FIXTURE = Path(__file__).parent / "fixtures" / "pipelines_valid.yaml"


def test_load_programs():
    cfg = load_config(FIXTURE)
    assert set(cfg.programs.keys()) == {"load_orders", "transform_orders"}
    load = cfg.programs["load_orders"]
    assert load.script == "programs/load_orders.py"
    assert load.parquet == "data/orders.parquet"
    assert load.timestamp_column == "timestamp"
    assert load.timeout_seconds == 3600  # default


def test_load_programs_custom_timeout():
    cfg = load_config(FIXTURE)
    assert cfg.programs["transform_orders"].timeout_seconds == 60


def test_load_pipeline():
    cfg = load_config(FIXTURE)
    p = cfg.pipelines["daily_orders"]
    assert p.name == "Tägliche Order-Verarbeitung"
    assert [s.program for s in p.steps] == ["load_orders", "transform_orders"]
    assert p.steps[0].needs == []
    assert p.steps[1].needs == ["load_orders"]


def test_missing_program_reference_raises():
    fixture = Path(__file__).parent / "fixtures" / "pipelines_missing_ref.yaml"
    with pytest.raises(ConfigError, match="nonexistent_program"):
        load_config(fixture)


def test_cycle_raises():
    fixture = Path(__file__).parent / "fixtures" / "pipelines_cycle.yaml"
    with pytest.raises(ConfigError, match="cycle"):
        load_config(fixture)


def test_topo_sort_linear():
    cfg = load_config(FIXTURE)
    order = cfg.topo_sort("daily_orders")
    assert order == ["load_orders", "transform_orders"]
