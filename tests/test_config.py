from pathlib import Path

from config import load_config


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
    # plain-string steps depend on the previous step in the list
    assert p.steps[0].needs == []
    assert p.steps[1].needs == ["load_orders"]
