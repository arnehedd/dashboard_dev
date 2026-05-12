from config import Config, Pipeline, Program, Step
from state import StepStatus
from app import STATUS_COLORS, build_cytoscape_elements, format_status_pill


def _cfg() -> Config:
    return Config(
        programs={
            "a": Program("a", "a.py", "a.parquet", "timestamp"),
            "b": Program("b", "b.py", "b.parquet", "timestamp"),
            "c": Program("c", "c.py", "c.parquet", "timestamp"),
        },
        pipelines={
            "p": Pipeline("p", "P", "",
                          [Step("a", []), Step("b", ["a"]), Step("c", ["a"])]),
        },
    )


def test_cytoscape_elements_have_nodes_and_edges():
    cfg = _cfg()
    statuses = {"a": StepStatus.SUCCESS, "b": StepStatus.RUNNING, "c": StepStatus.IDLE}
    elements = build_cytoscape_elements(cfg.pipelines["p"], statuses)
    node_ids = [
        e["data"]["id"]
        for e in elements
        if "source" not in e.get("data", {})
    ]
    edges = [e for e in elements if "source" in e.get("data", {})]
    assert set(node_ids) == {"a", "b", "c"}
    assert {(e["data"]["source"], e["data"]["target"]) for e in edges} == {
        ("a", "b"), ("a", "c"),
    }
    a_node = next(e for e in elements if e["data"].get("id") == "a")
    assert a_node["data"]["status"] == "success"


def test_format_status_pill_returns_styled_span():
    span = format_status_pill(StepStatus.FAILED)
    assert span.children == "failed"
    assert STATUS_COLORS[StepStatus.FAILED] in span.style["background"]


def test_render_pipeline_detail_contains_graph_and_table():
    from app import render_pipeline_detail
    cfg = _cfg()
    statuses = {"a": StepStatus.SUCCESS, "b": StepStatus.IDLE, "c": StepStatus.IDLE}
    parquet = {"a": None, "b": None, "c": None}
    runs: list = []
    rendered = render_pipeline_detail(cfg.pipelines["p"], statuses, parquet, runs)
    serialized = repr(rendered)
    assert "Cytoscape" in serialized
    assert "a" in serialized and "b" in serialized and "c" in serialized
