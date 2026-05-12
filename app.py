from datetime import datetime
from pathlib import Path

import dash_cytoscape as cyto
from dash import (
    ALL,
    Dash,
    Input,
    Output,
    State as DashState,
    callback_context,
    dcc,
    html,
    no_update,
)

from config import Config, Pipeline
from parquet_meta import ParquetMeta, get_meta
from runner import Runner
from state import RunRow, RunStatus, State, StepStatus


STATUS_COLORS: dict[StepStatus, str] = {
    StepStatus.IDLE:    "#9ca3af",
    StepStatus.PENDING: "#9ca3af",
    StepStatus.RUNNING: "#3b82f6",
    StepStatus.SUCCESS: "#10b981",
    StepStatus.FAILED:  "#ef4444",
    StepStatus.SKIPPED: "#f59e0b",
}


def build_cytoscape_elements(
    pipeline: Pipeline, statuses: dict[str, StepStatus]
) -> list[dict]:
    elements: list[dict] = []
    for step in pipeline.steps:
        st = statuses.get(step.program, StepStatus.IDLE)
        elements.append({
            "data": {
                "id": step.program,
                "label": step.program,
                "status": st.value,
                "color": STATUS_COLORS[st],
            }
        })
    for step in pipeline.steps:
        for dep in step.needs:
            elements.append({
                "data": {"source": dep, "target": step.program}
            })
    return elements


def format_status_pill(status: StepStatus) -> html.Span:
    return html.Span(
        status.value,
        style={
            "padding": "2px 8px",
            "borderRadius": "10px",
            "background": STATUS_COLORS[status],
            "color": "#fff",
            "fontSize": "12px",
        },
    )


CYTO_STYLESHEET = [
    {"selector": "node", "style": {
        "background-color": "data(color)",
        "label": "data(label)",
        "color": "#fff",
        "text-valign": "center",
        "text-halign": "center",
        "font-size": "11px",
        "shape": "round-rectangle",
        "width": "label",
        "height": "30",
        "padding": "8px",
    }},
    {"selector": "edge", "style": {
        "curve-style": "bezier",
        "target-arrow-shape": "triangle",
        "line-color": "#9ca3af",
        "target-arrow-color": "#9ca3af",
        "width": 1.5,
    }},
]


def _format_ts(ts) -> str:
    if ts is None:
        return "—"
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def _format_size(size: int | None) -> str:
    if size is None:
        return "—"
    if size < 1024:
        return f"{size} B"
    if size < 1024 ** 2:
        return f"{size / 1024:.1f} KB"
    return f"{size / 1024**2:.1f} MB"


def _sidebar(config: Config) -> html.Div:
    pipeline_items = [
        html.Div(
            id={"type": "pipeline-item", "id": pid},
            n_clicks=0,
            children=[
                html.Strong(p.name),
                html.Div(p.description,
                         style={"fontSize": "11px", "color": "#6b7280"}),
            ],
            style={
                "padding": "8px 10px",
                "border": "1px solid #e5e7eb",
                "borderRadius": "6px",
                "marginBottom": "6px",
                "cursor": "pointer",
                "background": "#fff",
            },
        )
        for pid, p in config.pipelines.items()
    ]
    return html.Div([
        html.H4("Pipelines", style={"margin": "8px 0"}),
        *pipeline_items,
        html.H4("Programme", style={"margin": "16px 0 8px"}),
        html.Div(
            id={"type": "section", "id": "all-programs"},
            n_clicks=0,
            children="Alle Programme",
            style={
                "padding": "8px 10px",
                "border": "1px solid #e5e7eb",
                "borderRadius": "6px",
                "cursor": "pointer",
                "background": "#fff",
            },
        ),
    ], style={"flex": "0 0 260px", "padding": "12px",
              "background": "#f9fafb", "height": "100%", "overflowY": "auto"})


def _detail_placeholder() -> html.Div:
    return html.Div(
        "Wähle links eine Pipeline oder die Programm-Übersicht.",
        style={"padding": "24px", "color": "#6b7280"},
    )


def _duration(start: str | None, end: str | None) -> str:
    if not start or not end:
        return "—"
    try:
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
    except ValueError:
        return "—"
    secs = int((e - s).total_seconds())
    if secs < 60:
        return f"{secs}s"
    return f"{secs // 60}m {secs % 60:02d}s"


def render_pipeline_detail(
    pipeline: Pipeline,
    statuses: dict[str, StepStatus],
    parquet: dict[str, ParquetMeta | None],
    runs: list[RunRow],
) -> html.Div:
    elements = build_cytoscape_elements(pipeline, statuses)
    graph = cyto.Cytoscape(
        id="pipeline-dag",
        elements=elements,
        layout={"name": "breadthfirst", "directed": True, "padding": 10},
        stylesheet=CYTO_STYLESHEET,
        style={"width": "100%", "height": "260px",
               "border": "1px solid #e5e7eb", "borderRadius": "6px"},
    )
    rows = [
        html.Tr([
            html.Td(prog),
            html.Td(format_status_pill(statuses.get(prog, StepStatus.IDLE))),
            html.Td(_format_ts(
                parquet[prog].latest_timestamp if parquet.get(prog) else None
            )),
            html.Td(_format_size(
                parquet[prog].size_bytes if parquet.get(prog) else None
            )),
        ])
        for prog in [s.program for s in pipeline.steps]
    ]
    table = html.Table(
        [html.Thead(html.Tr([html.Th(c) for c in
                             ["Programm", "Status", "Letzter TS", "Größe"]]))]
        + [html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse",
               "marginTop": "16px", "fontSize": "13px"},
    )
    history_rows = [
        html.Tr([
            html.Td(r.started_at),
            html.Td(_duration(r.started_at, r.ended_at)),
            html.Td(format_status_pill(StepStatus(r.status.value))),
            html.Td(html.Code(f"logs/{r.id}/", style={"fontSize": "11px"})),
        ])
        for r in runs
    ]
    history = html.Table(
        [html.Thead(html.Tr([html.Th(c) for c in
                             ["Start", "Dauer", "Status", "Logs"]]))]
        + [html.Tbody(history_rows)],
        style={"width": "100%", "borderCollapse": "collapse",
               "marginTop": "16px", "fontSize": "12px"},
    )
    return html.Div([
        html.Div([
            html.H3(pipeline.name, style={"margin": "0"}),
            html.Div(pipeline.description, style={"color": "#6b7280"}),
        ], style={"marginBottom": "12px"}),
        html.Button(
            "▶ Run Pipeline", id="run-pipeline-btn", n_clicks=0,
            **{"data-pipeline-id": pipeline.id},
            style={"padding": "6px 14px", "background": "#4f6df5",
                   "color": "#fff", "border": "none",
                   "borderRadius": "6px", "cursor": "pointer",
                   "marginBottom": "12px"},
        ),
        graph,
        html.H4("Programme", style={"margin": "16px 0 4px"}),
        table,
        html.H4("Letzte Läufe", style={"margin": "16px 0 4px"}),
        history,
    ])


def render_programs_detail(
    config: Config,
    statuses: dict[str, StepStatus],
    parquet: dict[str, ParquetMeta | None],
) -> html.Div:
    rows = [
        html.Tr([
            html.Td(pid),
            html.Td(format_status_pill(statuses.get(pid, StepStatus.IDLE))),
            html.Td(_format_ts(
                parquet[pid].latest_timestamp if parquet.get(pid) else None
            )),
            html.Td(_format_size(
                parquet[pid].size_bytes if parquet.get(pid) else None
            )),
            html.Td(html.Button(
                "▶", id={"type": "run-program", "id": pid}, n_clicks=0,
                style={"padding": "2px 8px", "background": "#4f6df5",
                       "color": "#fff", "border": "none",
                       "borderRadius": "4px", "cursor": "pointer"},
            )),
        ])
        for pid in config.programs
    ]
    return html.Div([
        html.H3("Alle Programme"),
        html.Table(
            [html.Thead(html.Tr([html.Th(c) for c in
                                 ["Programm", "Status", "Letzter TS",
                                  "Größe", ""]]))]
            + [html.Tbody(rows)],
            style={"width": "100%", "borderCollapse": "collapse",
                   "fontSize": "13px"},
        ),
    ])


def make_app(config: Config, state: State, runner: Runner) -> Dash:
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.title = "dash-pipeline"
    app.layout = html.Div([
        dcc.Store(id="selected-view", data={"kind": "none"}),
        dcc.Interval(id="refresh-interval", interval=2000),  # 2 s
        html.Div(
            [
                html.Span("dash-pipeline", style={"fontWeight": 600}),
                html.Span(
                    f" · {len(config.pipelines)} pipelines · "
                    f"{len(config.programs)} programs",
                    style={"color": "#9ca3af", "marginLeft": "8px"},
                ),
            ],
            style={"padding": "10px 14px", "background": "#1f2937",
                   "color": "#fff", "fontFamily": "sans-serif"},
        ),
        html.Div(
            [_sidebar(config),
             html.Div(_detail_placeholder(), id="detail-panel",
                      style={"flex": 1, "padding": "16px",
                             "overflowY": "auto"})],
            style={"display": "flex", "height": "calc(100vh - 44px)"},
        ),
        # Modal placeholder (filled in Task 12)
        html.Div(
            id="log-modal",
            children=[
                html.Div(
                    [
                        html.Div(
                            [
                                html.Strong(id="log-modal-title"),
                                html.Button(
                                    "✕", id="log-modal-close", n_clicks=0,
                                    style={"float": "right",
                                           "background": "none",
                                           "border": "none",
                                           "fontSize": "16px",
                                           "cursor": "pointer"},
                                ),
                            ],
                            style={"marginBottom": "8px"},
                        ),
                        html.Pre(
                            id="log-modal-body",
                            style={"maxHeight": "60vh",
                                   "overflow": "auto",
                                   "background": "#0f172a",
                                   "color": "#e5e7eb",
                                   "padding": "12px",
                                   "fontSize": "12px",
                                   "borderRadius": "6px",
                                   "whiteSpace": "pre-wrap"},
                        ),
                    ],
                    style={"background": "#fff", "padding": "16px",
                           "borderRadius": "8px",
                           "width": "min(800px, 90vw)",
                           "maxHeight": "80vh", "overflow": "auto"},
                )
            ],
            style={"display": "none", "position": "fixed", "inset": "0",
                   "background": "rgba(0,0,0,0.4)",
                   "alignItems": "center", "justifyContent": "center",
                   "zIndex": 1000},
        ),
    ])
    _register_callbacks(app, config, state, runner)
    return app


def _register_callbacks(
    app: Dash, config: Config, state: State, runner: Runner
) -> None:

    @app.callback(
        Output("selected-view", "data"),
        Input({"type": "pipeline-item", "id": ALL}, "n_clicks"),
        Input({"type": "section", "id": "all-programs"}, "n_clicks"),
        prevent_initial_call=True,
    )
    def on_select(_pipeline_clicks, _programs_click):
        ctx = callback_context
        if not ctx.triggered:
            return no_update
        triggered = ctx.triggered_id
        if isinstance(triggered, dict) and triggered.get("type") == "pipeline-item":
            return {"kind": "pipeline", "id": triggered["id"]}
        return {"kind": "programs"}

    @app.callback(
        Output("detail-panel", "children"),
        Input("selected-view", "data"),
        Input("refresh-interval", "n_intervals"),
    )
    def render_detail(view, _tick):
        if not view or view.get("kind") == "none":
            return _detail_placeholder()
        if view["kind"] == "pipeline":
            pipeline = config.pipelines[view["id"]]
            statuses = {s.program: state.get_program_status(s.program)
                        for s in pipeline.steps}
            parquet = {
                s.program: get_meta(
                    config.programs[s.program].parquet,
                    config.programs[s.program].timestamp_column,
                )
                for s in pipeline.steps
            }
            runs = state.list_recent_runs(pipeline_id=view["id"], limit=10)
            return render_pipeline_detail(pipeline, statuses, parquet, runs)
        # programs view
        statuses = {pid: state.get_program_status(pid) for pid in config.programs}
        parquet = {pid: get_meta(p.parquet, p.timestamp_column)
                   for pid, p in config.programs.items()}
        return render_programs_detail(config, statuses, parquet)

    @app.callback(
        Output("run-pipeline-btn", "disabled"),
        Input("run-pipeline-btn", "n_clicks"),
        DashState("selected-view", "data"),
        prevent_initial_call=True,
    )
    def on_run_pipeline(n, view):
        if not n or not view or view.get("kind") != "pipeline":
            return no_update
        try:
            runner.run_pipeline(view["id"])
        except RuntimeError:
            pass  # already running
        return True  # disable; auto-refresh re-renders shortly

    @app.callback(
        Output({"type": "run-program", "id": ALL}, "disabled"),
        Input({"type": "run-program", "id": ALL}, "n_clicks"),
        DashState({"type": "run-program", "id": ALL}, "id"),
        prevent_initial_call=True,
    )
    def on_run_program(_clicks, ids):
        ctx = callback_context
        if not ctx.triggered:
            return [no_update] * len(ids)
        triggered = ctx.triggered_id
        if isinstance(triggered, dict) and triggered.get("type") == "run-program":
            runner.run_program(triggered["id"])
        return [no_update] * len(ids)
