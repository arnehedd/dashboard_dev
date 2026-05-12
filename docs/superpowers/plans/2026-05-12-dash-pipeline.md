# Dash Pipeline Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local Dash dashboard that starts existing Python scripts (alone or in chains), tracks their status in SQLite, and shows the latest timestamp in each script's parquet output.

**Architecture:** Single Dash process. `runner.py` executes pipelines in a `ThreadPoolExecutor`, each step starts a `subprocess.Popen`. State lives in `runs.sqlite`. Pipelines are declared in `pipelines.yaml`. UI uses sidebar master/detail with a `dash-cytoscape` DAG.

**Tech Stack:** Python 3.11+, Dash, dash-cytoscape, pandas, pyarrow, pyyaml, pytest.

**Spec:** [docs/superpowers/specs/2026-05-12-dash-pipeline-design.md](../specs/2026-05-12-dash-pipeline-design.md)

---

## File Layout (target end-state)

```
dash-pipeline/
├── app.py                       # Dash app: layout + callbacks
├── runner.py                    # Pipeline executor (thread pool + subprocess)
├── state.py                     # SQLite wrapper
├── parquet_meta.py              # Parquet timestamp + size reader (cached)
├── config.py                    # Load + validate pipelines.yaml
├── pipelines.yaml               # User-edited pipeline config
├── programs/                    # Example dummy scripts
├── data/                        # Parquet outputs (gitignored)
├── logs/                        # Run logs (gitignored)
├── tests/
│   ├── conftest.py
│   ├── fixtures/scripts/        # Tiny scripts used by runner tests
│   ├── test_config.py
│   ├── test_state.py
│   ├── test_parquet_meta.py
│   └── test_runner.py
├── requirements.txt
├── pyproject.toml               # pytest config
└── README.md
```

Each module has one responsibility and a narrow interface:

| Module | Public API |
|---|---|
| `config.py` | `load_config(path) -> Config`; `Config.pipelines`, `Config.programs`, `Config.topo_sort(pipeline_id)` |
| `state.py` | `State(db_path)`; `.start_run`, `.start_step`, `.finish_step`, `.finish_run`, `.recover_orphans`, `.get_program_status`, `.list_recent_runs` |
| `parquet_meta.py` | `get_meta(path, timestamp_column) -> ParquetMeta` (cached) |
| `runner.py` | `Runner(config, state)`; `.run_pipeline(id) -> run_id`, `.run_program(id) -> run_id`, `.is_pipeline_running(id)` |
| `app.py` | `make_app(config, state, runner) -> dash.Dash` |

---

## Task 1: Project skeleton + dependencies

**Files:**
- Create: `requirements.txt`
- Create: `pyproject.toml`
- Create: `programs/.gitkeep`
- Create: `data/.gitkeep`
- Create: `logs/.gitkeep`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/fixtures/scripts/.gitkeep`

- [ ] **Step 1: Create `requirements.txt`**

```
dash>=2.17
dash-cytoscape>=1.0
pandas>=2.2
pyarrow>=15
pyyaml>=6.0
pytest>=8.0
```

- [ ] **Step 2: Create `pyproject.toml`** (only pytest config — keep tooling minimal)

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra"
```

- [ ] **Step 3: Create empty placeholder dirs via `.gitkeep`**

Write a single empty file `.gitkeep` into `programs/`, `data/`, `logs/`, `tests/fixtures/scripts/`. Plus `tests/__init__.py` (empty).

- [ ] **Step 4: Create `tests/conftest.py`** — shared fixture for a tmp project root

```python
import pathlib
import pytest


@pytest.fixture
def project_root(tmp_path: pathlib.Path) -> pathlib.Path:
    (tmp_path / "programs").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "logs").mkdir()
    return tmp_path
```

- [ ] **Step 5: Install dependencies and verify pytest finds nothing yet**

Run: `python -m pip install -r requirements.txt && python -m pytest`
Expected: `no tests ran` (exit code 5 is fine — pytest's "no tests collected").

- [ ] **Step 6: Commit**

```bash
git add requirements.txt pyproject.toml programs/.gitkeep data/.gitkeep logs/.gitkeep tests/__init__.py tests/conftest.py tests/fixtures/scripts/.gitkeep
git commit -m "chore: project skeleton and dev deps"
```

---

## Task 2: `config.py` — load YAML into typed objects

**Files:**
- Create: `config.py`
- Create: `tests/test_config.py`
- Create: `tests/fixtures/pipelines_valid.yaml`

- [ ] **Step 1: Write the fixture YAML** — `tests/fixtures/pipelines_valid.yaml`

```yaml
programs:
  load_orders:
    script: programs/load_orders.py
    parquet: data/orders.parquet
    timestamp_column: timestamp

  transform_orders:
    script: programs/transform_orders.py
    parquet: data/orders_clean.parquet
    timestamp_column: timestamp
    timeout_seconds: 60

pipelines:
  daily_orders:
    name: "Tägliche Order-Verarbeitung"
    description: "Lädt und transformiert."
    steps:
      - load_orders
      - transform_orders
```

- [ ] **Step 2: Write failing tests** — `tests/test_config.py`

```python
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
```

- [ ] **Step 3: Run tests — expect failure**

Run: `python -m pytest tests/test_config.py -v`
Expected: ImportError (`No module named 'config'`).

- [ ] **Step 4: Implement `config.py`**

```python
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class Program:
    id: str
    script: str
    parquet: str
    timestamp_column: str
    timeout_seconds: int = 3600


@dataclass
class Step:
    program: str
    needs: list[str] = field(default_factory=list)


@dataclass
class Pipeline:
    id: str
    name: str
    description: str
    steps: list[Step]


@dataclass
class Config:
    programs: dict[str, Program]
    pipelines: dict[str, Pipeline]


def _parse_step(raw: Any, prev_program: str | None) -> Step:
    if isinstance(raw, str):
        needs = [prev_program] if prev_program else []
        return Step(program=raw, needs=needs)
    if isinstance(raw, dict) and "program" in raw:
        return Step(program=raw["program"], needs=list(raw.get("needs", [])))
    raise ValueError(f"Invalid step entry: {raw!r}")


def load_config(path: str | Path) -> Config:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    programs = {
        pid: Program(
            id=pid,
            script=p["script"],
            parquet=p["parquet"],
            timestamp_column=p["timestamp_column"],
            timeout_seconds=p.get("timeout_seconds", 3600),
        )
        for pid, p in (data.get("programs") or {}).items()
    }
    pipelines: dict[str, Pipeline] = {}
    for pid, p in (data.get("pipelines") or {}).items():
        steps: list[Step] = []
        prev: str | None = None
        for raw in p["steps"]:
            step = _parse_step(raw, prev)
            steps.append(step)
            prev = step.program
        pipelines[pid] = Pipeline(
            id=pid, name=p["name"], description=p.get("description", ""), steps=steps
        )
    return Config(programs=programs, pipelines=pipelines)
```

- [ ] **Step 5: Run tests — expect pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add config.py tests/test_config.py tests/fixtures/pipelines_valid.yaml
git commit -m "feat(config): load pipelines.yaml into typed objects"
```

---

## Task 3: `config.py` — validation (missing refs, cycles, topo-sort)

**Files:**
- Modify: `config.py` — add `Config.topo_sort` and `validate()` raising on errors
- Modify: `tests/test_config.py` — add failure-case tests
- Create: `tests/fixtures/pipelines_missing_ref.yaml`
- Create: `tests/fixtures/pipelines_cycle.yaml`

- [ ] **Step 1: Write fixture for missing program reference**

`tests/fixtures/pipelines_missing_ref.yaml`:

```yaml
programs:
  load_orders:
    script: programs/load_orders.py
    parquet: data/orders.parquet
    timestamp_column: timestamp

pipelines:
  broken:
    name: "Broken"
    steps:
      - load_orders
      - nonexistent_program
```

- [ ] **Step 2: Write fixture for cycle**

`tests/fixtures/pipelines_cycle.yaml`:

```yaml
programs:
  a: {script: a.py, parquet: a.parquet, timestamp_column: ts}
  b: {script: b.py, parquet: b.parquet, timestamp_column: ts}

pipelines:
  loop:
    name: "Loop"
    steps:
      - program: a
        needs: [b]
      - program: b
        needs: [a]
```

- [ ] **Step 3: Add failing tests** to `tests/test_config.py`

```python
import pytest

from config import load_config, ConfigError


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
```

- [ ] **Step 4: Run tests — expect failure** (missing `ConfigError`, missing `topo_sort`)

Run: `python -m pytest tests/test_config.py -v`

- [ ] **Step 5: Implement validation and topo-sort** — additions to `config.py`

Add at top (after `import yaml`):

```python
class ConfigError(ValueError):
    pass
```

Replace `load_config` ending — after building `pipelines` dict, before `return Config(...)`:

```python
    cfg = Config(programs=programs, pipelines=pipelines)
    _validate(cfg)
    return cfg


def _validate(cfg: Config) -> None:
    for pipeline in cfg.pipelines.values():
        step_ids = {s.program for s in pipeline.steps}
        for step in pipeline.steps:
            if step.program not in cfg.programs:
                raise ConfigError(
                    f"Pipeline {pipeline.id!r} references unknown program "
                    f"{step.program!r}"
                )
            for dep in step.needs:
                if dep not in step_ids:
                    raise ConfigError(
                        f"Pipeline {pipeline.id!r}: step {step.program!r} "
                        f"needs {dep!r} which is not in this pipeline"
                    )
        # Cycle detection via topo-sort attempt
        try:
            _topo_sort_steps(pipeline.steps)
        except ConfigError as exc:
            raise ConfigError(f"Pipeline {pipeline.id!r}: {exc}") from exc


def _topo_sort_steps(steps: list[Step]) -> list[str]:
    remaining = {s.program: set(s.needs) for s in steps}
    order: list[str] = []
    while remaining:
        ready = [p for p, deps in remaining.items() if not deps]
        if not ready:
            raise ConfigError(f"cycle in steps {list(remaining)}")
        for p in ready:
            order.append(p)
            del remaining[p]
            for deps in remaining.values():
                deps.discard(p)
    return order
```

Add method on `Config` (replace the `@dataclass class Config:` block):

```python
@dataclass
class Config:
    programs: dict[str, Program]
    pipelines: dict[str, Pipeline]

    def topo_sort(self, pipeline_id: str) -> list[str]:
        return _topo_sort_steps(self.pipelines[pipeline_id].steps)
```

- [ ] **Step 6: Run tests — expect pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: 6 passed.

- [ ] **Step 7: Commit**

```bash
git add config.py tests/test_config.py tests/fixtures/pipelines_missing_ref.yaml tests/fixtures/pipelines_cycle.yaml
git commit -m "feat(config): validate references and detect cycles"
```

---

## Task 4: `state.py` — schema, run/step CRUD

**Files:**
- Create: `state.py`
- Create: `tests/test_state.py`

- [ ] **Step 1: Write failing tests** — `tests/test_state.py`

```python
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from state import State, RunStatus, StepStatus


@pytest.fixture
def state(tmp_path: Path) -> State:
    return State(tmp_path / "runs.sqlite")


def test_create_pipeline_run_returns_id(state: State):
    run_id = state.start_run(pipeline_id="daily_orders", program_id=None)
    assert isinstance(run_id, int) and run_id > 0
    runs = state.list_recent_runs(pipeline_id="daily_orders", limit=10)
    assert len(runs) == 1
    assert runs[0].status == RunStatus.RUNNING


def test_start_and_finish_step(state: State):
    run_id = state.start_run(pipeline_id="p", program_id=None)
    step_id = state.start_step(run_id, program_id="load_orders",
                               log_path="logs/1/load_orders.log")
    state.finish_step(step_id, status=StepStatus.SUCCESS, exit_code=0)
    steps = state.list_steps(run_id)
    assert len(steps) == 1
    assert steps[0].status == StepStatus.SUCCESS
    assert steps[0].exit_code == 0
    assert steps[0].ended_at is not None


def test_finish_run_marks_status(state: State):
    run_id = state.start_run(pipeline_id="p", program_id=None)
    state.finish_run(run_id, status=RunStatus.SUCCESS)
    runs = state.list_recent_runs(pipeline_id="p", limit=1)
    assert runs[0].status == RunStatus.SUCCESS
    assert runs[0].ended_at is not None


def test_get_program_status_returns_latest(state: State):
    run_id = state.start_run(pipeline_id="p", program_id=None)
    step_id = state.start_step(run_id, program_id="load_orders", log_path="x")
    state.finish_step(step_id, status=StepStatus.FAILED, exit_code=1)
    # Newer run that succeeds
    run2 = state.start_run(pipeline_id="p", program_id=None)
    step2 = state.start_step(run2, program_id="load_orders", log_path="y")
    state.finish_step(step2, status=StepStatus.SUCCESS, exit_code=0)
    assert state.get_program_status("load_orders") == StepStatus.SUCCESS


def test_get_program_status_idle_when_no_runs(state: State):
    assert state.get_program_status("never_ran") == StepStatus.IDLE
```

- [ ] **Step 2: Run tests — expect failure**

Run: `python -m pytest tests/test_state.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement `state.py`**

```python
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class StepStatus(str, Enum):
    IDLE = "idle"          # synthetic; never stored
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class RunRow:
    id: int
    pipeline_id: str | None
    program_id: str | None
    status: RunStatus
    started_at: str
    ended_at: str | None


@dataclass
class StepRow:
    id: int
    run_id: int
    program_id: str
    status: StepStatus
    started_at: str | None
    ended_at: str | None
    exit_code: int | None
    log_path: str | None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT,
    program_id TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    triggered_by TEXT NOT NULL DEFAULT 'ui'
);

CREATE TABLE IF NOT EXISTS steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    program_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    exit_code INTEGER,
    log_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON runs(pipeline_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id);
CREATE INDEX IF NOT EXISTS idx_steps_program ON steps(program_id, started_at DESC);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class State:
    def __init__(self, db_path: Path | str) -> None:
        self._path = str(db_path)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, isolation_level=None, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # --- runs ---

    def start_run(self, *, pipeline_id: str | None, program_id: str | None) -> int:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO runs(pipeline_id, program_id, status, started_at) "
                "VALUES (?, ?, ?, ?)",
                (pipeline_id, program_id, RunStatus.RUNNING.value, _now()),
            )
            return int(cur.lastrowid)

    def finish_run(self, run_id: int, *, status: RunStatus) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE runs SET status=?, ended_at=? WHERE id=?",
                (status.value, _now(), run_id),
            )

    def list_recent_runs(
        self, *, pipeline_id: str | None, limit: int = 10
    ) -> list[RunRow]:
        with self._connect() as conn:
            if pipeline_id is None:
                rows = conn.execute(
                    "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM runs WHERE pipeline_id=? "
                    "ORDER BY started_at DESC LIMIT ?",
                    (pipeline_id, limit),
                ).fetchall()
        return [
            RunRow(
                id=r["id"],
                pipeline_id=r["pipeline_id"],
                program_id=r["program_id"],
                status=RunStatus(r["status"]),
                started_at=r["started_at"],
                ended_at=r["ended_at"],
            )
            for r in rows
        ]

    # --- steps ---

    def start_step(self, run_id: int, *, program_id: str, log_path: str) -> int:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO steps(run_id, program_id, status, started_at, log_path) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, program_id, StepStatus.RUNNING.value, _now(), log_path),
            )
            return int(cur.lastrowid)

    def finish_step(
        self, step_id: int, *, status: StepStatus, exit_code: int | None
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE steps SET status=?, ended_at=?, exit_code=? WHERE id=?",
                (status.value, _now(), exit_code, step_id),
            )

    def list_steps(self, run_id: int) -> list[StepRow]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM steps WHERE run_id=? ORDER BY id",
                (run_id,),
            ).fetchall()
        return [
            StepRow(
                id=r["id"],
                run_id=r["run_id"],
                program_id=r["program_id"],
                status=StepStatus(r["status"]),
                started_at=r["started_at"],
                ended_at=r["ended_at"],
                exit_code=r["exit_code"],
                log_path=r["log_path"],
            )
            for r in rows
        ]

    # --- queries for UI ---

    def get_program_status(self, program_id: str) -> StepStatus:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM steps WHERE program_id=? "
                "ORDER BY started_at DESC LIMIT 1",
                (program_id,),
            ).fetchone()
        if row is None:
            return StepStatus.IDLE
        return StepStatus(row["status"])
```

- [ ] **Step 4: Run tests — expect pass**

Run: `python -m pytest tests/test_state.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add state.py tests/test_state.py
git commit -m "feat(state): SQLite schema and run/step CRUD"
```

---

## Task 5: `state.py` — orphan recovery on startup

**Files:**
- Modify: `state.py` — add `recover_orphans`
- Modify: `tests/test_state.py` — add recovery test

- [ ] **Step 1: Write failing test** (append to `tests/test_state.py`)

```python
def test_recover_orphans_marks_running_as_failed(state: State):
    run_id = state.start_run(pipeline_id="p", program_id=None)
    step_id = state.start_step(run_id, program_id="x", log_path="x.log")
    # Simulate dashboard crash: both run and step still 'running'

    n = state.recover_orphans()
    assert n == 2  # one run, one step

    steps = state.list_steps(run_id)
    assert steps[0].status == StepStatus.FAILED
    runs = state.list_recent_runs(pipeline_id="p", limit=1)
    assert runs[0].status == RunStatus.FAILED
    assert runs[0].ended_at is not None
```

- [ ] **Step 2: Run test — expect failure** (`AttributeError: recover_orphans`)

Run: `python -m pytest tests/test_state.py::test_recover_orphans_marks_running_as_failed -v`

- [ ] **Step 3: Implement `recover_orphans`** — add method to `State`

```python
    def recover_orphans(self) -> int:
        with self._lock, self._connect() as conn:
            now = _now()
            cur1 = conn.execute(
                "UPDATE steps SET status=?, ended_at=? WHERE status=?",
                (StepStatus.FAILED.value, now, StepStatus.RUNNING.value),
            )
            cur2 = conn.execute(
                "UPDATE runs SET status=?, ended_at=? WHERE status=?",
                (RunStatus.FAILED.value, now, RunStatus.RUNNING.value),
            )
            return (cur1.rowcount or 0) + (cur2.rowcount or 0)
```

- [ ] **Step 4: Run test — expect pass**

Run: `python -m pytest tests/test_state.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add state.py tests/test_state.py
git commit -m "feat(state): recover orphaned runs/steps on startup"
```

---

## Task 6: `parquet_meta.py` — latest timestamp + size with cache

**Files:**
- Create: `parquet_meta.py`
- Create: `tests/test_parquet_meta.py`

- [ ] **Step 1: Write failing tests**

```python
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from parquet_meta import get_meta, ParquetMeta


def _write_parquet(path: Path, timestamps: list[datetime]):
    df = pd.DataFrame({"timestamp": timestamps, "value": list(range(len(timestamps)))})
    df.to_parquet(path)


def test_returns_latest_timestamp_and_size(tmp_path: Path):
    p = tmp_path / "x.parquet"
    _write_parquet(p, [
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 5, 12, 9, 14, tzinfo=timezone.utc),
        datetime(2026, 3, 1, tzinfo=timezone.utc),
    ])
    meta = get_meta(p, "timestamp")
    assert meta.latest_timestamp == datetime(2026, 5, 12, 9, 14, tzinfo=timezone.utc)
    assert meta.size_bytes > 0
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
    pd.DataFrame({"timestamp": pd.Series([], dtype="datetime64[ns, UTC]")}).to_parquet(p)
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
```

- [ ] **Step 2: Run tests — expect failure**

Run: `python -m pytest tests/test_parquet_meta.py -v`

- [ ] **Step 3: Implement `parquet_meta.py`**

```python
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
    except KeyError:
        return ParquetMeta(None, size, f"no column: {column}")
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
```

- [ ] **Step 4: Update tests to clear cache between cases** — add a fixture at top of `tests/test_parquet_meta.py`

```python
@pytest.fixture(autouse=True)
def _reset_cache():
    from parquet_meta import clear_cache
    clear_cache()
    yield
    clear_cache()
```

- [ ] **Step 5: Run tests — expect pass**

Run: `python -m pytest tests/test_parquet_meta.py -v`
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add parquet_meta.py tests/test_parquet_meta.py
git commit -m "feat(parquet_meta): cached latest-timestamp reader"
```

---

## Task 7: `runner.py` — execute a single program

**Files:**
- Create: `runner.py`
- Create: `tests/test_runner.py`
- Create: `tests/fixtures/scripts/success.py`
- Create: `tests/fixtures/scripts/fail.py`

- [ ] **Step 1: Write fixture scripts**

`tests/fixtures/scripts/success.py`:

```python
import sys
print("hello from success", flush=True)
sys.exit(0)
```

`tests/fixtures/scripts/fail.py`:

```python
import sys
print("oops", file=sys.stderr, flush=True)
sys.exit(2)
```

- [ ] **Step 2: Write failing tests** — `tests/test_runner.py`

```python
from pathlib import Path

import pytest

from config import Config, Program, Pipeline, Step
from runner import Runner
from state import State, RunStatus, StepStatus


FIXTURES = Path(__file__).parent / "fixtures" / "scripts"


def _cfg(programs: dict[str, Program], pipelines: dict[str, Pipeline]) -> Config:
    return Config(programs=programs, pipelines=pipelines)


def _program(pid: str, script: str, tmp_path: Path, timeout: int = 30) -> Program:
    return Program(
        id=pid,
        script=str(FIXTURES / script),
        parquet=str(tmp_path / f"{pid}.parquet"),
        timestamp_column="timestamp",
        timeout_seconds=timeout,
    )


def test_run_single_program_success(tmp_path: Path):
    cfg = _cfg(
        programs={"ok": _program("ok", "success.py", tmp_path)},
        pipelines={},
    )
    state = State(tmp_path / "runs.sqlite")
    runner = Runner(cfg, state, logs_dir=tmp_path / "logs",
                    project_root=tmp_path)
    run_id = runner.run_program("ok")
    runner.wait_all()
    steps = state.list_steps(run_id)
    assert len(steps) == 1
    assert steps[0].status == StepStatus.SUCCESS
    assert steps[0].exit_code == 0
    # Log file exists and contains stdout
    log = Path(steps[0].log_path).read_text(encoding="utf-8")
    assert "hello from success" in log


def test_run_single_program_failure(tmp_path: Path):
    cfg = _cfg(
        programs={"bad": _program("bad", "fail.py", tmp_path)},
        pipelines={},
    )
    state = State(tmp_path / "runs.sqlite")
    runner = Runner(cfg, state, logs_dir=tmp_path / "logs",
                    project_root=tmp_path)
    run_id = runner.run_program("bad")
    runner.wait_all()
    steps = state.list_steps(run_id)
    assert steps[0].status == StepStatus.FAILED
    assert steps[0].exit_code == 2
    runs = state.list_recent_runs(pipeline_id=None, limit=1)
    assert runs[0].status == RunStatus.FAILED
```

- [ ] **Step 3: Run tests — expect failure** (no Runner module)

- [ ] **Step 4: Implement `runner.py`**

```python
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from config import Config
from state import RunStatus, State, StepStatus


class Runner:
    def __init__(
        self,
        config: Config,
        state: State,
        *,
        logs_dir: Path,
        project_root: Path,
        max_workers: int = 8,
    ) -> None:
        self._config = config
        self._state = state
        self._logs_dir = Path(logs_dir)
        self._project_root = Path(project_root)
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self._active_pipelines: set[str] = set()
        self._pending: list[Future] = []

    # --- public API ---

    def run_program(self, program_id: str) -> int:
        run_id = self._state.start_run(pipeline_id=None, program_id=program_id)
        fut = self._pool.submit(self._execute_single, run_id, program_id)
        self._pending.append(fut)
        return run_id

    def wait_all(self) -> None:
        """Block until every submitted future has finished. Test-only."""
        for fut in list(self._pending):
            fut.result()
        self._pending = [f for f in self._pending if not f.done()]

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)

    # --- internals ---

    def _run_subprocess(
        self, program_id: str, log_path: Path, timeout: int
    ) -> int:
        program = self._config.programs[program_id]
        script_path = self._project_root / program.script if not Path(
            program.script
        ).is_absolute() else Path(program.script)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w", encoding="utf-8") as log_file:
            proc = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=str(self._project_root),
            )
            try:
                return proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                log_file.write(f"\n[runner] TIMEOUT after {timeout}s\n")
                return -1

    def _execute_single(self, run_id: int, program_id: str) -> None:
        log_path = self._logs_dir / str(run_id) / f"{program_id}.log"
        step_id = self._state.start_step(
            run_id, program_id=program_id, log_path=str(log_path)
        )
        program = self._config.programs[program_id]
        try:
            exit_code = self._run_subprocess(
                program_id, log_path, program.timeout_seconds
            )
        except Exception as exc:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(f"[runner] crashed: {exc!r}\n", encoding="utf-8")
            self._state.finish_step(
                step_id, status=StepStatus.FAILED, exit_code=None
            )
            self._state.finish_run(run_id, status=RunStatus.FAILED)
            return
        status = StepStatus.SUCCESS if exit_code == 0 else StepStatus.FAILED
        self._state.finish_step(step_id, status=status, exit_code=exit_code)
        self._state.finish_run(
            run_id,
            status=RunStatus.SUCCESS if status == StepStatus.SUCCESS else RunStatus.FAILED,
        )
```

- [ ] **Step 5: Run tests — expect pass**

Run: `python -m pytest tests/test_runner.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add runner.py tests/test_runner.py tests/fixtures/scripts/success.py tests/fixtures/scripts/fail.py
git commit -m "feat(runner): execute single program via subprocess"
```

---

## Task 8: `runner.py` — execute a pipeline (linear + DAG, skipped on failure)

**Files:**
- Modify: `runner.py` — add `run_pipeline`, `is_pipeline_running`
- Modify: `tests/test_runner.py` — add pipeline tests

- [ ] **Step 1: Add failing tests** to `tests/test_runner.py`

```python
def test_run_linear_pipeline_all_success(tmp_path: Path):
    programs = {
        "a": _program("a", "success.py", tmp_path),
        "b": _program("b", "success.py", tmp_path),
    }
    pipelines = {
        "p": Pipeline(id="p", name="P", description="",
                      steps=[Step("a", []), Step("b", ["a"])]),
    }
    cfg = _cfg(programs, pipelines)
    state = State(tmp_path / "runs.sqlite")
    runner = Runner(cfg, state, logs_dir=tmp_path / "logs",
                    project_root=tmp_path)
    run_id = runner.run_pipeline("p")
    runner.wait_all()

    steps = state.list_steps(run_id)
    assert [s.status for s in steps] == [StepStatus.SUCCESS, StepStatus.SUCCESS]
    assert state.list_recent_runs(pipeline_id="p", limit=1)[0].status == RunStatus.SUCCESS


def test_pipeline_skips_downstream_on_failure(tmp_path: Path):
    programs = {
        "a": _program("a", "fail.py", tmp_path),
        "b": _program("b", "success.py", tmp_path),
        "c": _program("c", "success.py", tmp_path),
    }
    pipelines = {
        "p": Pipeline(id="p", name="P", description="",
                      steps=[Step("a", []), Step("b", ["a"]), Step("c", ["b"])]),
    }
    cfg = _cfg(programs, pipelines)
    state = State(tmp_path / "runs.sqlite")
    runner = Runner(cfg, state, logs_dir=tmp_path / "logs",
                    project_root=tmp_path)
    run_id = runner.run_pipeline("p")
    runner.wait_all()

    steps = {s.program_id: s for s in state.list_steps(run_id)}
    assert steps["a"].status == StepStatus.FAILED
    assert steps["b"].status == StepStatus.SKIPPED
    assert steps["c"].status == StepStatus.SKIPPED
    assert state.list_recent_runs(pipeline_id="p", limit=1)[0].status == RunStatus.FAILED


def test_pipeline_dag_parallel_branches(tmp_path: Path):
    programs = {
        "a": _program("a", "success.py", tmp_path),
        "b": _program("b", "success.py", tmp_path),
        "c": _program("c", "success.py", tmp_path),
        "d": _program("d", "success.py", tmp_path),
    }
    pipelines = {
        "p": Pipeline(id="p", name="P", description="",
                      steps=[
                          Step("a", []),
                          Step("b", ["a"]),
                          Step("c", ["a"]),
                          Step("d", ["b", "c"]),
                      ]),
    }
    cfg = _cfg(programs, pipelines)
    state = State(tmp_path / "runs.sqlite")
    runner = Runner(cfg, state, logs_dir=tmp_path / "logs",
                    project_root=tmp_path)
    run_id = runner.run_pipeline("p")
    runner.wait_all()
    steps = state.list_steps(run_id)
    assert all(s.status == StepStatus.SUCCESS for s in steps)


def test_is_pipeline_running_guard(tmp_path: Path):
    programs = {"a": _program("a", "success.py", tmp_path)}
    pipelines = {"p": Pipeline(id="p", name="P", description="",
                               steps=[Step("a", [])])}
    cfg = _cfg(programs, pipelines)
    state = State(tmp_path / "runs.sqlite")
    runner = Runner(cfg, state, logs_dir=tmp_path / "logs",
                    project_root=tmp_path)
    # Mark active before submission completes
    assert runner.is_pipeline_running("p") is False
    runner.run_pipeline("p")
    # While submitted, before completion, it should be flagged
    # (the test asserts the second start raises immediately)
    with pytest.raises(RuntimeError, match="already running"):
        runner.run_pipeline("p")
    runner.wait_all()
```

- [ ] **Step 2: Run tests — expect failure**

- [ ] **Step 3: Extend `runner.py`** — add inside class `Runner`:

```python
    # --- pipeline execution ---

    def is_pipeline_running(self, pipeline_id: str) -> bool:
        with self._lock:
            return pipeline_id in self._active_pipelines

    def run_pipeline(self, pipeline_id: str) -> int:
        with self._lock:
            if pipeline_id in self._active_pipelines:
                raise RuntimeError(f"Pipeline {pipeline_id!r} is already running")
            self._active_pipelines.add(pipeline_id)
        run_id = self._state.start_run(pipeline_id=pipeline_id, program_id=None)
        fut = self._pool.submit(self._execute_pipeline, run_id, pipeline_id)
        self._pending.append(fut)
        return run_id

    def _execute_pipeline(self, run_id: int, pipeline_id: str) -> None:
        try:
            self._execute_pipeline_inner(run_id, pipeline_id)
        finally:
            with self._lock:
                self._active_pipelines.discard(pipeline_id)

    def _execute_pipeline_inner(self, run_id: int, pipeline_id: str) -> None:
        pipeline = self._config.pipelines[pipeline_id]
        steps_by_program = {s.program: s for s in pipeline.steps}
        # Pre-create step rows so the UI can show them as 'pending' immediately
        step_ids: dict[str, int] = {}
        for s in pipeline.steps:
            sid = self._state.start_step(
                run_id,
                program_id=s.program,
                log_path=str(self._logs_dir / str(run_id) / f"{s.program}.log"),
            )
            # Mark as pending until we start it
            self._state.finish_step(sid, status=StepStatus.PENDING, exit_code=None)
            step_ids[s.program] = sid

        completed: dict[str, StepStatus] = {}
        in_flight: dict[str, Future] = {}

        def ready() -> list[str]:
            return [
                p
                for p, s in steps_by_program.items()
                if p not in completed
                and p not in in_flight
                and all(dep in completed for dep in s.needs)
            ]

        def has_failed_dep(prog: str) -> bool:
            return any(
                completed.get(dep) in (StepStatus.FAILED, StepStatus.SKIPPED)
                for dep in steps_by_program[prog].needs
            )

        while len(completed) < len(pipeline.steps):
            # Mark anything blocked by a failed upstream as skipped
            for p in list(steps_by_program):
                if p in completed or p in in_flight:
                    continue
                if has_failed_dep(p):
                    self._state.finish_step(
                        step_ids[p], status=StepStatus.SKIPPED, exit_code=None
                    )
                    completed[p] = StepStatus.SKIPPED

            for prog in ready():
                in_flight[prog] = self._pool.submit(
                    self._execute_step_in_pipeline, run_id, prog, step_ids[prog]
                )

            if not in_flight:
                break  # nothing ready; everything else was skipped

            # Wait for at least one to finish
            done_progs: list[str] = []
            for prog, fut in list(in_flight.items()):
                if fut.done():
                    completed[prog] = fut.result()
                    done_progs.append(prog)
            if not done_progs:
                # Block on whichever finishes first
                for prog, fut in list(in_flight.items()):
                    completed[prog] = fut.result()
                    done_progs.append(prog)
                    break
            for prog in done_progs:
                del in_flight[prog]

        any_failed = any(
            s in (StepStatus.FAILED, StepStatus.SKIPPED) for s in completed.values()
        )
        self._state.finish_run(
            run_id,
            status=RunStatus.FAILED if any_failed else RunStatus.SUCCESS,
        )

    def _execute_step_in_pipeline(
        self, run_id: int, program_id: str, step_id: int
    ) -> StepStatus:
        # Re-mark the step as running (we set it pending earlier)
        self._state.finish_step(step_id, status=StepStatus.RUNNING, exit_code=None)
        log_path = self._logs_dir / str(run_id) / f"{program_id}.log"
        program = self._config.programs[program_id]
        try:
            exit_code = self._run_subprocess(
                program_id, log_path, program.timeout_seconds
            )
        except Exception as exc:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(f"[runner] crashed: {exc!r}\n", encoding="utf-8")
            self._state.finish_step(step_id, status=StepStatus.FAILED, exit_code=None)
            return StepStatus.FAILED
        status = StepStatus.SUCCESS if exit_code == 0 else StepStatus.FAILED
        self._state.finish_step(step_id, status=status, exit_code=exit_code)
        return status
```

Note: `finish_step` is used to set intermediate statuses (`pending`, `running`) — it just `UPDATE`s the row. The earlier test `test_start_and_finish_step` still passes because the final write wins.

- [ ] **Step 4: Run all tests — expect pass**

Run: `python -m pytest -v`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add runner.py tests/test_runner.py
git commit -m "feat(runner): execute pipelines with DAG parallelism and skip-on-failure"
```

---

## Task 9: `app.py` — layout skeleton (sidebar + detail) with helpers

**Files:**
- Create: `app.py`
- Create: `tests/test_app_helpers.py`

- [ ] **Step 1: Write failing tests for pure data-shape helpers**

```python
from pathlib import Path

import pytest

from config import Config, Program, Pipeline, Step
from state import State, StepStatus
from app import build_cytoscape_elements, format_status_pill, STATUS_COLORS


def _cfg() -> Config:
    return Config(
        programs={
            "a": Program("a", "a.py", "a.parquet", "timestamp"),
            "b": Program("b", "b.py", "b.parquet", "timestamp"),
            "c": Program("c", "c.py", "c.parquet", "timestamp"),
        },
        pipelines={
            "p": Pipeline("p", "P", "", [Step("a", []), Step("b", ["a"]), Step("c", ["a"])]),
        },
    )


def test_cytoscape_elements_have_nodes_and_edges():
    cfg = _cfg()
    statuses = {"a": StepStatus.SUCCESS, "b": StepStatus.RUNNING, "c": StepStatus.IDLE}
    elements = build_cytoscape_elements(cfg.pipelines["p"], statuses)
    node_ids = [e["data"]["id"] for e in elements if "source" not in e.get("data", {})]
    edges = [e for e in elements if "source" in e.get("data", {})]
    assert set(node_ids) == {"a", "b", "c"}
    assert {(e["data"]["source"], e["data"]["target"]) for e in edges} == {
        ("a", "b"), ("a", "c"),
    }
    # Status reflected on node
    a_node = next(e for e in elements if e["data"].get("id") == "a")
    assert a_node["data"]["status"] == "success"


def test_format_status_pill_returns_styled_span():
    span = format_status_pill(StepStatus.FAILED)
    # Dash returns a html.Span — we just check the kwargs are sensible
    assert span.children == "failed"
    assert STATUS_COLORS[StepStatus.FAILED] in span.style["background"]
```

- [ ] **Step 2: Run tests — expect failure**

- [ ] **Step 3: Implement layout skeleton + helpers** — `app.py`

```python
from pathlib import Path

import dash
import dash_cytoscape as cyto
from dash import Dash, Input, Output, State as DashState, dcc, html, callback_context, no_update

from config import Config, Pipeline
from parquet_meta import get_meta
from runner import Runner
from state import RunStatus, State, StepStatus


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


def _sidebar(config: Config) -> html.Div:
    pipeline_items = [
        html.Div(
            id={"type": "pipeline-item", "id": pid},
            n_clicks=0,
            children=[
                html.Strong(p.name),
                html.Div(p.description, style={"fontSize": "11px", "color": "#6b7280"}),
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
                      style={"flex": 1, "padding": "16px", "overflowY": "auto"})],
            style={"display": "flex", "height": "calc(100vh - 44px)"},
        ),
    ])
    _register_callbacks(app, config, state, runner)
    return app


def _register_callbacks(app: Dash, config: Config, state: State, runner: Runner) -> None:
    # Filled in later tasks
    pass
```

- [ ] **Step 4: Run tests — expect pass**

Run: `python -m pytest tests/test_app_helpers.py -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_app_helpers.py
git commit -m "feat(app): layout skeleton with sidebar and cytoscape helpers"
```

---

## Task 10: `app.py` — selection callback + detail rendering

**Files:**
- Modify: `app.py` — wire selection from sidebar to detail panel; render pipeline detail
- Modify: `tests/test_app_helpers.py` — test the detail-render helper

- [ ] **Step 1: Add failing test** for the detail-render helper

```python
def test_render_pipeline_detail_contains_graph_and_table():
    cfg = _cfg()
    from app import render_pipeline_detail
    statuses = {"a": StepStatus.SUCCESS, "b": StepStatus.IDLE, "c": StepStatus.IDLE}
    parquet = {"a": None, "b": None, "c": None}  # ParquetMeta or None
    sizes = {"a": None, "b": None, "c": None}
    runs: list = []
    rendered = render_pipeline_detail(cfg.pipelines["p"], statuses, parquet, runs)
    # The returned component tree should include a cytoscape graph
    # and a table mentioning the three programs
    serialized = repr(rendered)
    assert "Cytoscape" in serialized
    assert "a" in serialized and "b" in serialized and "c" in serialized
```

- [ ] **Step 2: Run test — expect failure**

- [ ] **Step 3: Implement `render_pipeline_detail` and the selection callback**

Add to `app.py` (above `_register_callbacks`):

```python
from datetime import datetime

from state import RunRow
from parquet_meta import ParquetMeta


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
            html.Td(_format_ts(parquet[prog].latest_timestamp if parquet.get(prog) else None)),
            html.Td(_format_size(parquet[prog].size_bytes if parquet.get(prog) else None)),
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
        html.Button("▶ Run Pipeline", id="run-pipeline-btn", n_clicks=0,
                    **{"data-pipeline-id": pipeline.id},
                    style={"padding": "6px 14px", "background": "#4f6df5",
                           "color": "#fff", "border": "none",
                           "borderRadius": "6px", "cursor": "pointer"}),
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
            html.Td(_format_ts(parquet[pid].latest_timestamp if parquet.get(pid) else None)),
            html.Td(_format_size(parquet[pid].size_bytes if parquet.get(pid) else None)),
            html.Td(html.Button("▶", id={"type": "run-program", "id": pid},
                                n_clicks=0,
                                style={"padding": "2px 8px", "background": "#4f6df5",
                                       "color": "#fff", "border": "none",
                                       "borderRadius": "4px", "cursor": "pointer"})),
        ])
        for pid in config.programs
    ]
    return html.Div([
        html.H3("Alle Programme"),
        html.Table(
            [html.Thead(html.Tr([html.Th(c) for c in
                                 ["Programm", "Status", "Letzter TS", "Größe", ""]]))]
            + [html.Tbody(rows)],
            style={"width": "100%", "borderCollapse": "collapse",
                   "fontSize": "13px"},
        ),
    ])
```

Replace `_register_callbacks`:

```python
def _register_callbacks(app: Dash, config: Config, state: State, runner: Runner) -> None:
    from dash import ALL

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
```

- [ ] **Step 4: Run tests — expect pass**

Run: `python -m pytest tests/test_app_helpers.py -v`

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_app_helpers.py
git commit -m "feat(app): selection callback + pipeline/programs detail rendering"
```

---

## Task 11: `app.py` — run-button + run-program callbacks

**Files:**
- Modify: `app.py`

- [ ] **Step 1: Extend `_register_callbacks`** — append inside the function:

```python
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
        return True  # disable; refresh callback re-renders shortly

    @app.callback(
        Output({"type": "run-program", "id": ALL}, "disabled"),
        Input({"type": "run-program", "id": ALL}, "n_clicks"),
        DashState({"type": "run-program", "id": ALL}, "id"),
        prevent_initial_call=True,
    )
    def on_run_program(clicks, ids):
        ctx = callback_context
        if not ctx.triggered:
            return [no_update] * len(ids)
        triggered = ctx.triggered_id
        if isinstance(triggered, dict) and triggered.get("type") == "run-program":
            runner.run_program(triggered["id"])
        return [no_update] * len(ids)
```

- [ ] **Step 2: Sanity check — start the app manually**

In a separate terminal, run:

```bash
python -c "from config import load_config; from state import State; from runner import Runner; from app import make_app; from pathlib import Path; cfg = load_config('tests/fixtures/pipelines_valid.yaml'); st = State(Path('runs.sqlite')); st.recover_orphans(); r = Runner(cfg, st, logs_dir=Path('logs'), project_root=Path('.')); make_app(cfg, st, r).run(debug=False, port=8051)"
```

Open `http://localhost:8051`. Click a pipeline in the sidebar; the detail panel should show the DAG and tables. Click "▶ Run Pipeline" — the button disables; after a few seconds, the auto-refresh paints nodes green or red. Stop with Ctrl-C, then `rm runs.sqlite` if you want a clean slate.

- [ ] **Step 3: Commit**

```bash
git add app.py
git commit -m "feat(app): pipeline and program run callbacks"
```

---

## Task 12: Node-click modal showing the latest log

**Files:**
- Modify: `app.py` — add modal layout, callback to fill it on cytoscape node tap
- Modify: `state.py` — add `get_latest_step_for_program` helper

- [ ] **Step 1: Write failing test for `state.get_latest_step_for_program`** (append to `tests/test_state.py`)

```python
def test_get_latest_step_for_program(state: State):
    run_id = state.start_run(pipeline_id="p", program_id=None)
    s1 = state.start_step(run_id, program_id="x", log_path="logs/1/x.log")
    state.finish_step(s1, status=StepStatus.SUCCESS, exit_code=0)
    run2 = state.start_run(pipeline_id="p", program_id=None)
    s2 = state.start_step(run2, program_id="x", log_path="logs/2/x.log")
    state.finish_step(s2, status=StepStatus.FAILED, exit_code=1)

    step = state.get_latest_step_for_program("x")
    assert step is not None
    assert step.log_path == "logs/2/x.log"
    assert step.status == StepStatus.FAILED
    assert state.get_latest_step_for_program("never_ran") is None
```

- [ ] **Step 2: Run — expect failure**

Run: `python -m pytest tests/test_state.py::test_get_latest_step_for_program -v`

- [ ] **Step 3: Implement helper in `state.py`** (add method to `State`):

```python
    def get_latest_step_for_program(self, program_id: str) -> StepRow | None:
        with self._connect() as conn:
            r = conn.execute(
                "SELECT * FROM steps WHERE program_id=? "
                "ORDER BY started_at DESC LIMIT 1",
                (program_id,),
            ).fetchone()
        if r is None:
            return None
        return StepRow(
            id=r["id"],
            run_id=r["run_id"],
            program_id=r["program_id"],
            status=StepStatus(r["status"]),
            started_at=r["started_at"],
            ended_at=r["ended_at"],
            exit_code=r["exit_code"],
            log_path=r["log_path"],
        )
```

- [ ] **Step 4: Run — expect pass**

Run: `python -m pytest tests/test_state.py -v`

- [ ] **Step 5: Add modal layout to `app.py`** — modify `make_app` to include modal elements at the bottom of `app.layout`:

Find the closing `]` of `app.layout = html.Div([...])` and insert before it:

```python
            # Modal for node-click log preview (hidden by default via style.display)
            html.Div(
                id="log-modal",
                children=[
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Strong(id="log-modal-title"),
                                    html.Button("✕", id="log-modal-close", n_clicks=0,
                                                style={"float": "right",
                                                       "background": "none",
                                                       "border": "none",
                                                       "fontSize": "16px",
                                                       "cursor": "pointer"}),
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
                               "borderRadius": "8px", "width": "min(800px, 90vw)",
                               "maxHeight": "80vh", "overflow": "auto"},
                    )
                ],
                style={"display": "none", "position": "fixed", "inset": "0",
                       "background": "rgba(0,0,0,0.4)",
                       "alignItems": "center", "justifyContent": "center",
                       "zIndex": 1000},
            ),
```

- [ ] **Step 6: Add callbacks for the modal** — append inside `_register_callbacks`:

```python
    @app.callback(
        Output("log-modal", "style"),
        Output("log-modal-title", "children"),
        Output("log-modal-body", "children"),
        Input("pipeline-dag", "tapNodeData"),
        Input("log-modal-close", "n_clicks"),
        prevent_initial_call=True,
    )
    def on_node_tap(tap_data, _close_clicks):
        ctx = callback_context
        triggered = ctx.triggered_id
        base_hidden = {"display": "none", "position": "fixed", "inset": "0",
                       "background": "rgba(0,0,0,0.4)",
                       "alignItems": "center", "justifyContent": "center",
                       "zIndex": 1000}
        base_shown = {**base_hidden, "display": "flex"}
        if triggered == "log-modal-close":
            return base_hidden, "", ""
        if not tap_data:
            return no_update, no_update, no_update
        program_id = tap_data["id"]
        latest = state.get_latest_step_for_program(program_id)
        if latest is None or not latest.log_path:
            body = f"Noch kein Lauf für {program_id!r}."
        else:
            try:
                body = Path(latest.log_path).read_text(encoding="utf-8", errors="replace")
            except FileNotFoundError:
                body = f"Log file not found: {latest.log_path}"
            if not body.strip():
                body = "(leeres Log)"
        title = f"{program_id} — {latest.status.value if latest else 'idle'}"
        return base_shown, title, body
```

- [ ] **Step 7: Smoke test the modal**

Run the app again (as in Task 11 Step 2), click a pipeline, then click a node in the DAG. The modal should appear with the latest log (or a "no run yet" message). The ✕ button closes it.

- [ ] **Step 8: Commit**

```bash
git add app.py state.py tests/test_state.py
git commit -m "feat(app): node-click modal showing latest program log"
```

---

## Task 13: Entry point (`__main__`) and example config

**Files:**
- Modify: `app.py` — `if __name__ == "__main__":` block
- Create: `pipelines.yaml` (example real config)
- Create: `programs/example_load.py`
- Create: `programs/example_transform.py`
- Create: `README.md`

- [ ] **Step 1: Add entry point to `app.py`** (append at bottom)

```python
if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="pipelines.yaml")
    parser.add_argument("--db", default="runs.sqlite")
    parser.add_argument("--logs", default="logs")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()

    cfg = load_config(args.config)
    st = State(args.db)
    n = st.recover_orphans()
    if n:
        print(f"[startup] recovered {n} orphaned run/step entries")
    runner = Runner(cfg, st, logs_dir=Path(args.logs), project_root=Path("."))
    app = make_app(cfg, st, runner)
    app.run(debug=False, port=args.port)
```

- [ ] **Step 2: Write example programs** — `programs/example_load.py`

```python
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
```

`programs/example_transform.py`:

```python
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
```

- [ ] **Step 3: Write `pipelines.yaml`**

```yaml
programs:
  example_load:
    script: programs/example_load.py
    parquet: data/example_orders.parquet
    timestamp_column: timestamp

  example_transform:
    script: programs/example_transform.py
    parquet: data/example_orders_clean.parquet
    timestamp_column: timestamp

pipelines:
  example_pipeline:
    name: "Example Order Pipeline"
    description: "Lädt eine Demo-Tabelle und transformiert sie."
    steps:
      - example_load
      - example_transform
```

- [ ] **Step 4: Write `README.md`**

```markdown
# dash-pipeline

Local Dash dashboard to start Python scripts (single or in chains) and monitor
their parquet outputs.

## Run

    python -m pip install -r requirements.txt
    python app.py --config pipelines.yaml

Open http://localhost:8050.

## Editing pipelines

Edit `pipelines.yaml`. Restart the app to pick up changes (or use the in-app
reload button if added later). See
`docs/superpowers/specs/2026-05-12-dash-pipeline-design.md` for the full spec.

## Tests

    python -m pytest
```

- [ ] **Step 5: Run example end-to-end**

```bash
python app.py --config pipelines.yaml
```

Open `http://localhost:8050`, click "Example Order Pipeline", click "▶ Run Pipeline". After ~5 seconds, both nodes should be green and the table should show recent timestamps. Stop with Ctrl-C.

- [ ] **Step 6: Final commit**

```bash
git add app.py pipelines.yaml programs/example_load.py programs/example_transform.py README.md
git commit -m "feat: entry point, example pipeline, README"
```

---

## Final verification

- [ ] **Run all tests** — `python -m pytest -v`. All green.
- [ ] **Smoke-test in browser** — see Task 13 Step 5.
- [ ] **Check the example pipeline path** end-to-end, including the parquet timestamps appearing in the program table.

---

## Self-Review (already applied)

- **Spec coverage:**
  - Section 3 Architecture → Tasks 4–8 (state, parquet_meta, runner) + Task 9–11 (app).
  - Section 4 YAML format → Tasks 2–3.
  - Section 5 State model + recovery → Tasks 4–5.
  - Section 6 UI layout (sidebar + detail, node-click modal) → Tasks 9–12.
  - Section 7 Execution logic (topo-sort, parallel, skipped, timeout, already-running guard) → Tasks 7–8.
  - Section 8 Tests → covered per task.
  - Section 9 Dependencies → Task 1.
- **Placeholders:** none — every step shows the actual code or command.
- **Type consistency:** `StepStatus` / `RunStatus` used consistently; `Program`, `Pipeline`, `Step` dataclass shapes are stable across config / state / runner / app.

## Notes for the implementer

- The DAG-execution loop in Task 8 is the trickiest piece. Don't try to make it asynchronous beyond what's there; the busy-wait via `fut.done()` plus a single blocking `fut.result()` is intentional and easy to read.
- Dash's pattern-matching callbacks (`{"type": "run-program", "id": ALL}`) need `from dash import ALL` — imported at the top of `_register_callbacks`. Don't move it module-level or it breaks suppress-callback-exceptions semantics in some Dash versions.
- On Windows, `sys.executable` gives the venv python; that's what we want. `subprocess.Popen` with a list argv avoids any shell-quoting concern.
