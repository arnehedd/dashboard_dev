from pathlib import Path

import pytest

from config import Config, Pipeline, Program, Step
from runner import Runner
from state import RunStatus, State, StepStatus


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
