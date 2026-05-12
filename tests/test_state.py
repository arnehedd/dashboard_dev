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
    run2 = state.start_run(pipeline_id="p", program_id=None)
    step2 = state.start_step(run2, program_id="load_orders", log_path="y")
    state.finish_step(step2, status=StepStatus.SUCCESS, exit_code=0)
    assert state.get_program_status("load_orders") == StepStatus.SUCCESS


def test_get_program_status_idle_when_no_runs(state: State):
    assert state.get_program_status("never_ran") == StepStatus.IDLE
