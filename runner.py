import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
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
        script_path = (
            Path(program.script)
            if Path(program.script).is_absolute()
            else self._project_root / program.script
        )
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

    # --- pipeline execution ---

    def _execute_pipeline(self, run_id: int, pipeline_id: str) -> None:
        try:
            self._execute_pipeline_inner(run_id, pipeline_id)
        finally:
            with self._lock:
                self._active_pipelines.discard(pipeline_id)

    def _execute_pipeline_inner(self, run_id: int, pipeline_id: str) -> None:
        pipeline = self._config.pipelines[pipeline_id]
        steps_by_program = {s.program: s for s in pipeline.steps}

        # Pre-create step rows so the UI sees them as 'pending' immediately.
        step_ids: dict[str, int] = {}
        for s in pipeline.steps:
            sid = self._state.start_step(
                run_id,
                program_id=s.program,
                log_path=str(self._logs_dir / str(run_id) / f"{s.program}.log"),
            )
            self._state.finish_step(sid, status=StepStatus.PENDING, exit_code=None)
            step_ids[s.program] = sid

        completed: dict[str, StepStatus] = {}
        in_flight: dict[str, Future] = {}

        def has_failed_dep(prog: str) -> bool:
            return any(
                completed.get(dep) in (StepStatus.FAILED, StepStatus.SKIPPED)
                for dep in steps_by_program[prog].needs
            )

        def ready_to_run() -> list[str]:
            return [
                p
                for p, s in steps_by_program.items()
                if p not in completed
                and p not in in_flight
                and all(dep in completed for dep in s.needs)
                and not has_failed_dep(p)
            ]

        while len(completed) < len(pipeline.steps):
            # Mark anything blocked by a failed upstream as skipped.
            for p in list(steps_by_program):
                if p in completed or p in in_flight:
                    continue
                if has_failed_dep(p):
                    self._state.finish_step(
                        step_ids[p], status=StepStatus.SKIPPED, exit_code=None
                    )
                    completed[p] = StepStatus.SKIPPED

            for prog in ready_to_run():
                in_flight[prog] = self._pool.submit(
                    self._execute_step_in_pipeline,
                    run_id, prog, step_ids[prog],
                )

            if not in_flight:
                break  # nothing ready; everything else has been skipped

            done, _pending = wait(list(in_flight.values()),
                                  return_when=FIRST_COMPLETED)
            for prog, fut in list(in_flight.items()):
                if fut in done:
                    completed[prog] = fut.result()
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
        # Transition pending -> running.
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
