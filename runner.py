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
