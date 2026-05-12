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

    # --- recovery ---

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

    # --- queries for UI ---

    def get_program_status(self, program_id: str) -> StepStatus:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM steps WHERE program_id=? "
                "ORDER BY started_at DESC, id DESC LIMIT 1",
                (program_id,),
            ).fetchone()
        if row is None:
            return StepStatus.IDLE
        return StepStatus(row["status"])

    def get_latest_step_for_program(self, program_id: str) -> StepRow | None:
        with self._connect() as conn:
            r = conn.execute(
                "SELECT * FROM steps WHERE program_id=? "
                "ORDER BY started_at DESC, id DESC LIMIT 1",
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
