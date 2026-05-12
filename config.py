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
