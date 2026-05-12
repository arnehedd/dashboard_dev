from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


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

    def topo_sort(self, pipeline_id: str) -> list[str]:
        return _topo_sort_steps(self.pipelines[pipeline_id].steps)


def _parse_step(raw: Any, prev_program: str | None) -> Step:
    if isinstance(raw, str):
        needs = [prev_program] if prev_program else []
        return Step(program=raw, needs=needs)
    if isinstance(raw, dict) and "program" in raw:
        return Step(program=raw["program"], needs=list(raw.get("needs", [])))
    raise ValueError(f"Invalid step entry: {raw!r}")


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


def _validate(cfg: "Config") -> None:
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
        try:
            _topo_sort_steps(pipeline.steps)
        except ConfigError as exc:
            raise ConfigError(f"Pipeline {pipeline.id!r}: {exc}") from exc


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
    cfg = Config(programs=programs, pipelines=pipelines)
    _validate(cfg)
    return cfg
