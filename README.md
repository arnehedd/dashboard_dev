# dash-pipeline

Local Dash dashboard to start Python scripts (single or in chains) and monitor
their parquet outputs.

## Run

```bash
python -m pip install -r requirements.txt
python app.py --config pipelines.yaml
```

Open http://localhost:8050.

## Editing pipelines

Edit `pipelines.yaml`. Restart the app to pick up changes.
See `docs/superpowers/specs/2026-05-12-dash-pipeline-design.md` for the full spec.

## Tests

```bash
python -m pytest
```
