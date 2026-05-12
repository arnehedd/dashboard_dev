# Dash Pipeline Dashboard — Design

**Status:** Draft (in Brainstorming validiert, 2026-05-12)
**Projekt-Pfad:** `C:\Users\ahedd\dash-pipeline`
**Zielnutzer:** Eine Person, lokal auf Windows.

## 1. Ziel

Ein lokales Browser-Dashboard auf Basis von [Dash](https://dash.plotly.com/), um
zwischen 5 und 15 bereits existierende Python-Skripte zu starten und zu
überwachen. Die Skripte erzeugen Parquet-Tabellen mit einer einheitlichen
Timestamp-Spalte. Manche Skripte laufen in Ketten (meist linear, gelegentlich
verzweigt). Das Dashboard soll auf einen Blick zeigen:

- aktuellen Zustand jedes Programms (Idle / Running / Success / Failed / Skipped)
- den letzten Timestamp in der jeweils erzeugten Parquet-Datei
- Möglichkeit, einzelne Programme oder ganze Ketten per Klick zu starten

Mehrere Pipelines/Programme dürfen parallel laufen.

## 2. Nicht-Ziele

- Multi-User, Authentifizierung, Berechtigungen.
- Cron-/Zeitsteuerung (kein Scheduler — User startet manuell).
- Live-Streaming der stdout während eines laufenden Schritts (Logs werden
  nach Lauf-Ende über ein Modal sichtbar).
- Stop-Button für laufende Programme.
- Visueller Drag-and-Drop-Editor für Pipelines (nur read-only DAG).

## 3. Architektur

Ein einzelner Python-Prozess hostet die Dash-App. Pipeline-Ausführung läuft
in einem `ThreadPoolExecutor`; jeder Schritt startet ein externes Skript via
`subprocess.Popen`. Zustand wird in SQLite persistiert, Logs in Dateien.

```
┌──────────── Dash App (ein Python-Prozess) ────────────┐
│   Layout + Callbacks  (app.py)                        │
│        │                                              │
│        │ klick "Run"          dcc.Interval (alle 2s)  │
│        ▼                                              │
│   ┌──────────────┐         ┌──────────────────┐       │
│   │  runner.py   │  ◀──────│   state.py       │       │
│   │ ThreadPool + │         │  (SQLite-Wrapper)│       │
│   │ subprocess   │ ───────▶│   runs / steps   │       │
│   └──────────────┘         └──────────────────┘       │
│        │                            ▲                 │
│        │ Popen pro Skript           │                 │
│        ▼                            │                 │
│   externer Python-Prozess           │                 │
│        │                            │                 │
│        │ schreibt parquet           │                 │
│        ▼                            │                 │
│   data/*.parquet ──────▶ parquet_meta.py (cached 10s) │
└───────────────────────────────────────────────────────┘
```

### 3.1 Module

| Datei | Verantwortung |
|---|---|
| `app.py` | Dash-Layout, Callbacks, Routing |
| `runner.py` | Pipeline-Ausführung (Thread + `subprocess.Popen`) |
| `state.py` | SQLite-Wrapper (Runs/Steps lesen, schreiben) |
| `parquet_meta.py` | Letzten Timestamp + Dateigröße je Parquet lesen, mit 10 s Cache |
| `config.py` | `pipelines.yaml` laden, validieren (Schema, Zyklen) |
| `pipelines.yaml` | Pipeline- und Programm-Definitionen (vom Nutzer editiert) |
| `tests/` | Pytest-Suite (siehe Abschnitt 8) |

Jedes Modul ist isoliert testbar: `runner` kennt nur `state` und `config`,
`state` kennt nur SQLite, `parquet_meta` ist eine reine Funktion.

### 3.2 Verzeichnis-Layout

```
dash-pipeline/
├── app.py
├── runner.py
├── state.py
├── parquet_meta.py
├── config.py
├── pipelines.yaml
├── programs/           # Skripte oder Pfade darauf
├── data/               # Parquet-Tabellen
├── logs/<run_id>/      # stdout/stderr pro Schritt
├── runs.sqlite         # Persistenter Zustand
├── tests/
├── requirements.txt
└── README.md
```

## 4. Konfiguration: `pipelines.yaml`

Single Source of Truth für Programme und Pipelines. Wird beim Dashboard-Start
geladen und beim Editieren über einen Reload-Button neu eingelesen (kein
Hot-Reload nötig).

```yaml
programs:
  load_orders:
    script: programs/load_orders.py
    parquet: data/orders.parquet
    timestamp_column: timestamp
    timeout_seconds: 3600        # optional, default 3600

  transform_orders:
    script: programs/transform_orders.py
    parquet: data/orders_clean.parquet
    timestamp_column: timestamp

pipelines:
  daily_orders:
    name: "Tägliche Order-Verarbeitung"
    description: "Lädt, transformiert und aggregiert Orders"
    steps:
      - load_orders            # plain string = linearer Schritt
      - transform_orders
      - aggregate_orders

  branched_example:
    name: "Mit Verzweigung"
    steps:
      - load_data
      - clean_a
      - program: clean_b       # explizite Form für Branch/Join
        needs: [load_data]
      - program: merge
        needs: [clean_a, clean_b]
```

**Regeln:**
- Plain string → Schritt hängt automatisch am vorhergehenden Schritt der Liste.
- `{program, needs}` → Schritt hängt an den unter `needs` gelisteten Schritten.
- Ein Programm kann in mehreren Pipelines auftauchen.
- `script` und `parquet` können relative Pfade (zum Projektroot) oder
  absolute Pfade sein. Das Verzeichnis `programs/` ist nur Konvention, nicht
  Pflicht — du kannst auf existierende Skripte irgendwo auf dem System zeigen.
- `config.py` validiert: alle referenzierten Programme existieren, kein Zyklus,
  alle `needs`-Einträge sind vorher in der Liste definiert.

## 5. Zustands-Modell

### 5.1 SQLite-Schema

```sql
CREATE TABLE runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id     TEXT,              -- NULL bei Einzelprogramm-Start
    program_id      TEXT,              -- NULL bei Pipeline-Start
    status          TEXT NOT NULL,     -- pending|running|success|failed
    started_at      TEXT NOT NULL,     -- ISO-8601
    ended_at        TEXT,
    triggered_by    TEXT NOT NULL      -- 'ui' (Erweiterung möglich)
);

CREATE TABLE steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES runs(id),
    program_id      TEXT NOT NULL,
    status          TEXT NOT NULL,     -- pending|running|success|failed|skipped
    started_at      TEXT,
    ended_at        TEXT,
    exit_code       INTEGER,
    log_path        TEXT
);

CREATE INDEX idx_runs_pipeline ON runs(pipeline_id, started_at DESC);
CREATE INDEX idx_steps_run     ON steps(run_id);
```

### 5.2 Status-Zustände

| Status | Bedeutung | Farbe (UI) |
|---|---|---|
| `idle` | Noch nie gelaufen oder aktuell nicht aktiv (kein Eintrag in `runs` oder letzter Run liegt zurück) | Grau |
| `running` | Aktuell aktiver Run/Step | Blau (pulsierend) |
| `success` | Letzter Lauf: Exit-Code 0 | Grün |
| `failed` | Letzter Lauf: Exit-Code ≠ 0, Timeout oder Crash | Rot |
| `skipped` | Step nur: Upstream ist `failed`, Step wurde nicht gestartet | Orange |

Für Programme im UI: der angezeigte Status ist der Status der jüngsten
zugehörigen `steps`-Reihe; ein "frisches" Programm ohne Eintrag ist `idle`.

### 5.3 Recovery beim Dashboard-Start

Beim Start markiert `state.py` alle Runs/Steps mit `status='running'` als
`failed` (mit synthetischem Endzeitpunkt = aktueller Zeit und Notiz
"Dashboard restart"). Sonst hängen Geister-Runs nach einem Crash dauerhaft im
`running`-Zustand.

## 6. UI: Sidebar Master/Detail

### 6.1 Layout

```
┌─ Topbar ──────────────────────────────────────────┐
│  dash-pipeline · 3 pipelines · 8 programs    ↻    │
├──────────┬────────────────────────────────────────┤
│ Sidebar  │  Detail-Panel                          │
│          │                                        │
│ Pipelines│  Header (Name, Beschreibung, ▶ Run)    │
│  · …     │                                        │
│  · …     │  DAG-Graph (cytoscape, ~250 px)        │
│          │                                        │
│ Programme│  Programm-Tabelle dieser Pipeline      │
│          │                                        │
│          │  Run-Historie (letzte 10)              │
└──────────┴────────────────────────────────────────┘
```

### 6.2 Sidebar

Zwei Abschnitte:

1. **Pipelines** — eine Karte pro Pipeline mit:
   - Pipeline-Name
   - Mini-DAG (kleine farbige Kästchen mit Pfeilen) — zeigt aktuellen Status je Step
   - Klick → wählt diese Pipeline im Detail-Panel aus
2. **Programme** — Eintrag "Alle Programme"; Klick → Detail-Panel zeigt
   Tabelle aller Programme (sortier-/filterbar).

### 6.3 Detail-Panel: Pipeline

- **Header:** Name, Beschreibung, `▶ Run Pipeline`-Button. Button ist disabled,
  solange ein Run dieser Pipeline aktiv ist.
- **DAG-Graph:** [dash-cytoscape](https://dash.plotly.com/cytoscape) Component.
  Knoten = Programme, Kanten aus `needs`. Knoten-Farbe = aktueller Step-Status
  des Programms in dieser Pipeline. Klick auf einen Knoten öffnet ein Modal
  mit stdout/stderr aus dem letzten Lauf.
- **Programm-Tabelle:** Spalten *Programm*, *Status*, *Letzter Parquet-TS*,
  *Dateigröße*, *▶ (Einzelstart)*. Quellen: `state.py` für Status,
  `parquet_meta.py` für TS und Größe.
- **Run-Historie:** Tabelle mit den letzten 10 Runs der Pipeline (Start,
  Dauer, Status, Link zum Log-Verzeichnis).

### 6.4 Detail-Panel: Programme

Tabelle aller in `pipelines.yaml` definierten Programme:
*Programm*, *Status*, *Letzter Parquet-TS*, *Dateigröße*, *In Pipelines*,
*▶ (Einzelstart)*.

### 6.5 Refresh

Ein `dcc.Interval` mit 2 s Intervall triggert einen Callback, der den Status
für die aktuell sichtbare Seite aus SQLite neu lädt und in die UI-Komponenten
schreibt. Parquet-Metadaten werden über `parquet_meta.py` bezogen, das pro
Pfad ein 10 s-Cache hält, um unnötige Datei-IO zu vermeiden.

## 7. Ausführungs-Logik (`runner.py`)

### 7.1 Pipeline-Start

0. Vorab-Check: läuft schon ein Run für dieselbe Pipeline (`status='running'`)?
   Wenn ja, wird der neue Start abgelehnt (UI-Button ist außerdem disabled,
   aber der Runner setzt eine zweite Verteidigungslinie). Einzelprogramm-Starts
   sind davon unabhängig — dieselbe Pipeline darf neu gestartet werden, wenn
   nur ein Einzelprogramm desselben Skripts läuft.
1. Topo-Sort der Steps anhand `needs`.
2. Run-Eintrag in SQLite (`status='running'`), für jeden Step ein
   `steps`-Eintrag (`status='pending'`).
3. ThreadPool wählt Steps, deren Vorgänger alle `success` sind.
4. Pro Step:
   - Log-Datei `logs/<run_id>/<program_id>.log` öffnen.
   - `subprocess.Popen([sys.executable, script_path], stdout=log, stderr=log,
     cwd=<projekt>, timeout=<timeout_seconds>)`.
   - Bei Exit-Code 0 → Step `success`.
   - Sonst → Step `failed`, alle noch nicht gestarteten downstream-Steps werden
     auf `skipped` gesetzt.
5. Run-Status: `success` wenn alle Steps `success`, sonst `failed`.

### 7.2 Einzelprogramm-Start

Erzeugt einen Run mit `pipeline_id=NULL, program_id=<id>` und genau einem
Step. Sonst gleicher Ablauf.

### 7.3 Parallelität

`ThreadPoolExecutor(max_workers=8)` ist global. Mehrere Pipelines können
gleichzeitig laufen; sie konkurrieren nur um Worker-Slots. Innerhalb einer
Pipeline werden unabhängige Steps (gleiches `needs`-Niveau) parallel
ausgeführt.

### 7.4 Fehlerquellen

| Fall | Behandlung |
|---|---|
| Subprocess Exit ≠ 0 | Step `failed`, stderr im Log, downstream `skipped`. |
| Timeout | `Popen.terminate()` + `kill()` nach Grace-Period, Step `failed`. |
| Python-Exception im Runner | Try/Except um Step-Block, Traceback ins Log, Step `failed`. |
| Skript-Datei fehlt | Schon in `config.py` validiert — Pipeline wird gar nicht erst akzeptiert. |

## 8. Tests

| Datei | Inhalt |
|---|---|
| `tests/test_config.py` | Gültige/ungültige YAML, fehlende Programme, Zyklus-Erkennung, plain-string vs. dict-Step-Form. |
| `tests/test_state.py` | CRUD auf `runs`/`steps`, Recovery von hängenden `running`-Einträgen, paralleler Zugriff mehrerer Threads. |
| `tests/test_parquet_meta.py` | Sample-Parquet mit bekannten Timestamps, leere Datei, fehlende Datei, falsche Spalte, Cache-Verhalten. |
| `tests/test_runner.py` | Dummy-Skripte (linear, verzweigt, failure-path, timeout) in `tests/fixtures/scripts/`. Verifiziert finale Statusbäume und übersprungene Steps. |

Kein E2E-UI-Test. Manuelles Smoke-Testen über das Dashboard reicht für ein
lokales Tool.

## 9. Abhängigkeiten

```
dash >= 2.17
dash-cytoscape >= 1.0
pandas
pyarrow
pyyaml
pytest                # nur Dev
```

Keine externen Server (Redis, Postgres, Prefect …) — alles steht im
Python-Prozess.

## 10. Offene Punkte für die Umsetzungsphase

Diese Punkte beeinflussen nicht die Architektur und werden im
Implementierungs-Plan entschieden:

- Konkrete Farbpalette (Hex-Werte) und Schriftart der UI.
- Format der Dauer-Anzeige (`3m 21s` vs `00:03:21`).
- Genaue Sortierung der Programm-Tabelle (alphabetisch? nach letztem Lauf?).
- Ob das Log-Modal ANSI-Farbcodes rendern soll.
- Datei-Rotation für `logs/`: erst einmal nicht — manuelle Löschung genügt.
