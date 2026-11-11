# FactoryPulse architecture guide

FactoryPulse is a local IoT predictive maintenance pipeline. This guide maps
each component to its code. `README.md` holds the quickstart and the
tech-stack rationale; `CONTEXT.md` defines the domain terms.

---

## Where things live

| Path | Contents |
| :-- | :-- |
| `src/factorypulse/` | The Python package: event contract, validator, simulator, producer, lake read module, analytics functions, anomaly scoring rule, quality checks, dashboard. |
| `spark/` | The Spark jobs: streaming and batch ingestion, anomaly scoring, lake queries, and the shared Spark and Delta settings. |
| `dbt/` | The dbt project: four models, seeds and fixture assertions. |
| `dags/` | The Airflow DAG file. |
| `scripts/` | The driver scripts that run one compose service per step and their shared job runner, plus `release.sh` (publishes a release) and `import-contribution.sh` (imports an accepted pull request). |
| `docker/` | One Dockerfile per image: Spark (with dbt as a build option), producer, quality, dashboard, Airflow. |
| `tests/` | The pytest suite. |
| `compose.yaml` | Every service of the platform. |
| `.env.example` | Every variable compose reads, with development values. |

---

## Components

1. **Event contract.** `src/factorypulse/contract.py` defines the sensor
   event: its fields, sensor types, units, physical ranges and statuses.
   `src/factorypulse/validator.py` validates an event against it and returns
   field-prefixed errors. `src/factorypulse/schema/event_v1.json` mirrors the
   contract for non-Python stages, and a test keeps the two in sync.
2. **Simulator and producer.** `src/factorypulse/simulator.py` generates
   deterministic events for a fleet of machines, with injected spikes and
   drift that stay inside each sensor's physical range.
   `src/factorypulse/producer.py` publishes them to the Kafka topic
   `sensor-events` and writes the same events to MinIO as a JSONL snapshot.
3. **Ingestion.** Both paths write `raw_sensor_events`, a Delta table on
   MinIO.
   - `spark/streaming_ingest.py` reads the topic with Structured Streaming,
     validates each event, and appends valid events. Rejected events go to a
     sibling invalid table with their validation errors.
   - `spark/batch_ingest.py` replays the snapshot through a MERGE on
     `event_id`, so a rerun adds no duplicates.
4. **dbt models.** `dbt/models/marts/` holds four models built with
   dbt-spark as Delta tables: `dim_machine` (one row per machine),
   `fct_sensor_readings_clean` (one row per deduplicated event),
   `mrt_machine_hourly` (one row per machine per UTC hour) and
   `mrt_machine_weekly` (one row per machine per ISO week). Fixture seeds and
   tests assert the hourly and weekly aggregates.
5. **Anomaly scoring.** `spark/anomaly_detection.py` reads
   `mrt_machine_hourly` and, for each machine's hourly temperature and
   vibration, computes the mean and standard deviation of the hourly windows
   in the previous 24 hours, never the scored window itself. `src/factorypulse/anomaly_core.py` holds the scoring rule:
   the z-score of the window against that baseline, its mapping to a sensor
   status, and the machine status as the worst sensor status. The job merges
   the scores into `mrt_anomaly_scores`, keyed on machine, window and sensor.
6. **Quality checks.** `src/factorypulse/quality/` runs Great Expectations
   over the lake in three stages: the raw lake, the daily models and the
   weekly mart. The checks take their allowed values and ranges from the
   event contract. A failed check fails the run and is named in the report.
7. **Lake reads outside Spark.** `src/factorypulse/lake.py` reads a Delta
   table with `deltalake`. It owns the MinIO endpoint and credentials and
   each table's declared columns. A table not yet built raises a named error
   instead of returning an empty frame.
8. **Orchestration.** `dags/factorypulse_dag.py` defines
   `factorypulse_daily` (batch ingestion, raw checks, the daily dbt build,
   model checks) and `factorypulse_weekly` (the weekly mart and its checks).
   Each task runs one driver step from `scripts/`.
9. **Dashboard.** `src/factorypulse/dashboard.py` is a Streamlit app over
   `dim_machine`, `mrt_machine_hourly` and `mrt_anomaly_scores`. It shows
   fleet KPIs, hourly temperature and vibration trends, and the at-risk
   machines. The computation lives in `src/factorypulse/analytics.py`.

---

## Development and testing

- Run the whole pipeline: `bash scripts/lake.sh all`.
- Run the host tests: `python -m pytest` (setup in `README.md`).
- Run one job: `docker compose --profile lake-jobs run --rm <service>`, or the
  driver step that wraps it.
