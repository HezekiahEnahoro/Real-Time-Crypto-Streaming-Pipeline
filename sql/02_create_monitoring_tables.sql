CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_failures (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100),
    task_id         VARCHAR(100),
    execution_date  TIMESTAMP,
    error_message   TEXT,
    recorded_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitoring.health_checks (
    id          SERIAL PRIMARY KEY,
    check_name  VARCHAR(100),
    status      VARCHAR(20),
    details     JSONB,
    checked_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_failures_dag      ON monitoring.pipeline_failures(dag_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_health_checked_at ON monitoring.health_checks(checked_at DESC);
