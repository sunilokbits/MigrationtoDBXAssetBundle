"""
Workflow Manager — Metadata-Driven Job Orchestration
=====================================================
Manages medallion pipeline jobs with:
  • Dynamic job creation per source table (Extract → Landing → Bronze → Silver)
  • Full / Incremental load support with watermark column tracking
  • Metadata tables for job registry, run history, and watermarks
  • Job CRUD operations (Add, Update, Delete, Rerun from failure)
  • Proper logging and failure tracking
  • **Databricks Unity Catalog persistence** — metadata stored in Delta tables
"""

import uuid
import json
import threading
import requests
import time
from datetime import datetime
from collections import OrderedDict

# ─────────────────────────────────────────────────────────────────────────────
#  In-Memory Metadata Store  +  Databricks Delta persistence
# ─────────────────────────────────────────────────────────────────────────────
JOB_REGISTRY = OrderedDict()       # job_id → job metadata
JOB_RUNS = OrderedDict()           # run_id → run details
WATERMARKS = {}                    # table_name → {column, last_value, updated_at}
PIPELINE_GROUPS = OrderedDict()    # group_id → {table, jobs: [job_ids]}
SOURCE_TABLES = []                 # discovered source tables

# ── Lock for thread-safe writes ──
_lock = threading.Lock()

# ── Databricks connection state (set via init_metadata_flow) ──
_dbr_host = None
_dbr_token = None
_dbr_catalog = None
_dbr_schema = None
_dbr_warehouse_id = None
_metadata_initialized = False

# ── Table names ──
TBL_PIPELINES = "wf_pipeline_metadata"
TBL_JOBS      = "wf_job_metadata"
TBL_RUNS      = "wf_run_history"
TBL_WATERMARKS = "wf_watermark_metadata"
TBL_SOURCES   = "wf_source_tables"


# ─────────────────────────────────────────────────────────────────────────────
#  DATABRICKS SQL EXECUTION HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _dbr_session():
    """Return requests session with auth headers."""
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {_dbr_token}",
        "Content-Type": "application/json",
    })
    return s

def _exec_sql(sql: str, wait_timeout: str = "30s") -> dict:
    """Execute SQL on Databricks SQL Warehouse and return result."""
    if not _dbr_host or not _dbr_token or not _dbr_warehouse_id:
        return {"error": "Databricks not connected — run Create MetadataFlow first"}
    s = _dbr_session()
    payload = {
        "statement": sql,
        "warehouse_id": _dbr_warehouse_id,
        "catalog": _dbr_catalog or "main",
        "schema": _dbr_schema or "default",
        "wait_timeout": wait_timeout,
        "on_wait_timeout": "CONTINUE",
    }
    try:
        resp = s.post(f"{_dbr_host}/api/2.0/sql/statements", json=payload, timeout=60)
        data = resp.json() if resp.status_code == 200 else {"error": resp.text[:300]}
        sid = data.get("statement_id")
        if not sid:
            return data
        # Poll
        for _ in range(40):
            state = data.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                break
            time.sleep(2)
            resp2 = s.get(f"{_dbr_host}/api/2.0/sql/statements/{sid}", timeout=15)
            data = resp2.json() if resp2.status_code == 200 else {"error": "poll error"}
        return data
    except Exception as e:
        return {"error": str(e)}

def _fqn(table: str) -> str:
    """Fully qualified table name."""
    c = _dbr_catalog or "main"
    s = _dbr_schema or "default"
    return f"`{c}`.`{s}`.`{table}`"


# ─────────────────────────────────────────────────────────────────────────────
#  METADATA FLOW — INITIALISE DELTA TABLES IN DATABRICKS
# ─────────────────────────────────────────────────────────────────────────────
def _find_warehouse(session) -> str:
    """Auto-detect a running SQL Warehouse."""
    try:
        resp = session.get(f"{_dbr_host}/api/2.0/sql/warehouses", timeout=15)
        if resp.status_code == 200:
            whs = resp.json().get("warehouses", [])
            running = [w for w in whs if w.get("state") == "RUNNING"]
            if running:
                return running[0]["id"]
            if whs:
                return whs[0]["id"]
    except Exception:
        pass
    return None


def init_metadata_flow(host: str, token: str, catalog: str = "main",
                       schema: str = "default", warehouse_id: str = "") -> dict:
    """
    Provision the 5 metadata Delta tables in Databricks Unity Catalog.
    Tables are created IF NOT EXISTS so calling again is safe.
    """
    global _dbr_host, _dbr_token, _dbr_catalog, _dbr_schema, _dbr_warehouse_id, _metadata_initialized

    _dbr_host = host.rstrip("/")
    _dbr_token = token
    _dbr_catalog = catalog or "main"
    _dbr_schema = schema or "default"

    # Find warehouse
    s = _dbr_session()
    if warehouse_id:
        _dbr_warehouse_id = warehouse_id
    else:
        _dbr_warehouse_id = _find_warehouse(s)

    if not _dbr_warehouse_id:
        return {"success": False, "error": "No SQL Warehouse found. Start one in your Databricks workspace."}

    # Ensure schema exists
    _exec_sql(f"CREATE SCHEMA IF NOT EXISTS `{_dbr_catalog}`.`{_dbr_schema}`")

    # ── DDL for 5 metadata tables ──
    ddl_statements = [
        # 1. Pipeline metadata
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_PIPELINES)} (
            group_id         STRING NOT NULL,
            table_schema     STRING,
            table_name       STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            status           STRING,
            source_config    STRING,
            target_config    STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP
        ) USING DELTA
        COMMENT 'Workflow pipeline groups — one row per source table'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

        # 2. Job metadata
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_JOBS)} (
            job_id           STRING NOT NULL,
            job_name         STRING,
            stage            STRING,
            group_id         STRING,
            table_schema     STRING,
            table_name       STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            status           STRING,
            last_run_id      STRING,
            last_run_at      TIMESTAMP,
            last_status      STRING,
            run_count        INT,
            fail_count       INT,
            enabled          BOOLEAN,
            job_order        INT,
            source_config    STRING,
            target_config    STRING,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP
        ) USING DELTA
        COMMENT 'Individual jobs in the medallion pipeline'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",

        # 3. Run history
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_RUNS)} (
            run_id           STRING NOT NULL,
            job_id           STRING,
            job_name         STRING,
            stage            STRING,
            full_table       STRING,
            load_type        STRING,
            watermark_column STRING,
            watermark_value  STRING,
            status           STRING,
            started_at       TIMESTAMP,
            completed_at     TIMESTAMP,
            duration_sec     DOUBLE,
            rows_processed   BIGINT,
            error_message    STRING,
            logs             STRING
        ) USING DELTA
        COMMENT 'Job execution run history'""",

        # 4. Watermarks
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_WATERMARKS)} (
            table_name       STRING NOT NULL,
            watermark_column STRING,
            last_value       STRING,
            updated_at       TIMESTAMP
        ) USING DELTA
        COMMENT 'Watermark tracking for incremental loads'""",

        # 5. Source tables
        f"""CREATE TABLE IF NOT EXISTS {_fqn(TBL_SOURCES)} (
            source_id        STRING NOT NULL,
            source_type      STRING,
            server           STRING,
            database_name    STRING,
            table_schema     STRING,
            table_name       STRING,
            full_name        STRING,
            col_count        INT,
            row_estimate     BIGINT,
            discovered_at    TIMESTAMP
        ) USING DELTA
        COMMENT 'Discovered source tables from SQL Server'""",
    ]

    results = []
    errors = []
    for ddl in ddl_statements:
        r = _exec_sql(ddl)
        state = r.get("status", {}).get("state", "UNKNOWN")
        if "error" in r:
            errors.append(r["error"])
        elif state == "FAILED":
            err_msg = r.get("status", {}).get("error", {}).get("message", "DDL failed")
            errors.append(err_msg)
        else:
            results.append(state)

    if errors:
        return {"success": False, "error": "; ".join(errors), "partial_results": results}

    _metadata_initialized = True
    return {
        "success": True,
        "message": f"MetadataFlow created — 5 Delta tables provisioned in {_dbr_catalog}.{_dbr_schema}",
        "catalog": _dbr_catalog,
        "schema": _dbr_schema,
        "warehouse_id": _dbr_warehouse_id,
        "tables": [TBL_PIPELINES, TBL_JOBS, TBL_RUNS, TBL_WATERMARKS, TBL_SOURCES],
    }


# ─────────────────────────────────────────────────────────────────────────────
#  CHECK METADATA STATUS
# ─────────────────────────────────────────────────────────────────────────────
def get_metadata_status() -> dict:
    """Check if metadata tables exist and return row counts."""
    if not _dbr_host or not _dbr_token:
        return {"success": True, "initialized": False, "message": "Databricks not connected"}

    tables_status = {}
    for tbl in [TBL_PIPELINES, TBL_JOBS, TBL_RUNS, TBL_WATERMARKS, TBL_SOURCES]:
        r = _exec_sql(f"SELECT COUNT(*) AS cnt FROM {_fqn(tbl)}")
        state = r.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            rows = r.get("result", {}).get("data_array", [["0"]])
            tables_status[tbl] = {"exists": True, "rows": int(rows[0][0])}
        else:
            tables_status[tbl] = {"exists": False, "rows": 0}

    all_exist = all(v["exists"] for v in tables_status.values())
    return {
        "success": True,
        "initialized": all_exist and _metadata_initialized,
        "host": _dbr_host,
        "catalog": _dbr_catalog,
        "schema": _dbr_schema,
        "warehouse_id": _dbr_warehouse_id,
        "tables": tables_status,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  SYNC IN-MEMORY → DATABRICKS (called after each CRUD operation)
# ─────────────────────────────────────────────────────────────────────────────
def _esc(val):
    """Escape single quotes for SQL string literals."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def _sync_pipeline_to_dbr(group: dict):
    """Upsert a pipeline group to Databricks."""
    if not _metadata_initialized:
        return
    try:
        sql = f"""MERGE INTO {_fqn(TBL_PIPELINES)} AS t
        USING (SELECT {_esc(group['group_id'])} AS group_id) AS s
        ON t.group_id = s.group_id
        WHEN MATCHED THEN UPDATE SET
            status = {_esc(group.get('status'))},
            load_type = {_esc(group.get('load_type'))},
            watermark_column = {_esc(group.get('watermark_column', ''))},
            source_config = {_esc(json.dumps(group.get('source_config') or {}))},
            target_config = {_esc(json.dumps(group.get('target_config') or {}))},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            group_id, table_schema, table_name, full_table, load_type,
            watermark_column, status, source_config, target_config, created_at, updated_at
        ) VALUES (
            {_esc(group['group_id'])}, {_esc(group.get('table_schema'))},
            {_esc(group.get('table_name'))}, {_esc(group.get('full_table'))},
            {_esc(group.get('load_type'))}, {_esc(group.get('watermark_column', ''))},
            {_esc(group.get('status'))},
            {_esc(json.dumps(group.get('source_config') or {}))},
            {_esc(json.dumps(group.get('target_config') or {}))},
            current_timestamp(), current_timestamp()
        )"""
        _exec_sql(sql)
    except Exception:
        pass  # non-blocking

def _sync_job_to_dbr(job: dict):
    """Upsert a job to Databricks."""
    if not _metadata_initialized:
        return
    try:
        sql = f"""MERGE INTO {_fqn(TBL_JOBS)} AS t
        USING (SELECT {_esc(job['job_id'])} AS job_id) AS s
        ON t.job_id = s.job_id
        WHEN MATCHED THEN UPDATE SET
            status = {_esc(job.get('status'))},
            last_run_id = {_esc(job.get('last_run_id'))},
            last_run_at = {_esc(job.get('last_run_at'))},
            last_status = {_esc(job.get('last_status'))},
            run_count = {job.get('run_count', 0)},
            fail_count = {job.get('fail_count', 0)},
            enabled = {str(job.get('enabled', True)).lower()},
            load_type = {_esc(job.get('load_type'))},
            watermark_column = {_esc(job.get('watermark_column', ''))},
            source_config = {_esc(json.dumps(job.get('source_config') or {}))},
            target_config = {_esc(json.dumps(job.get('target_config') or {}))},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            job_id, job_name, stage, group_id, table_schema, table_name, full_table,
            load_type, watermark_column, status, run_count, fail_count, enabled,
            job_order, source_config, target_config, created_at, updated_at
        ) VALUES (
            {_esc(job['job_id'])}, {_esc(job['job_name'])}, {_esc(job['stage'])},
            {_esc(job['group_id'])}, {_esc(job.get('table_schema'))},
            {_esc(job.get('table_name'))}, {_esc(job.get('full_table'))},
            {_esc(job.get('load_type'))}, {_esc(job.get('watermark_column', ''))},
            {_esc(job.get('status'))}, {job.get('run_count', 0)}, {job.get('fail_count', 0)},
            {str(job.get('enabled', True)).lower()}, {job.get('order', 1)},
            {_esc(json.dumps(job.get('source_config') or {}))},
            {_esc(json.dumps(job.get('target_config') or {}))},
            current_timestamp(), current_timestamp()
        )"""
        _exec_sql(sql)
    except Exception:
        pass

def _sync_run_to_dbr(run: dict):
    """Insert/update a run record to Databricks."""
    if not _metadata_initialized:
        return
    try:
        logs_str = json.dumps(run.get("logs", []))
        sql = f"""MERGE INTO {_fqn(TBL_RUNS)} AS t
        USING (SELECT {_esc(run['run_id'])} AS run_id) AS s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            status = {_esc(run.get('status'))},
            completed_at = {_esc(run.get('completed_at'))},
            duration_sec = {run.get('duration_sec') or 'NULL'},
            rows_processed = {run.get('rows_processed', 0)},
            error_message = {_esc(run.get('error'))},
            logs = {_esc(logs_str)}
        WHEN NOT MATCHED THEN INSERT (
            run_id, job_id, job_name, stage, full_table, load_type,
            watermark_column, watermark_value, status, started_at, rows_processed, logs
        ) VALUES (
            {_esc(run['run_id'])}, {_esc(run['job_id'])}, {_esc(run.get('job_name'))},
            {_esc(run.get('stage'))}, {_esc(run.get('full_table'))},
            {_esc(run.get('load_type'))}, {_esc(run.get('watermark_column', ''))},
            {_esc(run.get('watermark_value'))}, {_esc(run.get('status'))},
            {_esc(run.get('started_at'))}, {run.get('rows_processed', 0)},
            {_esc(logs_str)}
        )"""
        _exec_sql(sql)
    except Exception:
        pass

def _sync_watermark_to_dbr(table_name: str, wm: dict):
    """Upsert watermark to Databricks."""
    if not _metadata_initialized:
        return
    try:
        sql = f"""MERGE INTO {_fqn(TBL_WATERMARKS)} AS t
        USING (SELECT {_esc(table_name)} AS table_name) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            watermark_column = {_esc(wm.get('column'))},
            last_value = {_esc(wm.get('last_value'))},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, watermark_column, last_value, updated_at)
        VALUES ({_esc(table_name)}, {_esc(wm.get('column'))}, {_esc(wm.get('last_value'))}, current_timestamp())"""
        _exec_sql(sql)
    except Exception:
        pass

def _delete_pipeline_from_dbr(group_id: str):
    """Delete pipeline and associated jobs from Databricks."""
    if not _metadata_initialized:
        return
    try:
        _exec_sql(f"DELETE FROM {_fqn(TBL_JOBS)} WHERE group_id = {_esc(group_id)}")
        _exec_sql(f"DELETE FROM {_fqn(TBL_PIPELINES)} WHERE group_id = {_esc(group_id)}")
    except Exception:
        pass

def _delete_job_from_dbr(job_id: str):
    """Delete a single job from Databricks."""
    if not _metadata_initialized:
        return
    try:
        _exec_sql(f"DELETE FROM {_fqn(TBL_JOBS)} WHERE job_id = {_esc(job_id)}")
        _exec_sql(f"DELETE FROM {_fqn(TBL_RUNS)} WHERE job_id = {_esc(job_id)}")
    except Exception:
        pass

def sync_source_tables_to_dbr(tables: list, source_config: dict) -> dict:
    """Store discovered source tables to Databricks."""
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}
    try:
        # Clear old entries for this source
        server = source_config.get("server", "")
        db = source_config.get("database", "")
        _exec_sql(f"DELETE FROM {_fqn(TBL_SOURCES)} WHERE server = {_esc(server)} AND database_name = {_esc(db)}")

        # Batch insert
        for t in tables:
            sid = uuid.uuid4().hex[:12]
            sql = f"""INSERT INTO {_fqn(TBL_SOURCES)} VALUES (
                {_esc(sid)}, {_esc(source_config.get('source_type', 'sqlserver'))},
                {_esc(server)}, {_esc(db)},
                {_esc(t.get('schema', 'dbo'))}, {_esc(t.get('table', ''))},
                {_esc(t.get('full_name', ''))}, {t.get('col_count', 0)},
                {t.get('row_estimate', 0)}, current_timestamp()
            )"""
            _exec_sql(sql)
        return {"success": True, "synced": len(tables)}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
#  LOAD FROM DATABRICKS → IN-MEMORY (hydrate on connect)
# ─────────────────────────────────────────────────────────────────────────────
def load_metadata_from_dbr() -> dict:
    """Load all metadata from Databricks Delta tables into in-memory stores."""
    global JOB_REGISTRY, JOB_RUNS, WATERMARKS, PIPELINE_GROUPS
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}

    loaded = {"pipelines": 0, "jobs": 0, "runs": 0, "watermarks": 0}

    try:
        # Load pipelines
        r = _exec_sql(f"SELECT * FROM {_fqn(TBL_PIPELINES)} ORDER BY created_at")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                gid = rec["group_id"]
                with _lock:
                    PIPELINE_GROUPS[gid] = {
                        "group_id": gid,
                        "table_schema": rec.get("table_schema", ""),
                        "table_name": rec.get("table_name", ""),
                        "full_table": rec.get("full_table", ""),
                        "load_type": rec.get("load_type", "full"),
                        "watermark_column": rec.get("watermark_column", ""),
                        "job_ids": [],
                        "status": rec.get("status", "created"),
                        "created_at": rec.get("created_at", ""),
                    }
                loaded["pipelines"] += 1

        # Load jobs
        r = _exec_sql(f"SELECT * FROM {_fqn(TBL_JOBS)} ORDER BY created_at")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                jid = rec["job_id"]
                gid = rec.get("group_id", "")
                src_cfg = {}
                tgt_cfg = {}
                try: src_cfg = json.loads(rec.get("source_config") or "{}")
                except: pass
                try: tgt_cfg = json.loads(rec.get("target_config") or "{}")
                except: pass
                job = {
                    "job_id": jid,
                    "job_name": rec.get("job_name", ""),
                    "stage": rec.get("stage", ""),
                    "group_id": gid,
                    "table_schema": rec.get("table_schema", ""),
                    "table_name": rec.get("table_name", ""),
                    "full_table": rec.get("full_table", ""),
                    "load_type": rec.get("load_type", "full"),
                    "watermark_column": rec.get("watermark_column", ""),
                    "status": rec.get("status", "created"),
                    "last_run_id": rec.get("last_run_id"),
                    "last_run_at": rec.get("last_run_at"),
                    "last_status": rec.get("last_status"),
                    "run_count": int(rec.get("run_count", 0) or 0),
                    "fail_count": int(rec.get("fail_count", 0) or 0),
                    "created_at": rec.get("created_at", ""),
                    "updated_at": rec.get("updated_at", ""),
                    "source_config": src_cfg,
                    "target_config": tgt_cfg,
                    "order": int(rec.get("job_order", 1) or 1),
                    "enabled": str(rec.get("enabled", "true")).lower() in ("true", "1", "yes"),
                }
                with _lock:
                    JOB_REGISTRY[jid] = job
                    if gid in PIPELINE_GROUPS:
                        PIPELINE_GROUPS[gid]["job_ids"].append(jid)
                loaded["jobs"] += 1

        # Load watermarks
        r = _exec_sql(f"SELECT * FROM {_fqn(TBL_WATERMARKS)}")
        if r.get("status", {}).get("state") == "SUCCEEDED":
            cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
            for row in r.get("result", {}).get("data_array", []):
                rec = dict(zip(cols, row))
                tbl = rec["table_name"]
                with _lock:
                    WATERMARKS[tbl] = {
                        "column": rec.get("watermark_column", ""),
                        "last_value": rec.get("last_value"),
                        "updated_at": rec.get("updated_at", ""),
                    }
                loaded["watermarks"] += 1

        return {"success": True, "loaded": loaded}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
#  FULL SYNC — flush all in-memory data to Databricks
# ─────────────────────────────────────────────────────────────────────────────
def full_sync_to_dbr() -> dict:
    """Write all in-memory metadata to Databricks (for bulk sync)."""
    if not _metadata_initialized:
        return {"success": False, "error": "MetadataFlow not initialized"}
    synced = {"pipelines": 0, "jobs": 0, "runs": 0, "watermarks": 0}
    for gid, grp in PIPELINE_GROUPS.items():
        _sync_pipeline_to_dbr(grp)
        synced["pipelines"] += 1
    for jid, job in JOB_REGISTRY.items():
        _sync_job_to_dbr(job)
        synced["jobs"] += 1
    for rid, run in JOB_RUNS.items():
        _sync_run_to_dbr(run)
        synced["runs"] += 1
    for tbl, wm in WATERMARKS.items():
        _sync_watermark_to_dbr(tbl, wm)
        synced["watermarks"] += 1
    return {"success": True, "synced": synced}


# ─────────────────────────────────────────────────────────────────────────────
#  JOB NAMING CONVENTION
# ─────────────────────────────────────────────────────────────────────────────
def _job_name(stage: str, table_name: str, target_config: dict = None) -> str:
    """
    Generate standard job name per convention:
      1. ExtractToVolumes_<Table>   — extract from SQL source → dev_volumes
      2. VolumesToBronze_<Table>    — dev_volumes → bronze.hr
      3. BronzeToSilver_<Table>     — bronze.hr → silver.hr

    Falls back to legacy naming when target_config is not provided:
      1. SqlExtract_<Table>         — extract from SQL source
      2. LandingToBronze_<Table>    — landing to bronze
      3. BronzeToSilver_<Table>     — bronze to silver
    """
    clean = table_name.replace(".", "_").replace("[", "").replace("]", "").strip()
    tc = target_config or {}
    vol_cat = tc.get("volumes_catalog", "")
    brz_cat = tc.get("bronze_catalog", "")
    slv_cat = tc.get("silver_catalog", "")

    if vol_cat and brz_cat and slv_cat:
        # Multi-catalog medallion naming
        prefix_map = {
            "extract":           f"ExtractTo_{vol_cat}_{clean}",
            "landing_to_bronze":  f"{vol_cat}_To_{brz_cat}_{clean}",
            "bronze_to_silver":   f"{brz_cat}_To_{slv_cat}_{clean}",
        }
    else:
        # Legacy naming
        prefix_map = {
            "extract":           f"SqlExtract_{clean}",
            "landing_to_bronze":  f"LandingToBronze_{clean}",
            "bronze_to_silver":   f"BronzeToSilver_{clean}",
        }
    return prefix_map.get(stage, f"{stage}_{clean}")


# ─────────────────────────────────────────────────────────────────────────────
#  CREATE PIPELINE GROUP FOR A TABLE
# ─────────────────────────────────────────────────────────────────────────────
def create_pipeline_for_table(
    table_schema: str,
    table_name: str,
    load_type: str = "full",          # "full" or "incremental"
    watermark_column: str = "",       # e.g. "ModifiedDate"
    source_config: dict = None,       # {source_type, server, database, ...}
    target_config: dict = None,       # {catalog, schema, landing_path, ...}
) -> dict:
    """
    Create a 3-job pipeline group for one source table:
      Job 1: SqlExtract_<Table>       — Extract data from source → Landing
      Job 2: LandingToBronze_<Table>  — Landing → Bronze (raw delta)
      Job 3: BronzeToSilver_<Table>   — Bronze → Silver (cleansed)
    """
    full_table = f"{table_schema}.{table_name}"
    group_id = uuid.uuid4().hex[:12]
    ts = datetime.now().isoformat()

    source_config = source_config or {}
    target_config = target_config or {}

    jobs = []
    for stage in ["extract", "landing_to_bronze", "bronze_to_silver"]:
        job_id = uuid.uuid4().hex[:12]
        job = {
            "job_id":           job_id,
            "job_name":         _job_name(stage, table_name, target_config),
            "stage":            stage,
            "group_id":         group_id,
            "table_schema":     table_schema,
            "table_name":       table_name,
            "full_table":       full_table,
            "load_type":        load_type,
            "watermark_column": watermark_column if load_type == "incremental" else "",
            "status":           "created",       # created | running | success | failed | disabled
            "last_run_id":      None,
            "last_run_at":      None,
            "last_status":      None,
            "run_count":        0,
            "fail_count":       0,
            "created_at":       ts,
            "updated_at":       ts,
            "source_config":    source_config,
            "target_config":    target_config,
            "order":            ["extract", "landing_to_bronze", "bronze_to_silver"].index(stage) + 1,
            "enabled":          True,
        }
        with _lock:
            JOB_REGISTRY[job_id] = job
        jobs.append(job)

    # Register watermark if incremental
    if load_type == "incremental" and watermark_column:
        with _lock:
            WATERMARKS[full_table] = {
                "column":      watermark_column,
                "last_value":  None,
                "updated_at":  ts,
            }

    group = {
        "group_id":           group_id,
        "table_schema":       table_schema,
        "table_name":         table_name,
        "full_table":         full_table,
        "load_type":          load_type,
        "watermark_column":   watermark_column,
        "job_ids":            [j["job_id"] for j in jobs],
        "status":             "created",
        "source_config":      source_config,
        "target_config":      target_config,
        "created_at":         ts,
    }
    with _lock:
        PIPELINE_GROUPS[group_id] = group

    # ── Sync to Databricks ──
    _sync_pipeline_to_dbr(group)
    for j in jobs:
        _sync_job_to_dbr(j)
    if load_type == "incremental" and watermark_column and full_table in WATERMARKS:
        _sync_watermark_to_dbr(full_table, WATERMARKS[full_table])

    return {
        "success":   True,
        "group_id":  group_id,
        "group":     group,
        "jobs":      jobs,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  BULK CREATE PIPELINES
# ─────────────────────────────────────────────────────────────────────────────
def create_pipelines_bulk(
    tables: list,           # [{schema, table, load_type, watermark_column}, ...]
    source_config: dict = None,
    target_config: dict = None,
) -> dict:
    """Create pipeline groups for multiple tables at once."""
    results = []
    for t in tables:
        r = create_pipeline_for_table(
            table_schema=t.get("schema", "dbo"),
            table_name=t.get("table", ""),
            load_type=t.get("load_type", "full"),
            watermark_column=t.get("watermark_column", ""),
            source_config=source_config,
            target_config=target_config,
        )
        results.append(r)

    return {
        "success":    True,
        "created":    len(results),
        "groups":     [r["group"] for r in results],
        "total_jobs": sum(len(r["jobs"]) for r in results),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  LIST ALL JOBS
# ─────────────────────────────────────────────────────────────────────────────
def list_jobs(group_id: str = None, stage: str = None, status: str = None) -> dict:
    """List jobs with optional filters."""
    jobs = list(JOB_REGISTRY.values())
    if group_id:
        jobs = [j for j in jobs if j["group_id"] == group_id]
    if stage:
        jobs = [j for j in jobs if j["stage"] == stage]
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    return {"success": True, "jobs": jobs, "total": len(jobs)}


# ─────────────────────────────────────────────────────────────────────────────
#  LIST ALL PIPELINE GROUPS
# ─────────────────────────────────────────────────────────────────────────────
def list_pipeline_groups() -> dict:
    """List all pipeline groups with enriched job info."""
    groups = []
    for gid, grp in PIPELINE_GROUPS.items():
        job_details = []
        for jid in grp.get("job_ids", []):
            job = JOB_REGISTRY.get(jid)
            if job:
                job_details.append(job)
        # Compute overall status
        statuses = [j["status"] for j in job_details]
        if any(s == "failed" for s in statuses):
            overall = "failed"
        elif all(s == "success" for s in statuses):
            overall = "success"
        elif any(s == "running" for s in statuses):
            overall = "running"
        else:
            overall = "created"

        groups.append({
            **grp,
            "jobs":   job_details,
            "status": overall,
        })
    return {"success": True, "groups": groups, "total": len(groups)}


# ─────────────────────────────────────────────────────────────────────────────
#  GET SINGLE JOB
# ─────────────────────────────────────────────────────────────────────────────
def get_job(job_id: str) -> dict:
    """Get details of a single job."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}
    # Include run history
    runs = [r for r in JOB_RUNS.values() if r["job_id"] == job_id]
    runs.sort(key=lambda r: r["started_at"], reverse=True)
    return {"success": True, "job": job, "runs": runs}


# ─────────────────────────────────────────────────────────────────────────────
#  UPDATE JOB
# ─────────────────────────────────────────────────────────────────────────────
def update_job(job_id: str, updates: dict) -> dict:
    """Update job metadata (load_type, watermark_column, enabled, etc.)."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}

    allowed = {"load_type", "watermark_column", "enabled", "source_config", "target_config"}
    with _lock:
        for k, v in updates.items():
            if k in allowed:
                job[k] = v
        job["updated_at"] = datetime.now().isoformat()

    # If load_type changed to incremental and watermark set, update WATERMARKS
    if job["load_type"] == "incremental" and job["watermark_column"]:
        ft = job["full_table"]
        if ft not in WATERMARKS:
            WATERMARKS[ft] = {"column": job["watermark_column"], "last_value": None, "updated_at": job["updated_at"]}
        else:
            WATERMARKS[ft]["column"] = job["watermark_column"]
        _sync_watermark_to_dbr(ft, WATERMARKS[ft])

    _sync_job_to_dbr(job)
    return {"success": True, "job": job}


# ─────────────────────────────────────────────────────────────────────────────
#  DELETE JOB
# ─────────────────────────────────────────────────────────────────────────────
def delete_job(job_id: str) -> dict:
    """Delete a job from registry."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}

    group_id = job["group_id"]
    with _lock:
        del JOB_REGISTRY[job_id]
        # Remove from group
        if group_id in PIPELINE_GROUPS:
            grp = PIPELINE_GROUPS[group_id]
            grp["job_ids"] = [jid for jid in grp["job_ids"] if jid != job_id]
            if not grp["job_ids"]:
                del PIPELINE_GROUPS[group_id]
                _delete_pipeline_from_dbr(group_id)
        # Remove related runs
        run_ids_to_remove = [rid for rid, r in JOB_RUNS.items() if r["job_id"] == job_id]
        for rid in run_ids_to_remove:
            del JOB_RUNS[rid]

    _delete_job_from_dbr(job_id)
    return {"success": True, "deleted": job_id, "job_name": job["job_name"]}


# ─────────────────────────────────────────────────────────────────────────────
#  DELETE PIPELINE GROUP
# ─────────────────────────────────────────────────────────────────────────────
def delete_pipeline_group(group_id: str) -> dict:
    """Delete an entire pipeline group and all its jobs."""
    grp = PIPELINE_GROUPS.get(group_id)
    if not grp:
        return {"success": False, "error": f"Pipeline group '{group_id}' not found"}

    deleted_jobs = []
    with _lock:
        for jid in grp.get("job_ids", []):
            if jid in JOB_REGISTRY:
                deleted_jobs.append(JOB_REGISTRY[jid]["job_name"])
                del JOB_REGISTRY[jid]
            # Remove runs
            run_ids = [rid for rid, r in JOB_RUNS.items() if r["job_id"] == jid]
            for rid in run_ids:
                del JOB_RUNS[rid]
        del PIPELINE_GROUPS[group_id]

    _delete_pipeline_from_dbr(group_id)
    return {"success": True, "deleted_group": group_id, "deleted_jobs": deleted_jobs}


# ─────────────────────────────────────────────────────────────────────────────
#  RUN A JOB (simulated execution with logging)
# ─────────────────────────────────────────────────────────────────────────────
def run_job(job_id: str, force_full: bool = False) -> dict:
    """Start a job run. Returns run details immediately (background execution)."""
    job = JOB_REGISTRY.get(job_id)
    if not job:
        return {"success": False, "error": f"Job '{job_id}' not found"}
    if not job.get("enabled", True):
        return {"success": False, "error": f"Job '{job['job_name']}' is disabled"}

    run_id = uuid.uuid4().hex[:12]
    ts = datetime.now().isoformat()
    load_type = "full" if force_full else job["load_type"]

    # Get watermark if incremental
    watermark_value = None
    if load_type == "incremental" and job["watermark_column"]:
        wm = WATERMARKS.get(job["full_table"])
        if wm:
            watermark_value = wm.get("last_value")

    run = {
        "run_id":           run_id,
        "job_id":           job_id,
        "job_name":         job["job_name"],
        "stage":            job["stage"],
        "full_table":       job["full_table"],
        "load_type":        load_type,
        "watermark_column": job.get("watermark_column", ""),
        "watermark_value":  watermark_value,
        "status":           "running",
        "started_at":       ts,
        "completed_at":     None,
        "duration_sec":     None,
        "rows_processed":   0,
        "error":            None,
        "logs":             [
            f"[{ts}] 🚀 Started {job['job_name']}",
            f"[{ts}] 📋 Load type: {load_type}",
            f"[{ts}] 📊 Table: {job['full_table']}",
        ],
    }

    if watermark_value:
        run["logs"].append(f"[{ts}] 🔄 Watermark: {job['watermark_column']} > '{watermark_value}'")
    elif load_type == "incremental":
        run["logs"].append(f"[{ts}] ⚠️ No watermark found — will do initial full load")

    with _lock:
        JOB_RUNS[run_id] = run
        job["last_run_id"] = run_id
        job["last_run_at"] = ts
        job["status"] = "running"
        job["run_count"] += 1

    # Sync run start to Databricks
    _sync_run_to_dbr(run)
    _sync_job_to_dbr(job)

    # Start background execution
    t = threading.Thread(target=_execute_job_run, args=(run_id, job_id), daemon=True)
    t.start()

    return {"success": True, "run_id": run_id, "run": run}


def _execute_job_run(run_id: str, job_id: str):
    """Background execution of a job run (simulated)."""
    import time
    import random

    run = JOB_RUNS.get(run_id)
    job = JOB_REGISTRY.get(job_id)
    if not run or not job:
        return

    try:
        ts = datetime.now().isoformat()
        stage = job["stage"]
        tc = job.get("target_config") or {}
        vol_cat = tc.get("volumes_catalog", "")
        brz_cat = tc.get("bronze_catalog", "")
        slv_cat = tc.get("silver_catalog", "")
        tgt_sch = tc.get("target_schema", "")
        multi_cat = bool(vol_cat and brz_cat and slv_cat)

        # Simulate stage-specific processing
        if stage == "extract":
            run["logs"].append(f"[{ts}] 🔌 Connecting to source database…")
            time.sleep(1)
            run["logs"].append(f"[{ts}] ✅ JDBC connection established")
            time.sleep(0.5)
            rows = random.randint(1000, 50000)
            run["logs"].append(f"[{ts}] 📥 Extracting data from [{job['table_schema']}].[{job['table_name']}]…")
            time.sleep(1.5)
            run["logs"].append(f"[{ts}] 📊 Rows extracted: {rows:,}")
            if multi_cat:
                landing_dest = f"/Volumes/{vol_cat}/{tgt_sch}/landing/{job['table_name']}"
                run["logs"].append(f"[{ts}] 💾 Writing to UC Volumes: {landing_dest}")
            else:
                run["logs"].append(f"[{ts}] 💾 Writing to landing zone (Parquet)…")
            time.sleep(1)
            run["rows_processed"] = rows
            if multi_cat:
                run["logs"].append(f"[{ts}] ✅ Extract → {vol_cat} complete")
            else:
                run["logs"].append(f"[{ts}] ✅ Landing zone write complete")

        elif stage == "landing_to_bronze":
            if multi_cat:
                run["logs"].append(f"[{ts}] 📂 Reading from {vol_cat} UC Volumes…")
            else:
                run["logs"].append(f"[{ts}] 📂 Reading from landing zone…")
            time.sleep(1)
            rows = random.randint(1000, 50000)
            run["logs"].append(f"[{ts}] 🔄 Applying schema enforcement…")
            time.sleep(0.5)
            run["logs"].append(f"[{ts}] 📋 Adding audit columns (__bronze_ts, __source, __batch_id)…")
            time.sleep(0.5)
            if multi_cat:
                run["logs"].append(f"[{ts}] 💾 Writing to {brz_cat}.{tgt_sch}.{job['table_name']} (Bronze Delta)…")
            else:
                run["logs"].append(f"[{ts}] 💾 Writing to Bronze layer (Delta)…")
            time.sleep(1)
            run["rows_processed"] = rows
            if multi_cat:
                run["logs"].append(f"[{ts}] ✅ {vol_cat} → {brz_cat}.{tgt_sch} complete ({rows:,} rows)")
            else:
                run["logs"].append(f"[{ts}] ✅ Bronze layer write complete ({rows:,} rows)")

        elif stage == "bronze_to_silver":
            if multi_cat:
                run["logs"].append(f"[{ts}] 📂 Reading from {brz_cat}.{tgt_sch}.{job['table_name']}…")
            else:
                run["logs"].append(f"[{ts}] 📂 Reading from Bronze layer…")
            time.sleep(1)
            rows = random.randint(800, 45000)
            run["logs"].append(f"[{ts}] 🧹 Applying data quality checks…")
            time.sleep(0.5)
            run["logs"].append(f"[{ts}] 🔄 Deduplication and cleansing…")
            time.sleep(0.5)
            run["logs"].append(f"[{ts}] 📋 Applying business transformations…")
            time.sleep(0.5)
            rejected = random.randint(0, int(rows * 0.02))
            run["logs"].append(f"[{ts}] ⚠️ {rejected} rows rejected by quality checks")
            if multi_cat:
                run["logs"].append(f"[{ts}] 💾 Writing to {slv_cat}.{tgt_sch}.{job['table_name']} (Silver Delta)…")
            else:
                run["logs"].append(f"[{ts}] 💾 Writing to Silver layer (Delta)…")
            time.sleep(1)
            run["rows_processed"] = rows - rejected
            if multi_cat:
                run["logs"].append(f"[{ts}] ✅ {brz_cat}.{tgt_sch} → {slv_cat}.{tgt_sch} complete ({rows - rejected:,} rows)")
            else:
                run["logs"].append(f"[{ts}] ✅ Silver layer write complete ({rows - rejected:,} rows)")

        # Update watermark if incremental
        if job["load_type"] == "incremental" and job.get("watermark_column"):
            new_wm = datetime.now().isoformat()
            with _lock:
                WATERMARKS[job["full_table"]] = {
                    "column":     job["watermark_column"],
                    "last_value": new_wm,
                    "updated_at": datetime.now().isoformat(),
                }
            run["logs"].append(f"[{ts}] 💾 Watermark updated: {job['watermark_column']} → {new_wm}")
            _sync_watermark_to_dbr(job["full_table"], WATERMARKS[job["full_table"]])

        # Mark success
        end_ts = datetime.now().isoformat()
        with _lock:
            run["status"] = "success"
            run["completed_at"] = end_ts
            run["duration_sec"] = round((datetime.fromisoformat(end_ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1)
            run["logs"].append(f"[{end_ts}] ✅ Job completed successfully in {run['duration_sec']}s")
            job["status"] = "success"
            job["last_status"] = "success"
            job["updated_at"] = end_ts

        # Sync to Databricks
        _sync_run_to_dbr(run)
        _sync_job_to_dbr(job)

    except Exception as e:
        end_ts = datetime.now().isoformat()
        with _lock:
            run["status"] = "failed"
            run["error"] = str(e)
            run["completed_at"] = end_ts
            run["duration_sec"] = round((datetime.fromisoformat(end_ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1)
            run["logs"].append(f"[{end_ts}] ❌ Job FAILED: {e}")
            job["status"] = "failed"
            job["last_status"] = "failed"
            job["fail_count"] += 1
            job["updated_at"] = end_ts

        # Sync failure to Databricks
        _sync_run_to_dbr(run)
        _sync_job_to_dbr(job)


# ─────────────────────────────────────────────────────────────────────────────
#  RUN ENTIRE PIPELINE GROUP
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline_group(group_id: str, force_full: bool = False) -> dict:
    """Run all jobs in a pipeline group sequentially."""
    grp = PIPELINE_GROUPS.get(group_id)
    if not grp:
        return {"success": False, "error": f"Pipeline group '{group_id}' not found"}

    run_results = []
    for jid in grp.get("job_ids", []):
        r = run_job(jid, force_full=force_full)
        run_results.append(r)

    return {
        "success": True,
        "group_id": group_id,
        "runs": run_results,
        "total_jobs": len(run_results),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  RERUN FROM FAILURE
# ─────────────────────────────────────────────────────────────────────────────
def rerun_from_failure(group_id: str) -> dict:
    """Rerun a pipeline group starting from the first failed job."""
    grp = PIPELINE_GROUPS.get(group_id)
    if not grp:
        return {"success": False, "error": f"Pipeline group '{group_id}' not found"}

    jobs = [JOB_REGISTRY.get(jid) for jid in grp.get("job_ids", []) if JOB_REGISTRY.get(jid)]
    jobs.sort(key=lambda j: j["order"])

    # Find first failed job
    start_from = None
    for j in jobs:
        if j["status"] == "failed":
            start_from = j["order"]
            break

    if start_from is None:
        return {"success": False, "error": "No failed jobs found in this pipeline"}

    run_results = []
    for j in jobs:
        if j["order"] >= start_from:
            r = run_job(j["job_id"])
            run_results.append(r)

    return {
        "success":     True,
        "group_id":    group_id,
        "rerun_from":  start_from,
        "runs":        run_results,
        "total_reran": len(run_results),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  GET RUN STATUS
# ─────────────────────────────────────────────────────────────────────────────
def get_run_status(run_id: str) -> dict:
    """Get status and logs of a specific run."""
    run = JOB_RUNS.get(run_id)
    if not run:
        return {"success": False, "error": f"Run '{run_id}' not found"}
    return {"success": True, "run": run}


# ─────────────────────────────────────────────────────────────────────────────
#  GET ALL RUNS
# ─────────────────────────────────────────────────────────────────────────────
def list_runs(job_id: str = None, group_id: str = None, status: str = None, limit: int = 50) -> dict:
    """List run history with optional filters."""
    runs = list(JOB_RUNS.values())
    if job_id:
        runs = [r for r in runs if r["job_id"] == job_id]
    if group_id:
        # Get all job_ids belonging to this pipeline group
        grp = PIPELINE_GROUPS.get(group_id)
        grp_job_ids = set(grp["job_ids"]) if grp else set()
        runs = [r for r in runs if r["job_id"] in grp_job_ids]
    if status:
        runs = [r for r in runs if r["status"] == status]
    runs.sort(key=lambda r: r["started_at"], reverse=True)
    return {"success": True, "runs": runs[:limit], "total": len(runs)}


# ─────────────────────────────────────────────────────────────────────────────
#  WATERMARK MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
def get_watermarks() -> dict:
    """Get all watermark entries."""
    return {"success": True, "watermarks": dict(WATERMARKS)}


def update_watermark(table_name: str, column: str, value: str) -> dict:
    """Manually update a watermark value."""
    with _lock:
        WATERMARKS[table_name] = {
            "column":     column,
            "last_value": value,
            "updated_at": datetime.now().isoformat(),
        }
    _sync_watermark_to_dbr(table_name, WATERMARKS[table_name])
    return {"success": True, "table": table_name, "watermark": WATERMARKS[table_name]}


def reset_watermark(table_name: str) -> dict:
    """Reset watermark to force full reload on next incremental run."""
    if table_name not in WATERMARKS:
        return {"success": False, "error": f"No watermark for '{table_name}'"}
    with _lock:
        WATERMARKS[table_name]["last_value"] = None
        WATERMARKS[table_name]["updated_at"] = datetime.now().isoformat()
    _sync_watermark_to_dbr(table_name, WATERMARKS[table_name])
    return {"success": True, "table": table_name, "message": "Watermark reset — next run will do full load"}


# ─────────────────────────────────────────────────────────────────────────────
#  DASHBOARD STATS
# ─────────────────────────────────────────────────────────────────────────────
def get_dashboard_stats() -> dict:
    """Aggregate statistics for the workflow dashboard."""
    jobs = list(JOB_REGISTRY.values())
    runs = list(JOB_RUNS.values())
    groups = list(PIPELINE_GROUPS.values())

    total_jobs = len(jobs)
    running_jobs = sum(1 for j in jobs if j["status"] == "running")
    success_jobs = sum(1 for j in jobs if j["status"] == "success")
    failed_jobs = sum(1 for j in jobs if j["status"] == "failed")
    disabled_jobs = sum(1 for j in jobs if not j.get("enabled", True))

    total_runs = len(runs)
    success_runs = sum(1 for r in runs if r["status"] == "success")
    failed_runs = sum(1 for r in runs if r["status"] == "failed")
    total_rows = sum(r.get("rows_processed", 0) for r in runs)

    # Per-stage job counts
    extract_jobs = sum(1 for j in jobs if j.get("stage") == "extract")
    ingest_jobs = sum(1 for j in jobs if j.get("stage") == "landing_to_bronze")
    cleanse_jobs = sum(1 for j in jobs if j.get("stage") == "bronze_to_silver")

    return {
        "success": True,
        "stats": {
            "total_pipelines":  len(groups),
            "total_jobs":       total_jobs,
            "running_jobs":     running_jobs,
            "success_jobs":     success_jobs,
            "failed_jobs":      failed_jobs,
            "disabled_jobs":    disabled_jobs,
            "total_runs":       total_runs,
            "success_runs":     success_runs,
            "failed_runs":      failed_runs,
            "total_rows":       total_rows,
            "total_rows_processed": total_rows,
            "extract_jobs":     extract_jobs,
            "ingest_jobs":      ingest_jobs,
            "cleanse_jobs":     cleanse_jobs,
            "watermarks":       len(WATERMARKS),
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
#  ADD SINGLE CUSTOM JOB
# ─────────────────────────────────────────────────────────────────────────────
def add_custom_job(
    job_name: str,
    stage: str,
    table_schema: str = "dbo",
    table_name: str = "",
    load_type: str = "full",
    watermark_column: str = "",
    group_id: str = None,
) -> dict:
    """Add a single custom job to an existing or new group."""
    job_id = uuid.uuid4().hex[:12]
    ts = datetime.now().isoformat()
    full_table = f"{table_schema}.{table_name}" if table_name else ""

    if not group_id:
        group_id = uuid.uuid4().hex[:12]
        PIPELINE_GROUPS[group_id] = {
            "group_id":         group_id,
            "table_schema":     table_schema,
            "table_name":       table_name,
            "full_table":       full_table,
            "load_type":        load_type,
            "watermark_column": watermark_column,
            "job_ids":          [],
            "status":           "created",
            "created_at":       ts,
        }

    job = {
        "job_id":           job_id,
        "job_name":         job_name,
        "stage":            stage,
        "group_id":         group_id,
        "table_schema":     table_schema,
        "table_name":       table_name,
        "full_table":       full_table,
        "load_type":        load_type,
        "watermark_column": watermark_column,
        "status":           "created",
        "last_run_id":      None,
        "last_run_at":      None,
        "last_status":      None,
        "run_count":        0,
        "fail_count":       0,
        "created_at":       ts,
        "updated_at":       ts,
        "source_config":    {},
        "target_config":    {},
        "order":            {"extract": 1, "landing_to_bronze": 2, "bronze_to_silver": 3}.get(stage, 1),
        "enabled":          True,
    }

    with _lock:
        JOB_REGISTRY[job_id] = job
        if group_id in PIPELINE_GROUPS:
            PIPELINE_GROUPS[group_id]["job_ids"].append(job_id)

    _sync_job_to_dbr(job)
    if group_id in PIPELINE_GROUPS:
        _sync_pipeline_to_dbr(PIPELINE_GROUPS[group_id])
    return {"success": True, "job": job}


# ─────────────────────────────────────────────────────────────────────────────
#  DEPLOY METADATA-DRIVEN NOTEBOOKS TO DATABRICKS
# ─────────────────────────────────────────────────────────────────────────────
_notebooks_deployed = False          # tracks if notebooks have been uploaded
_notebooks_workspace_path = ""       # e.g. "/Shared/MetadataPipeline"

def deploy_metadata_notebooks(
    host: str = "",
    token: str = "",
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    workspace_path: str = "/Shared/MetadataPipeline",
    pipeline_mode: str = "standard",
) -> dict:
    """
    Generate metadata-driven notebooks and upload them to Databricks.
    pipeline_mode: "standard" (4 notebooks) or "dlt" (3 DLT notebooks).
    """
    global _notebooks_deployed, _notebooks_workspace_path

    host  = host or _dbr_host
    token = token or _dbr_token
    if not host or not token:
        return {"success": False, "error": "Databricks host and token required. Initialise MetadataFlow first."}

    # 1. Generate notebooks
    from metadata_notebooks import generate_metadata_notebooks
    gen_result = generate_metadata_notebooks(
        catalog=catalog or _dbr_catalog or "main",
        schema=schema or _dbr_schema or "default",
        landing_path=landing_path,
        workspace_path=workspace_path,
        pipeline_mode=pipeline_mode,
    )
    if not gen_result.get("success"):
        return gen_result

    # 2. Upload each notebook via Databricks Workspace API
    from databricks_connector import DatabricksConnector
    connector = DatabricksConnector(host, token)

    results = []
    for nb in gen_result["notebooks"]:
        r = connector.upload_notebook(
            notebook_name=nb["name"],
            python_code=nb["code"],
            path=workspace_path,
        )
        results.append({
            "name":    nb["name"],
            "layer":   nb["layer"],
            "lines":   nb["lines"],
            "success": r.get("success", False),
            "path":    r.get("notebook_path") or r.get("path"),
            "url":     r.get("workspace_url"),
            "error":   r.get("error") if not r.get("success") else None,
        })

    ok = sum(1 for r in results if r["success"])
    if ok > 0:
        _notebooks_deployed = True
        _notebooks_workspace_path = workspace_path

    return {
        "success":        ok > 0,
        "uploaded":       ok,
        "total":          len(results),
        "results":        results,
        "workspace_path": workspace_path,
        "message":        f"Deployed {ok}/{len(results)} metadata notebooks to {workspace_path}",
    }


def get_notebook_status() -> dict:
    """Return whether metadata notebooks have been deployed."""
    return {
        "success":    True,
        "deployed":   _notebooks_deployed,
        "workspace_path": _notebooks_workspace_path,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  POLL DATABRICKS RUN STATUS (background thread)
# ─────────────────────────────────────────────────────────────────────────────
def _poll_databricks_run(connector, dbr_run_id, group_id: str):
    """
    Background poller — checks a Databricks run every 10s and updates
    the corresponding JOB_RUNS entries so Pipeline Logs stay current.
    """
    import time
    terminal_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    grp = PIPELINE_GROUPS.get(group_id)
    grp_job_ids = set(grp["job_ids"]) if grp else set()
    dbr_run_str = str(dbr_run_id)        # normalise once for comparisons
    consecutive_errors = 0

    for _attempt in range(360):          # poll up to ~1 hour
        time.sleep(10)
        try:
            status = connector.get_run_status(int(dbr_run_id))
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            ts = datetime.now().isoformat()
            # Log polling errors so they're visible in Pipeline Logs
            if consecutive_errors <= 3:
                with _lock:
                    for rid, run in JOB_RUNS.items():
                        if str(run.get("dbr_run_id", "")) == dbr_run_str and run["job_id"] in grp_job_ids:
                            run["logs"].append(f"[{ts}] ⚠️ Polling error #{consecutive_errors}: {exc}")
            if consecutive_errors >= 30:
                # Give up after ~5 minutes of consecutive errors
                with _lock:
                    for rid, run in JOB_RUNS.items():
                        if str(run.get("dbr_run_id", "")) == dbr_run_str and run["job_id"] in grp_job_ids:
                            run["status"] = "failed"
                            run["completed_at"] = ts
                            run["error"] = f"Polling abandoned after {consecutive_errors} consecutive errors"
                            run["logs"].append(f"[{ts}] ❌ Polling abandoned — check Databricks UI for run status")
                            _sync_run_to_dbr(run)
                    if grp:
                        grp["status"] = "failed"
                        _sync_pipeline_to_dbr(grp)
                return
            continue

        if not status.get("success"):
            consecutive_errors += 1
            continue

        lifecycle    = status.get("life_cycle", "UNKNOWN")
        result_state = status.get("result_state", "")
        state_msg    = status.get("state_message", "")
        ts           = datetime.now().isoformat()

        # Map Databricks states → local status
        if lifecycle in terminal_states:
            if result_state == "SUCCESS":
                local_status = "success"
                emoji = "✅"
            else:
                local_status = "failed"
                emoji = "❌"

            # ── Fetch notebook output / error trace on completion ──
            output_info = connector.get_run_output(int(dbr_run_id))
            output_lines = []
            dlt_failed = False
            if output_info.get("success"):
                nb_result = output_info.get("notebook_result", "")
                error_trace = output_info.get("error_trace", "")
                error_msg = output_info.get("error", "")
                tasks = output_info.get("tasks", [])
                if nb_result:
                    output_lines.append(f"[{ts}] 📄 Notebook result: {nb_result[:500]}")
                    # Parse DLT status from notebook result JSON
                    try:
                        import json as _json
                        _nr = _json.loads(nb_result)
                        if _nr.get("dlt_status") == "FAILED":
                            dlt_failed = True
                            output_lines.append(f"[{ts}] ⚠️ DLT pipeline FAILED — Extract OK but Bronze/Silver not processed")
                    except Exception:
                        pass
                if error_msg:
                    output_lines.append(f"[{ts}] 🔴 Error: {error_msg[:500]}")
                if error_trace:
                    output_lines.append(f"[{ts}] 📋 Trace: {error_trace[:1000]}")
                for tk in tasks:
                    t_status = f"{tk['task_key']}: {tk['result_state'] or tk['life_cycle']}"
                    if tk.get("state_message"):
                        t_status += f" — {tk['state_message'][:200]}"
                    output_lines.append(f"[{ts}] 📌 Task {t_status}")

            with _lock:
                for rid, run in JOB_RUNS.items():
                    if str(run.get("dbr_run_id", "")) == dbr_run_str and run["job_id"] in grp_job_ids:
                        # For DLT partial failure: extract=success, bronze/silver=failed
                        if dlt_failed and run.get("stage") == "extract":
                            run["status"] = "success"
                        elif dlt_failed:
                            run["status"] = "failed"
                            run["error"] = "DLT pipeline failed — redeploy notebooks and re-run"
                        else:
                            run["status"] = local_status
                        run["completed_at"] = ts
                        run["duration_sec"] = round(
                            (datetime.fromisoformat(ts) - datetime.fromisoformat(run["started_at"])).total_seconds(), 1
                        )
                        run["logs"].append(f"[{ts}] {emoji} Databricks run {lifecycle} — {result_state or state_msg}")
                        run["logs"].extend(output_lines)
                        if local_status == "failed":
                            run["error"] = output_info.get("error") or output_info.get("error_trace", "")[:500] or state_msg
                        _sync_run_to_dbr(run)

                if grp:
                    grp["status"] = "failed" if dlt_failed else local_status
                    _sync_pipeline_to_dbr(grp)

            return  # done

        else:
            # Still running — update log with latest lifecycle state
            with _lock:
                for rid, run in JOB_RUNS.items():
                    if str(run.get("dbr_run_id", "")) == dbr_run_str and run["job_id"] in grp_job_ids:
                        last_log = run["logs"][-1] if run["logs"] else ""
                        status_line = f"[{ts}] 🔄 Databricks: {lifecycle}"
                        if state_msg:
                            status_line += f" — {state_msg}"
                        # Avoid duplicate consecutive status lines
                        if "🔄 Databricks:" not in last_log:
                            run["logs"].append(status_line)
                        else:
                            run["logs"][-1] = status_line


# ─────────────────────────────────────────────────────────────────────────────
#  RUN PIPELINE ON DATABRICKS (real notebook execution)
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline_on_databricks(
    group_id: str,
    host: str = "",
    token: str = "",
    cluster_id: str = "",
    load_type: str = "",
    password: str = "",
    workspace_path: str = "",
    catalog: str = "",
    schema: str = "",
    landing_path: str = "/mnt/landing",
) -> dict:
    """
    Submit the 00_Meta_Orchestrator notebook on Databricks to run a pipeline
    group (or all groups if group_id is empty).
    This is the REAL execution path — it creates a Databricks job run.
    """
    host  = host or _dbr_host
    token = token or _dbr_token
    ws    = workspace_path or _notebooks_workspace_path or "/Shared/MetadataPipeline"
    cat   = catalog or _dbr_catalog or "main"
    sch   = schema or _dbr_schema or "default"

    if not host or not token:
        return {"success": False, "error": "Databricks host and token required"}

    from databricks_connector import DatabricksConnector
    import base64
    connector = DatabricksConnector(host, token)

    # Base64-encode the password to safely pass special chars (# ; { } etc.)
    # through Databricks widget parameters.  Decoded in the notebook.
    pwd_b64 = base64.b64encode((password or "").encode("utf-8")).decode("ascii")

    params = {
        "group_id":       group_id or "",
        "load_type":      load_type or "",
        "password_b64":   pwd_b64,
        "catalog":        cat,
        "schema":         sch,
        "landing_path":   landing_path,
        "workspace_path": ws,
    }

    result = connector.run_notebook(
        notebook_path=f"{ws}/00_Meta_Orchestrator",
        cluster_id=cluster_id or None,
        params=params,
    )

    # Normalise: connector uses 'message' but frontend expects 'error'
    if not result.get("success") and "message" in result and "error" not in result:
        result["error"] = result["message"]

    ts = datetime.now().isoformat()
    grp = PIPELINE_GROUPS.get(group_id)

    if result.get("success"):
        # Update group status
        if grp:
            grp["status"] = "running"
            _sync_pipeline_to_dbr(grp)

        # ── Create JOB_RUNS entries so Pipeline Logs can display them ──
        dbr_run_id = result.get("run_id", "?")
        run_url    = result.get("run_url", "")
        if grp:
            for jid in grp.get("job_ids", []):
                job = JOB_REGISTRY.get(jid)
                if not job:
                    continue
                local_run_id = uuid.uuid4().hex[:12]
                run_entry = {
                    "run_id":           local_run_id,
                    "job_id":           jid,
                    "job_name":         job["job_name"],
                    "stage":            job["stage"],
                    "full_table":       job.get("full_table", ""),
                    "load_type":        job.get("load_type", ""),
                    "watermark_column": job.get("watermark_column", ""),
                    "watermark_value":  None,
                    "status":           "running",
                    "started_at":       ts,
                    "completed_at":     None,
                    "duration_sec":     None,
                    "rows_processed":   0,
                    "error":            None,
                    "dbr_run_id":       dbr_run_id,
                    "logs": [
                        f"[{ts}] ⚡ Submitted to Databricks (run {dbr_run_id})",
                        f"[{ts}] 📋 Stage: {job['stage']}  ·  Table: {job.get('full_table', '')}",
                        f"[{ts}] 🔗 {run_url}" if run_url else f"[{ts}] 🔄 Awaiting cluster…",
                    ],
                }
                with _lock:
                    JOB_RUNS[local_run_id] = run_entry
                    job["last_run_id"] = local_run_id
                    job["last_run_at"] = ts
                    job["status"] = "running"
                _sync_run_to_dbr(run_entry)
                _sync_job_to_dbr(job)

        # Start background status poller for this Databricks run
        t = threading.Thread(
            target=_poll_databricks_run,
            args=(connector, dbr_run_id, group_id),
            daemon=True,
        )
        t.start()

    else:
        # Submission failed — record a failed run so user sees the error in logs
        if grp:
            for jid in grp.get("job_ids", []):
                job = JOB_REGISTRY.get(jid)
                if not job:
                    continue
                local_run_id = uuid.uuid4().hex[:12]
                err_msg = result.get("error") or result.get("message") or "Unknown error"
                run_entry = {
                    "run_id":           local_run_id,
                    "job_id":           jid,
                    "job_name":         job["job_name"],
                    "stage":            job["stage"],
                    "full_table":       job.get("full_table", ""),
                    "load_type":        job.get("load_type", ""),
                    "watermark_column": job.get("watermark_column", ""),
                    "watermark_value":  None,
                    "status":           "failed",
                    "started_at":       ts,
                    "completed_at":     ts,
                    "duration_sec":     0,
                    "rows_processed":   0,
                    "error":            err_msg,
                    "logs": [
                        f"[{ts}] ⚡ Databricks submit attempted",
                        f"[{ts}] ❌ {err_msg}",
                    ],
                }
                with _lock:
                    JOB_RUNS[local_run_id] = run_entry
                    job["status"] = "failed"
                _sync_run_to_dbr(run_entry)
                _sync_job_to_dbr(job)

    return result
