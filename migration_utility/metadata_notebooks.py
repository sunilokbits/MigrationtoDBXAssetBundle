"""
Metadata-Driven Medallion Notebooks
=====================================
Generates Databricks notebooks that read pipeline/job metadata from Delta tables
and dynamically execute the appropriate ETL stage.

Unlike the static `medallion_notebooks.py` (which embeds table lists in code),
these notebooks query the `wf_job_metadata` / `wf_pipeline_metadata` Delta tables
at runtime to determine WHAT to extract, WHERE to land, and HOW to transform.

The generated notebooks are idempotent — you deploy them ONCE, then trigger them
with parameters (job_id, run_id, load_type) from the Workflow Manager.

Notebooks produced:
  1. 00_Meta_Orchestrator.py  — reads metadata, chains Extract → Bronze → Silver
  2. 01_Meta_Extract.py       — JDBC extraction driven by metadata
  3. 02_Meta_Bronze.py        — Landing → Bronze (Delta) driven by metadata
  4. 03_Meta_Silver.py        — Bronze → Silver (Delta) driven by metadata
"""

import json
import os
from datetime import datetime


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DEPLOY CONFIG LOADER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _load_deploy_config() -> dict:
    """Load deployconfig.json from the same directory as this module."""
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "deployconfig.json")
    if os.path.isfile(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_metadata_notebooks(
    catalog: str = "",
    schema: str = "",
    landing_path: str = "",
    workspace_path: str = "/Shared/MetadataPipeline",
    pipeline_mode: str = "standard",
    recon_catalog: str = "",
    recon_schema: str = "",
    recon_table: str = "",
    log_catalog: str = "",
    log_schema: str = "",
    log_table: str = "",
    recon_location: str = "",
    log_location: str = "",
    cdc_mode: str = "watermark",
    primary_keys: list = None,
) -> dict:
    """
    Generate metadata-driven notebooks.
    pipeline_mode: "standard" (4+2 notebooks) or "dlt" (3+2 notebooks with DLT).
    All parameters fall back to values from deployconfig.json when not provided.
    Returns: {success, notebooks: [{name, code, description, layer, lines}], summary}
    """
    # ── Load defaults from deployconfig.json ──────────────────────────
    cfg = _load_deploy_config()
    recon_cfg = cfg.get("reconciliation", {})
    log_cfg   = cfg.get("logging", {})

    catalog        = catalog        or cfg.get("catalogs", {}).get("bronze", {}).get("schemas", [""])[0] and "main"
    schema         = schema         or "default"
    landing_path   = landing_path   or cfg.get("volume_path", "/mnt/landing")
    recon_catalog  = recon_catalog  or recon_cfg.get("catalog", "reconciliation")
    recon_schema   = recon_schema   or recon_cfg.get("schema", "hr")
    recon_table    = recon_table    or recon_cfg.get("table", "ReconcilationDetails")
    recon_location = recon_location or recon_cfg.get("location", "")
    log_catalog    = log_catalog    or log_cfg.get("catalog", "logging")
    log_schema     = log_schema     or log_cfg.get("schema", "hr")
    log_table      = log_table      or log_cfg.get("table", "ExecutionLog")
    log_location   = log_location   or log_cfg.get("location", "")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    if pipeline_mode == "dlt":
        notebooks = [
            {
                "name":        "01_Meta_Extract",
                "code":        _gen_extract(catalog, schema, landing_path, ts),
                "description": "Metadata-driven JDBC extraction → Landing Zone",
                "layer":       "extract",
            },
            {
                "name":        "02_Meta_DLT_Pipeline",
                "code":        _gen_dlt_pipeline(catalog, schema, landing_path, ts),
                "description": "DLT Pipeline — Bronze + Silver with Auto Loader & expectations",
                "layer":       "dlt",
            },
            {
                "name":        "00_Meta_Orchestrator",
                "code":        _gen_orchestrator_dlt(catalog, schema, landing_path, workspace_path, ts),
                "description": "DLT Orchestrator — Extract → DLT pipeline trigger",
                "layer":       "orchestrator",
            },
            {
                "name":        "04_Meta_Reconciliation",
                "code":        _gen_reconciliation(catalog, schema, landing_path, recon_catalog, recon_schema, recon_table, ts, recon_location=recon_location),
                "description": "Aggregate reconciliation — Source vs Bronze numeric column validation",
                "layer":       "reconciliation",
            },
            {
                "name":        "05_Meta_ExecutionLog",
                "code":        _gen_execution_log(catalog, schema, log_catalog, log_schema, log_table, ts, log_location=log_location),
                "description": "Execution logging — saves per-job run details to logging catalog",
                "layer":       "logging",
            },
        ]
    else:
        notebooks = [
            {
                "name":        "01_Meta_Extract",
                "code":        _gen_extract(catalog, schema, landing_path, ts),
                "description": "Metadata-driven JDBC extraction → Landing Zone",
                "layer":       "extract",
            },
            {
                "name":        "02_Meta_Bronze",
                "code":        _gen_bronze(catalog, schema, landing_path, ts),
                "description": "Metadata-driven Landing → Bronze Delta layer",
                "layer":       "bronze",
            },
            {
                "name":        "03_Meta_Silver",
                "code":        _gen_silver(catalog, schema, ts),
                "description": "Metadata-driven Bronze → Silver Delta (cleansed)",
                "layer":       "silver",
            },
            {
                "name":        "00_Meta_Orchestrator",
                "code":        _gen_orchestrator(catalog, schema, landing_path, workspace_path, ts, recon_catalog, recon_schema, recon_table, log_catalog, log_schema, log_table),
                "description": "Orchestrator — reads metadata, chains all stages",
                "layer":       "orchestrator",
            },
            {
                "name":        "04_Meta_Reconciliation",
                "code":        _gen_reconciliation(catalog, schema, landing_path, recon_catalog, recon_schema, recon_table, ts, recon_location=recon_location),
                "description": "Aggregate reconciliation — Source vs Bronze numeric column validation",
                "layer":       "reconciliation",
            },
            {
                "name":        "05_Meta_ExecutionLog",
                "code":        _gen_execution_log(catalog, schema, log_catalog, log_schema, log_table, ts, log_location=log_location),
                "description": "Execution logging — saves per-job run details to logging catalog",
                "layer":       "logging",
            },
        ]

    for nb in notebooks:
        nb["lines"] = nb["code"].count("\n") + 1

    return {
        "success":   True,
        "notebooks": notebooks,
        "summary": {
            "total_notebooks":  len(notebooks),
            "catalog":          catalog,
            "schema":           schema,
            "landing_path":     landing_path,
            "workspace_path":   workspace_path,
            "pipeline_mode":    pipeline_mode,
            "multi_catalog":    "Supported — reads volumes_catalog, bronze_catalog, silver_catalog, target_schema from target_config at runtime",
            "generated_at":     ts,
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  1. METADATA-DRIVEN EXTRACT NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_extract(catalog, schema, landing_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Metadata-Driven Extract — Landing Zone
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC This notebook reads job metadata from Delta tables and extracts
# MAGIC the specified source table via JDBC into the Landing Zone.
# MAGIC
# MAGIC **Parameters (widgets):**
# MAGIC - `job_id` — The job to execute (looked up from `wf_job_metadata`)
# MAGIC - `run_id` — Run tracking ID (written to `wf_run_history`)
# MAGIC - `load_type` — `full` or `incremental` (override)
# MAGIC - `password_b64` — Base64-encoded source DB password (use Databricks Secrets in prod)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")

import base64

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
LOAD_OVERRIDE= dbutils.widgets.get("load_type").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
# Decode base64 password — special chars like # ; {{ }} are safe this way
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()

print(f"🔧 Job ID  : {{JOB_ID}}")
print(f"🔧 Run ID  : {{RUN_ID}}")
print(f"🔧 Catalog : {{CATALOG}}.{{SCHEMA}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Read Job Metadata from Delta

# COMMAND ----------

import json
from pyspark.sql import functions as F
from datetime import datetime

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
wm_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_watermark_metadata"
run_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{JOB_ID}}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": f"Job {{JOB_ID}} not found in metadata"}}))

job = job_df.collect()[0].asDict()
print(f"📋 Job Name       : {{job['job_name']}}")
print(f"📋 Table           : {{job['full_table']}}")
print(f"📋 Stage           : {{job['stage']}}")
print(f"📋 Load Type (meta): {{job['load_type']}}")

# Parse source config (JSON string)
source_config = json.loads(job.get("source_config", "{{}}") or "{{}}")
SERVER   = source_config.get("server", "")
DATABASE = source_config.get("database", "")
USERNAME = source_config.get("username", "")
SRC_TYPE = source_config.get("source_type", "sqlserver")
TABLE_SCHEMA = job["table_schema"]
TABLE_NAME   = job["table_name"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = LOAD_OVERRIDE if LOAD_OVERRIDE else (job["load_type"] or "full")
WM_COL       = job.get("watermark_column", "")

# Multi-catalog: override landing path with UC Volumes
target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
VOLUMES_CATALOG = target_config.get("volumes_catalog", "")
TGT_SCHEMA      = target_config.get("target_schema", "")
if VOLUMES_CATALOG and TGT_SCHEMA:
    LANDING_PATH = f"/Volumes/{{VOLUMES_CATALOG}}/{{TGT_SCHEMA}}/landing"
    print(f"📦 Multi-catalog: Landing → UC Volumes: {{LANDING_PATH}}")
    # Auto-create schema and volume if they don't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{VOLUMES_CATALOG}}`.`{{TGT_SCHEMA}}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{{VOLUMES_CATALOG}}`.`{{TGT_SCHEMA}}`.`landing`")
    print(f"✅ Ensured volume exists: {{VOLUMES_CATALOG}}.{{TGT_SCHEMA}}.landing")

print(f"🔧 Source: {{SRC_TYPE}} → {{SERVER}}/{{DATABASE}}")
print(f"🔧 Load Type: {{LOAD_TYPE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 JDBC Connection

# COMMAND ----------

encrypt = "true" if SRC_TYPE in ("azuresql", "synapse") else "false"
trust   = "false" if SRC_TYPE in ("azuresql", "synapse") else "true"

# Normalize server address to hostname:port for JDBC
# Azure SQL often uses comma notation (server.database.windows.net,1433) but
# the JDBC driver only accepts colon notation (server:1433) in the URL.
if "," in SERVER:
    _host, _port = SERVER.rsplit(",", 1)
elif ":" in SERVER:
    _host, _port = SERVER.rsplit(":", 1)
else:
    _host, _port = SERVER, "1433"
print(f"🔧 JDBC target: {{_host}}:{{_port}}")

jdbc_url = f"jdbc:sqlserver://{{_host}}:{{_port}};databaseName={{DATABASE}};encrypt={{encrypt}};trustServerCertificate={{trust}}"

jdbc_props = {{
    "user":     USERNAME,
    "password": PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
    "loginTimeout": "30",
    "socketTimeout": "300",
}}

# Verify JDBC connectivity
try:
    test_df = spark.read.jdbc(jdbc_url, "(SELECT 1 AS ok) AS t", properties=jdbc_props)
    test_df.collect()
    print("✅ JDBC connection verified")
except Exception as e:
    msg = f"❌ JDBC connection failed: {{e}}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{str(e).replace("'","''")[:500]}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "connection", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Read Watermark (Incremental)

# COMMAND ----------

watermark = None
use_incremental = (LOAD_TYPE == "incremental" and WM_COL)

if use_incremental:
    try:
        wm_df = spark.sql(f"SELECT last_value FROM {{wm_tbl}} WHERE table_name = '{{FULL_TABLE}}'")
        rows = wm_df.collect()
        if rows and rows[0]["last_value"]:
            watermark = rows[0]["last_value"]
            print(f"🔄 Watermark found: {{WM_COL}} > '{{watermark}}'")
        else:
            print("🔄 No watermark — will do initial full load")
    except Exception:
        print("🔄 Watermark table not found — will do full load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Extract Data

# COMMAND ----------

run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
landing_dest = f"{{LANDING_PATH}}/{{TABLE_NAME}}"

# Build query
if use_incremental and watermark:
    query = f"(SELECT * FROM [{{TABLE_SCHEMA}}].[{{TABLE_NAME}}] WHERE [{{WM_COL}}] > '{{watermark}}') AS q"
    print(f"📥 Incremental extract: {{WM_COL}} > '{{watermark}}'")
else:
    query = f"[{{TABLE_SCHEMA}}].[{{TABLE_NAME}}]"
    print(f"📥 Full extract from [{{TABLE_SCHEMA}}].[{{TABLE_NAME}}]")

# Read from source
try:
    df = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)

    # Add audit columns
    df = (df
          .withColumn("__landing_ts", F.current_timestamp())
          .withColumn("__source_system", F.lit(f"{{SERVER}}/{{DATABASE}}"))
          .withColumn("__load_type", F.lit("incremental" if use_incremental and watermark else "full"))
          .withColumn("__batch_id", F.lit(run_ts))
          .withColumn("__job_id", F.lit(JOB_ID))
          .withColumn("__run_id", F.lit(RUN_ID)))

    row_count = df.count()
    print(f"📊 Rows extracted: {{row_count:,}}")

    # Write to landing zone
    if LOAD_TYPE == "full" or not (use_incremental and watermark):
        df.write.mode("overwrite").option("overwriteSchema", "true").parquet(landing_dest)
        print(f"✅ Written to {{landing_dest}} (overwrite)")
    else:
        df.write.mode("append").parquet(landing_dest)
        print(f"✅ Appended to {{landing_dest}}")

except Exception as e:
    msg = f"❌ Extract failed: {{e}}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{str(e).replace("'","''")[:500]}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "stage": "extract", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Update Watermark & Run History

# COMMAND ----------

# Update watermark if incremental
if use_incremental and WM_COL and row_count > 0:
    try:
        new_wm = df.agg(F.max(F.col(WM_COL)).cast("string")).collect()[0][0]
        if new_wm:
            spark.sql(f"""
                MERGE INTO {{wm_tbl}} AS t
                USING (SELECT '{{FULL_TABLE}}' AS table_name, '{{new_wm}}' AS last_value, '{{WM_COL}}' AS watermark_column, current_timestamp() AS updated_at) AS s
                ON t.table_name = s.table_name
                WHEN MATCHED THEN UPDATE SET t.last_value = s.last_value, t.updated_at = s.updated_at
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"💾 Watermark updated: {{WM_COL}} → {{new_wm}}")
    except Exception as e:
        print(f"⚠️ Watermark update failed: {{e}}")

# Update run history
try:
    spark.sql(f"""
        MERGE INTO {{run_tbl}} AS t
        USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {{row_count}},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"⚠️ Run history update failed: {{e}}")

# Update job metadata
try:
    spark.sql(f"""
        UPDATE {{job_tbl}}
        SET last_run_id = '{{RUN_ID}}',
            last_run_at = current_timestamp(),
            last_status = 'success',
            status = 'success',
            run_count = run_count + 1,
            updated_at = current_timestamp()
        WHERE job_id = '{{JOB_ID}}'
    """)
except Exception as e:
    print(f"⚠️ Job metadata update failed: {{e}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

exit_payload = json.dumps({{
    "status":       "COMPLETED",
    "job_id":       JOB_ID,
    "run_id":       RUN_ID,
    "table":        FULL_TABLE,
    "rows":         row_count,
    "load_type":    LOAD_TYPE,
    "landing_path": landing_dest,
    "batch_id":     run_ts,
}})

print(f"\\n✅ EXTRACT COMPLETE — {{FULL_TABLE}} — {{row_count:,}} rows")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  2. METADATA-DRIVEN BRONZE NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_bronze(catalog, schema, landing_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Metadata-Driven Bronze Layer
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads raw Parquet from Landing Zone, applies schema enforcement,
# MAGIC adds audit columns, and writes to Bronze Delta table.
# MAGIC Driven by `wf_job_metadata` — no hardcoded table names.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Read Job Metadata

# COMMAND ----------

import json
from pyspark.sql import functions as F
from datetime import datetime

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{JOB_ID}}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": f"Job {{JOB_ID}} not found"}}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
TABLE_SCHEMA = job["table_schema"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = job.get("load_type", "full") or "full"

target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
VOLUMES_CATALOG = target_config.get("volumes_catalog", "")
BRONZE_CATALOG  = target_config.get("bronze_catalog", "")
TGT_SCHEMA      = target_config.get("target_schema", "")
MULTI_CATALOG   = bool(VOLUMES_CATALOG and BRONZE_CATALOG and TGT_SCHEMA)

if MULTI_CATALOG:
    TARGET_CATALOG = BRONZE_CATALOG
    TARGET_SCHEMA  = TGT_SCHEMA
    TABLE_PREFIX   = ""
    LANDING_PATH   = f"/Volumes/{{VOLUMES_CATALOG}}/{{TGT_SCHEMA}}/landing"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`")
    print(f"📦 Multi-catalog mode: {{VOLUMES_CATALOG}} → {{BRONZE_CATALOG}}.{{TGT_SCHEMA}}")
else:
    TARGET_CATALOG = target_config.get("catalog", CATALOG)
    TARGET_SCHEMA  = target_config.get("schema", SCHEMA)
    TABLE_PREFIX   = "bronze_"

print(f"📋 Job: {{job['job_name']}}")
print(f"📋 Table: {{FULL_TABLE}}")
print(f"📋 Target: {{TARGET_CATALOG}}.{{TARGET_SCHEMA}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Read from Landing Zone

# COMMAND ----------

landing_src = f"{{LANDING_PATH}}/{{TABLE_NAME}}"
print(f"📂 Reading from: {{landing_src}}")

try:
    df = spark.read.parquet(landing_src)
    row_count = df.count()
    print(f"📊 Rows in landing: {{row_count:,}}")
except Exception as e:
    msg = f"❌ Failed to read landing zone: {{e}}"
    print(msg)
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{str(e).replace("'","''")[:500]}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Schema Enforcement & Data Quality Checks

# COMMAND ----------

# Create restore point if table exists
restore_version = None
bronze_table = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.`{{TABLE_PREFIX}}{{TABLE_NAME}}`"
try:
    history = spark.sql(f"DESCRIBE HISTORY {{bronze_table}} LIMIT 1").collect()
    if history:
        restore_version = history[0]["version"]
        print(f"📌 Restore point: v{{restore_version}}")
except Exception:
    print("📌 No existing table — first load")

# ── DQ-01: Empty file check ─────────────────────────────────────────
if row_count == 0:
    print("⚠️ DQ-01: Landing file has 0 rows — skipping Bronze write")
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'skipped',
                t.error_message = 'Empty landing file — 0 rows',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": "empty_landing", "rows": 0}}))

# ── DQ-02: Null-key row detection (all data cols null) ──────────────
audit_cols = [c for c in df.columns if c.startswith("__")]
data_cols  = [c for c in df.columns if c not in audit_cols]
null_key_count = 0
if data_cols:
    null_expr = data_cols[0]
    all_null = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        all_null = all_null & F.col(dc).isNull()
    null_key_count = df.filter(all_null).count()
    if null_key_count > 0:
        print(f"⚠️ DQ-02: {{null_key_count}} all-null rows detected")

# ── DQ-03: Duplicate detection ──────────────────────────────────────
dup_count = row_count - df.dropDuplicates(data_cols).count() if data_cols else 0
if dup_count > 0:
    print(f"⚠️ DQ-03: {{dup_count}} duplicate rows detected")

# ── DQ-04: Schema drift detection ───────────────────────────────────
schema_drift = False
try:
    existing = spark.sql(f"DESCRIBE {{bronze_table}}").select("col_name").rdd.flatMap(lambda x: x).collect()
    existing_data_cols = [c for c in existing if not c.startswith("__") and not c.startswith("#")]
    incoming_data_cols = [c for c in df.columns if not c.startswith("__")]
    new_cols     = set(incoming_data_cols) - set(existing_data_cols)
    dropped_cols = set(existing_data_cols) - set(incoming_data_cols)
    if new_cols:
        schema_drift = True
        print(f"⚠️ DQ-04 Schema drift — new columns: {{new_cols}}")
    if dropped_cols:
        schema_drift = True
        print(f"⚠️ DQ-04 Schema drift — missing columns: {{dropped_cols}}")
except Exception:
    pass  # Table doesn't exist yet

# ── DQ-05: Quarantine flagging ──────────────────────────────────────
# Flag rows with all-null data columns as quarantined instead of dropping
if data_cols:
    all_null_expr = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        all_null_expr = all_null_expr & F.col(dc).isNull()
    is_quarantined = all_null_expr
else:
    is_quarantined = F.lit(False)

# Add audit columns
df_bronze = (df
    .withColumn("__bronze_ts", F.current_timestamp())
    .withColumn("__bronze_version", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    .withColumn("__source_table", F.lit(FULL_TABLE))
    .withColumn("__job_id", F.lit(JOB_ID))
    .withColumn("__run_id", F.lit(RUN_ID))
    .withColumn("__is_quarantined", is_quarantined)
)

quarantined_count = df_bronze.filter(F.col("__is_quarantined") == True).count()
clean_count = row_count - quarantined_count

print(f"\\n📊 Bronze DQ Summary:")
print(f"   Total rows      : {{row_count:,}}")
print(f"   Clean rows      : {{clean_count:,}}")
print(f"   Quarantined     : {{quarantined_count:,}}")
print(f"   Null-key rows   : {{null_key_count}}")
print(f"   Duplicates      : {{dup_count}}")
print(f"   Schema drift    : {{'Yes' if schema_drift else 'No'}}")

# ── Save Bronze DQ metrics ──────────────────────────────────────────
try:
    dq_tbl = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.__dq_metrics"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {{dq_tbl}} (
            run_id STRING, job_id STRING, table_name STRING, layer STRING,
            input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
            null_rows BIGINT, dupe_rows BIGINT, quarantined_rows BIGINT,
            schema_drift BOOLEAN, dq_checks_passed INT, dq_checks_total INT,
            dq_score DOUBLE, checked_at TIMESTAMP
        ) USING DELTA
    """)
    checks_passed = sum([1 for c in [row_count > 0, null_key_count == 0, dup_count < row_count * 0.5, not schema_drift] if c])
    checks_total = 4
    dq_score = round(checks_passed / checks_total * 100, 1)
    spark.sql(f"""
        INSERT INTO {{dq_tbl}} VALUES (
            '{{RUN_ID}}', '{{JOB_ID}}', '{{FULL_TABLE}}', 'bronze',
            {{row_count}}, {{clean_count}}, {{quarantined_count}},
            {{null_key_count}}, {{dup_count}}, {{quarantined_count}},
            {{'true' if schema_drift else 'false'}}, {{checks_passed}}, {{checks_total}},
            {{dq_score}}, current_timestamp()
        )
    """)
except Exception as e:
    print(f"⚠️ DQ metrics save failed: {{e}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write to Bronze Delta

# COMMAND ----------

try:
    if LOAD_TYPE == "full":
        (df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table))
        print(f"✅ Full load → {{bronze_table}} ({{row_count:,}} rows)")
    else:
        (df_bronze.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table))
        print(f"✅ Append → {{bronze_table}} ({{row_count:,}} rows)")

except Exception as e:
    # Attempt restore on failure
    if restore_version is not None:
        try:
            spark.sql(f"RESTORE TABLE {{bronze_table}} TO VERSION AS OF {{restore_version}}")
            print(f"🔄 Restored to v{{restore_version}} after failure")
        except Exception:
            pass
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{str(e).replace("'","''")[:500]}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Update Metadata

# COMMAND ----------

try:
    spark.sql(f"""
        MERGE INTO {{run_tbl}} AS t
        USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {{row_count}},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"⚠️ Run history update failed: {{e}}")

try:
    spark.sql(f"""
        UPDATE {{job_tbl}}
        SET last_run_id = '{{RUN_ID}}', last_run_at = current_timestamp(),
            last_status = 'success', status = 'success',
            run_count = run_count + 1, updated_at = current_timestamp()
        WHERE job_id = '{{JOB_ID}}'
    """)
except Exception as e:
    print(f"⚠️ Job update failed: {{e}}")

# COMMAND ----------

exit_payload = json.dumps({{
    "status": "COMPLETED", "job_id": JOB_ID, "run_id": RUN_ID,
    "table": FULL_TABLE, "rows": row_count, "bronze_table": bronze_table,
}})
print(f"\\n✅ BRONZE COMPLETE — {{FULL_TABLE}} — {{row_count:,}} rows")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  3. METADATA-DRIVEN SILVER NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_silver(catalog, schema, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Metadata-Driven Silver Layer
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads Bronze Delta, applies data quality checks, deduplication,
# MAGIC cleansing, and writes to Silver Delta table.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")

JOB_ID  = dbutils.widgets.get("job_id").strip()
RUN_ID  = dbutils.widgets.get("run_id").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Read Job Metadata

# COMMAND ----------

import json
from pyspark.sql import functions as F
from datetime import datetime

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"

job_df = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{JOB_ID}}'")
if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": f"Job {{JOB_ID}} not found"}}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
FULL_TABLE   = job["full_table"]
LOAD_TYPE    = job.get("load_type", "full") or "full"

target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
BRONZE_CATALOG = target_config.get("bronze_catalog", "")
SILVER_CATALOG = target_config.get("silver_catalog", "")
TGT_SCHEMA     = target_config.get("target_schema", "")
MULTI_CATALOG  = bool(BRONZE_CATALOG and SILVER_CATALOG and TGT_SCHEMA)

if MULTI_CATALOG:
    bronze_table = f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`.`{{TABLE_NAME}}`"
    silver_table = f"`{{SILVER_CATALOG}}`.`{{TGT_SCHEMA}}`.`{{TABLE_NAME}}`"
    DQ_CATALOG   = SILVER_CATALOG
    DQ_SCHEMA    = TGT_SCHEMA
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{SILVER_CATALOG}}`.`{{TGT_SCHEMA}}`")
    print(f"📦 Multi-catalog mode: {{BRONZE_CATALOG}}.{{TGT_SCHEMA}} → {{SILVER_CATALOG}}.{{TGT_SCHEMA}}")
else:
    TARGET_CATALOG = target_config.get("catalog", CATALOG)
    TARGET_SCHEMA  = target_config.get("schema", SCHEMA)
    bronze_table = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.`bronze_{{TABLE_NAME}}`"
    silver_table = f"`{{TARGET_CATALOG}}`.`{{TARGET_SCHEMA}}`.`silver_{{TABLE_NAME}}`"
    DQ_CATALOG   = TARGET_CATALOG
    DQ_SCHEMA    = TARGET_SCHEMA

print(f"📋 Job: {{job['job_name']}}")
print(f"📋 Bronze: {{bronze_table}}")
print(f"📋 Silver: {{silver_table}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Read from Bronze

# COMMAND ----------

try:
    df = spark.read.table(bronze_table)
    initial_count = df.count()
    print(f"📊 Bronze rows: {{initial_count:,}}")
except Exception as e:
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{str(e).replace("'","''")[:500]}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Data Quality Checks

# COMMAND ----------

# Create restore point
restore_version = None
try:
    history = spark.sql(f"DESCRIBE HISTORY {{silver_table}} LIMIT 1").collect()
    if history:
        restore_version = history[0]["version"]
        print(f"📌 Restore point: v{{restore_version}}")
except Exception:
    print("📌 No existing silver table — first load")

# ── DQ-01: Filter quarantined rows ──────────────────────────────────
quarantined_count = df.filter(F.col("__is_quarantined") == True).count()
df_clean = df.filter(F.col("__is_quarantined") == False)
print(f"🚮 DQ-01 Quarantine filter: {{quarantined_count}} quarantined rows excluded")

# ── DQ-02: Remove all-null rows ─────────────────────────────────────
audit_cols = [c for c in df_clean.columns if c.startswith("__")]
data_cols  = [c for c in df_clean.columns if c not in audit_cols]

if data_cols:
    null_check = [F.col(c).isNull() for c in data_cols]
    all_null   = null_check[0]
    for nc in null_check[1:]:
        all_null = all_null & nc
    rejected_nulls = df_clean.filter(all_null).count()
    df_clean = df_clean.filter(~all_null)
    print(f"🚮 DQ-02 Null-key removal: {{rejected_nulls}} all-null rows dropped")
else:
    rejected_nulls = 0

# ── DQ-03: Per-column null percentage check ─────────────────────────
high_null_cols = []
total_for_null = df_clean.count()
if total_for_null > 0 and data_cols:
    null_counts = df_clean.select(
        *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in data_cols]
    ).collect()[0].asDict()
    for col_name, cnt in null_counts.items():
        pct = (cnt / total_for_null) * 100 if cnt else 0
        if pct > 80:
            high_null_cols.append(f"{{col_name}}({{pct:.0f}}%)")
    if high_null_cols:
        print(f"⚠️ DQ-03 High null columns (>80%): {{', '.join(high_null_cols)}}")
    else:
        print(f"✅ DQ-03 No columns exceed 80% null threshold")

# ── DQ-04: Deduplication ────────────────────────────────────────────
before_dedup = df_clean.count()
df_clean = df_clean.dropDuplicates(data_cols) if data_cols else df_clean
after_dedup = df_clean.count()
dupes_removed = before_dedup - after_dedup
print(f"🔄 DQ-04 Deduplication: {{dupes_removed}} duplicates removed")

# ── DQ-05: Trim string columns (whitespace normalization) ──────────
string_cols = [f.name for f in df_clean.schema.fields if str(f.dataType) == "StringType"]
for sc in string_cols:
    df_clean = df_clean.withColumn(sc, F.trim(F.col(sc)))
print(f"✅ DQ-05 String trimming: {{len(string_cols)}} columns normalized")

# ── DQ-06: Empty string → NULL normalization ────────────────────────
for sc in string_cols:
    df_clean = df_clean.withColumn(sc, F.when(F.col(sc) == "", None).otherwise(F.col(sc)))
print(f"✅ DQ-06 Empty-to-NULL: {{len(string_cols)}} string columns normalized")

# ── DQ-07: Row count anomaly detection ──────────────────────────────
row_anomaly = False
try:
    prev = spark.sql(f"SELECT MAX(output_rows) AS prev_rows FROM `{{DQ_CATALOG}}`.`{{DQ_SCHEMA}}`.__dq_metrics WHERE table_name = '{{FULL_TABLE}}' AND layer = 'silver'").collect()[0]["prev_rows"]
    if prev and prev > 0:
        pct_change = abs(after_dedup - prev) / prev * 100
        if pct_change > 50:
            row_anomaly = True
            print(f"⚠️ DQ-07 Row count anomaly: {{pct_change:.0f}}% change vs previous ({{prev:,}} → {{after_dedup:,}})")
        else:
            print(f"✅ DQ-07 Row count change: {{pct_change:.1f}}% (within threshold)")
except Exception:
    print("ℹ️ DQ-07 No previous run — skipping anomaly detection")

# ── Compute DQ status per row ───────────────────────────────────────
if data_cols:
    _any_null = F.col(data_cols[0]).isNull()
    for dc in data_cols[1:]:
        _any_null = _any_null | F.col(dc).isNull()
    dq_status_expr = F.when(_any_null, F.lit("warn")).otherwise(F.lit("passed"))
else:
    dq_status_expr = F.lit("passed")

# Add silver audit columns
df_silver = (df_clean
    .withColumn("__silver_ts", F.current_timestamp())
    .withColumn("__silver_version", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    .withColumn("__dq_status", dq_status_expr)
    .withColumn("__job_id", F.lit(JOB_ID))
    .withColumn("__run_id", F.lit(RUN_ID))
)

final_count = df_silver.count()
warn_count  = df_silver.filter(F.col("__dq_status") == "warn").count()
total_rejected = initial_count - final_count

# DQ score calculation
checks_passed = sum([1 for c in [
    quarantined_count == 0,
    rejected_nulls == 0,
    len(high_null_cols) == 0,
    dupes_removed == 0,
    True,   # trim always passes
    True,   # empty-to-null always passes
    not row_anomaly,
] if c])
checks_total = 7
dq_score = round(checks_passed / checks_total * 100, 1)

print(f"\\n📊 Silver DQ Summary:")
print(f"   Input:          {{initial_count:,}}")
print(f"   Output:         {{final_count:,}}")
print(f"   Rejected:       {{total_rejected:,}} (quarantined={{quarantined_count}}, nulls={{rejected_nulls}}, dupes={{dupes_removed}})")
print(f"   Warn rows:      {{warn_count:,}} (partial nulls)")
print(f"   High-null cols: {{len(high_null_cols)}}")
print(f"   Row anomaly:    {{'Yes' if row_anomaly else 'No'}}")
print(f"   DQ Score:       {{dq_score}}% ({{checks_passed}}/{{checks_total}} checks passed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Write to Silver Delta

# COMMAND ----------

try:
    if LOAD_TYPE == "full":
        (df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table))
        print(f"✅ Full load → {{silver_table}} ({{final_count:,}} rows)")
    else:
        (df_silver.write
            .format("delta")
            .mode("append")
            .saveAsTable(silver_table))
        print(f"✅ Append → {{silver_table}} ({{final_count:,}} rows)")
except Exception as e:
    if restore_version is not None:
        try:
            spark.sql(f"RESTORE TABLE {{silver_table}} TO VERSION AS OF {{restore_version}}")
            print(f"🔄 Restored to v{{restore_version}}")
        except Exception:
            pass
    try:
        spark.sql(f"""
            MERGE INTO {{run_tbl}} AS t
            USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET t.status = 'failed',
                t.error_message = '{{str(e).replace("'","''")[:500]}}',
                t.completed_at = current_timestamp()
        """)
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Update Metadata

# COMMAND ----------

# Save DQ metrics
try:
    dq_table = f"`{{DQ_CATALOG}}`.`{{DQ_SCHEMA}}`.__dq_metrics"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {{dq_table}} (
            run_id STRING, job_id STRING, table_name STRING, layer STRING,
            input_rows BIGINT, output_rows BIGINT, rejected_rows BIGINT,
            null_rows BIGINT, dupe_rows BIGINT, quarantined_rows BIGINT,
            schema_drift BOOLEAN, dq_checks_passed INT, dq_checks_total INT,
            dq_score DOUBLE, checked_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {{dq_table}} VALUES (
            '{{RUN_ID}}', '{{JOB_ID}}', '{{FULL_TABLE}}', 'silver',
            {{initial_count}}, {{final_count}}, {{total_rejected}},
            {{rejected_nulls}}, {{dupes_removed}}, {{quarantined_count}},
            false, {{checks_passed}}, {{checks_total}},
            {{dq_score}}, current_timestamp()
        )
    """)
except Exception as e:
    print(f"⚠️ DQ metrics save failed: {{e}}")

# Update run history
try:
    spark.sql(f"""
        MERGE INTO {{run_tbl}} AS t
        USING (SELECT '{{RUN_ID}}' AS run_id) AS s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            t.status = 'success',
            t.rows_processed = {{final_count}},
            t.completed_at = current_timestamp(),
            t.duration_sec = unix_timestamp(current_timestamp()) - unix_timestamp(t.started_at)
    """)
except Exception as e:
    print(f"⚠️ Run history update failed: {{e}}")

# Update job metadata
try:
    spark.sql(f"""
        UPDATE {{job_tbl}}
        SET last_run_id = '{{RUN_ID}}', last_run_at = current_timestamp(),
            last_status = 'success', status = 'success',
            run_count = run_count + 1, updated_at = current_timestamp()
        WHERE job_id = '{{JOB_ID}}'
    """)
except Exception as e:
    print(f"⚠️ Job update failed: {{e}}")

# COMMAND ----------

exit_payload = json.dumps({{
    "status": "COMPLETED", "job_id": JOB_ID, "run_id": RUN_ID,
    "table": FULL_TABLE, "rows": final_count,
    "rejected": total_rejected, "silver_table": silver_table,
}})
print(f"\\n✅ SILVER COMPLETE — {{FULL_TABLE}} — {{final_count:,}} rows ({{total_rejected}} rejected)")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4. METADATA-DRIVEN ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_orchestrator(catalog, schema, landing_path, workspace_path, ts, recon_catalog="reconciliation", recon_schema="hr", recon_table="ReconcilationDetails", log_catalog="logging", log_schema="hr", log_table="ExecutionLog"):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Metadata-Driven Orchestrator
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads pipeline metadata from Delta tables and chains:
# MAGIC   Extract → Bronze → Reconciliation → Silver for each pipeline group.
# MAGIC   Then logs all execution details to the Logging catalog.
# MAGIC
# MAGIC Can run a **single pipeline group** or **all groups**.
# MAGIC ---

# COMMAND ----------

dbutils.widgets.text("group_id", "", "Pipeline Group ID (blank = run all)")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental, blank = use metadata)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("workspace_path", "{workspace_path}", "Notebook Workspace Path")
dbutils.widgets.text("recon_catalog", "{recon_catalog}", "Reconciliation Catalog")
dbutils.widgets.text("recon_schema", "{recon_schema}", "Reconciliation Schema")
dbutils.widgets.text("recon_table", "{recon_table}", "Reconciliation Table")
dbutils.widgets.text("log_catalog", "{log_catalog}", "Logging Catalog")
dbutils.widgets.text("log_schema", "{log_schema}", "Logging Schema")
dbutils.widgets.text("log_table", "{log_table}", "Logging Table")

GROUP_ID       = dbutils.widgets.get("group_id").strip()
LOAD_OVERRIDE  = dbutils.widgets.get("load_type").strip()
PASSWORD_B64   = dbutils.widgets.get("password_b64").strip()
CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
LANDING_PATH   = dbutils.widgets.get("landing_path").strip()
WORKSPACE_PATH = dbutils.widgets.get("workspace_path").strip()
RECON_CATALOG  = dbutils.widgets.get("recon_catalog").strip()
RECON_SCHEMA   = dbutils.widgets.get("recon_schema").strip()
RECON_TABLE    = dbutils.widgets.get("recon_table").strip()
LOG_CATALOG    = dbutils.widgets.get("log_catalog").strip()
LOG_SCHEMA     = dbutils.widgets.get("log_schema").strip()
LOG_TABLE      = dbutils.widgets.get("log_table").strip()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

import json, uuid
from datetime import datetime

job_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"
pipe_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_pipeline_metadata"

# Get pipeline groups
if GROUP_ID:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}} WHERE group_id = '{{GROUP_ID}}'")
else:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}}")

groups = [r.asDict() for r in groups_df.collect()]
print(f"📋 Pipeline groups to run: {{len(groups)}}")
for g in groups:
    print(f"   • {{g['full_table']}} ({{g.get('load_type','full')}})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Pipelines

# COMMAND ----------

stage_notebook = {{
    "extract":           f"{{WORKSPACE_PATH}}/01_Meta_Extract",
    "landing_to_bronze": f"{{WORKSPACE_PATH}}/02_Meta_Bronze",
    "bronze_to_silver":  f"{{WORKSPACE_PATH}}/03_Meta_Silver",
}}

results = []

for group in groups:
    gid = group["group_id"]
    print(f"\\n{{'='*60}}")
    print(f"🔗 Pipeline: {{group['full_table']}}")
    print(f"{{'='*60}}")

    # Get jobs for this group, ordered by job_order
    jobs_df = spark.sql(f"""
        SELECT * FROM {{job_tbl}}
        WHERE group_id = '{{gid}}' AND (enabled = true OR enabled IS NULL)
        ORDER BY job_order ASC
    """)
    jobs = [r.asDict() for r in jobs_df.collect()]

    group_ok = True
    for job in jobs:
        job_id   = job["job_id"]
        stage    = job["stage"]
        nb_path  = stage_notebook.get(stage)
        if not nb_path:
            print(f"   ⚠️ Unknown stage '{{stage}}' — skipping")
            continue

        run_id = uuid.uuid4().hex[:12]
        load_type = LOAD_OVERRIDE if LOAD_OVERRIDE else (job.get("load_type") or "full")

        # Create run record in metadata
        try:
            spark.sql(f"""
                INSERT INTO {{run_tbl}} (run_id, job_id, job_name, stage, full_table,
                    load_type, watermark_column, status, started_at)
                VALUES ('{{run_id}}', '{{job_id}}', '{{job["job_name"]}}', '{{stage}}',
                    '{{job["full_table"]}}', '{{load_type}}', '{{job.get("watermark_column","")}}',
                    'running', current_timestamp())
            """)
        except Exception as e:
            print(f"   ⚠️ Could not create run record: {{e}}")

        print(f"\\n   ▶ Running: {{job['job_name']}} ({{stage}})")

        try:
            result_json = dbutils.notebook.run(
                nb_path,
                timeout_seconds=3600,
                arguments={{
                    "job_id":       job_id,
                    "run_id":       run_id,
                    "load_type":    load_type,
                    "password_b64": PASSWORD_B64,
                    "catalog":      CATALOG,
                    "schema":       SCHEMA,
                    "landing_path": LANDING_PATH,
                }}
            )
            result = json.loads(result_json) if result_json else {{}}
            status = result.get("status", "UNKNOWN")
            rows   = result.get("rows", 0)
            error  = result.get("error", "")

            if status in ("FAILED", "ERROR"):
                print(f"   ❌ {{job['job_name']}}: {{status}} — {{error}}")
                results.append({{"job": job["job_name"], "status": "FAILED", "rows": rows, "error": error}})
                group_ok = False
                print(f"   ⛔ Stopping pipeline for {{group['full_table']}} due to failure")
                break
            else:
                print(f"   ✅ {{job['job_name']}}: {{status}} ({{rows:,}} rows)")
                results.append({{"job": job["job_name"], "status": status, "rows": rows}})

                # ── Reconciliation after Bronze ──────────────────────────
                if stage == "landing_to_bronze":
                    print(f"\\n   🔍 Running Reconciliation for {{job['job_name']}}…")
                    try:
                        recon_json = dbutils.notebook.run(
                            f"{{WORKSPACE_PATH}}/04_Meta_Reconciliation",
                            timeout_seconds=1800,
                            arguments={{
                                "job_id":        job_id,
                                "run_id":        run_id,
                                "password_b64":  PASSWORD_B64,
                                "catalog":       CATALOG,
                                "schema":        SCHEMA,
                                "landing_path":  LANDING_PATH,
                                "recon_catalog": RECON_CATALOG,
                                "recon_schema":  RECON_SCHEMA,
                                "recon_table":   RECON_TABLE,
                            }}
                        )
                        recon_result = json.loads(recon_json) if recon_json else {{}}
                        r_status = recon_result.get("status", "UNKNOWN")
                        r_checks = recon_result.get("checks", 0)
                        r_passed = recon_result.get("passed", 0)
                        r_failed = recon_result.get("failed", 0)
                        print(f"   🔍 Reconciliation: {{r_status}} — {{r_checks}} checks ({{r_passed}} pass, {{r_failed}} fail)")
                        results.append({{"job": f"Recon_{{job['job_name']}}", "status": r_status, "rows": r_checks}})
                    except Exception as re:
                        print(f"   ⚠️ Reconciliation failed (non-blocking): {{re}}")
                        results.append({{"job": f"Recon_{{job['job_name']}}", "status": "WARN", "rows": 0, "error": str(re)[:200]}})

        except Exception as e:
            print(f"   ❌ {{job['job_name']}} FAILED: {{e}}")
            # Mark failure in run history
            try:
                spark.sql(f"""
                    MERGE INTO {{run_tbl}} AS t
                    USING (SELECT '{{run_id}}' AS run_id) AS s ON t.run_id = s.run_id
                    WHEN MATCHED THEN UPDATE SET
                        t.status = 'failed',
                        t.error_message = '{{str(e).replace("'","''")}}'
                        , t.completed_at = current_timestamp()
                """)
                spark.sql(f"""
                    UPDATE {{job_tbl}}
                    SET last_status = 'failed', status = 'failed',
                        fail_count = fail_count + 1, updated_at = current_timestamp()
                    WHERE job_id = '{{job_id}}'
                """)
            except Exception:
                pass
            results.append({{"job": job["job_name"], "status": "FAILED", "error": str(e)}})
            group_ok = False
            print(f"   ⛔ Stopping pipeline for {{group['full_table']}} due to failure")
            break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Orchestration Summary

# COMMAND ----------

succeeded = [r for r in results if r.get("status") in ("COMPLETED", "SUCCESS") and not r.get("job","").startswith("Recon_")]
failed    = [r for r in results if r.get("status") == "FAILED" and not r.get("job","").startswith("Recon_")]
recon_results = [r for r in results if r.get("job","").startswith("Recon_")]
total_rows = sum(r.get("rows", 0) for r in results if not r.get("job","").startswith("Recon_"))

print(f"\\n{{'='*60}}")
print(f"📊 ORCHESTRATION COMPLETE")
print(f"{{'='*60}}")
print(f"  ✅ Succeeded : {{len(succeeded)}} / {{len(succeeded) + len(failed)}}")
print(f"  ❌ Failed    : {{len(failed)}} / {{len(succeeded) + len(failed)}}")
print(f"  📊 Total Rows: {{total_rows:,}}")
if recon_results:
    r_ok = sum(1 for r in recon_results if r.get("status") in ("COMPLETED",))
    r_warn = sum(1 for r in recon_results if r.get("status") not in ("COMPLETED",))
    print(f"  🔍 Recon     : {{r_ok}} pass, {{r_warn}} warn/fail")

if failed:
    print(f"\\n⚠️ Failed jobs:")
    for f_item in failed:
        print(f"   • {{f_item['job']}}: {{f_item.get('error','unknown')}}")

# Build error detail list for visibility
error_details = []
for f_item in failed:
    error_details.append(f"{{f_item['job']}}: {{f_item.get('error','unknown')[:200]}}")

exit_payload = json.dumps({{
    "status":     "COMPLETED" if not failed else "PARTIAL",
    "succeeded":  len(succeeded),
    "failed":     len(failed),
    "total_rows": total_rows,
    "groups":     len(groups),
    "errors":     error_details,
}})

# ── Execution Logging ──────────────────────────────────────────────
print(f"\\n📝 Saving execution log to {{LOG_CATALOG}}.{{LOG_SCHEMA}}.{{LOG_TABLE}}…")
try:
    log_json = dbutils.notebook.run(
        f"{{WORKSPACE_PATH}}/05_Meta_ExecutionLog",
        timeout_seconds=600,
        arguments={{
            "catalog":      CATALOG,
            "schema":       SCHEMA,
            "log_catalog":  LOG_CATALOG,
            "log_schema":   LOG_SCHEMA,
            "log_table":    LOG_TABLE,
            "results_json": json.dumps(results),
            "groups_json":  json.dumps([{{"group_id": g["group_id"], "full_table": g["full_table"], "load_type": g.get("load_type","full")}} for g in groups]),
            "orchestrator_status": "COMPLETED" if not failed else "PARTIAL",
        }}
    )
    print(f"   ✅ Execution log saved")
except Exception as log_err:
    print(f"   ⚠️ Execution logging failed (non-blocking): {{log_err}}")

dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  4b. AGGREGATE RECONCILIATION NOTEBOOK  (Source vs Bronze)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_reconciliation(catalog, schema, landing_path, recon_catalog, recon_schema, recon_table, ts, recon_location=""):
    _loc_clause = f" MANAGED LOCATION '{recon_location}'" if recon_location else ""
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Aggregate Reconciliation — Source vs Bronze
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC This notebook performs aggregate reconciliation between the **source database**
# MAGIC and the **Bronze Delta table** for the current pipeline execution.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Identifies all numeric columns (int, bigint, float, decimal, numeric, smallint, tinyint, real, money)
# MAGIC 2. Computes SUM for each numeric column from **Source** (via JDBC) and **Bronze** (Delta)
# MAGIC 3. Compares row counts
# MAGIC 4. Saves per-column results to `{recon_catalog}.{recon_schema}.{recon_table}`
# MAGIC 5. Each execution creates a unique `recon_run_id` — no duplicates, full audit trail
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("recon_catalog", "{recon_catalog}", "Reconciliation Catalog")
dbutils.widgets.text("recon_schema", "{recon_schema}", "Reconciliation Schema")
dbutils.widgets.text("recon_table", "{recon_table}", "Reconciliation Table")

import base64, json, uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

JOB_ID       = dbutils.widgets.get("job_id").strip()
RUN_ID       = dbutils.widgets.get("run_id").strip()
_PWD_B64     = dbutils.widgets.get("password_b64").strip()
PASSWORD     = base64.b64decode(_PWD_B64.encode("ascii")).decode("utf-8") if _PWD_B64 else ""
CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LANDING_PATH = dbutils.widgets.get("landing_path").strip()
RECON_CATALOG= dbutils.widgets.get("recon_catalog").strip()
RECON_SCHEMA = dbutils.widgets.get("recon_schema").strip()
RECON_TABLE  = dbutils.widgets.get("recon_table").strip()

RECON_RUN_ID = uuid.uuid4().hex[:12]

print(f"🔍 Reconciliation for Job: {{JOB_ID}}, Run: {{RUN_ID}}")
print(f"📦 Results → {{RECON_CATALOG}}.{{RECON_SCHEMA}}.{{RECON_TABLE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## � Ensure Reconciliation Table Exists

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{RECON_CATALOG}}`{_loc_clause}")
except Exception as cat_err:
    print(f"⚠️ Could not create catalog {{RECON_CATALOG}}: {{cat_err}} — assuming it exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{RECON_CATALOG}}`.`{{RECON_SCHEMA}}`")

recon_full_table = f"`{{RECON_CATALOG}}`.`{{RECON_SCHEMA}}`.`{{RECON_TABLE}}`"

recon_schema_def = StructType([
    StructField("recon_run_id",    StringType(),    False),
    StructField("pipeline_run_id", StringType(),    False),
    StructField("job_id",          StringType(),    False),
    StructField("source_table",    StringType(),    False),
    StructField("bronze_table",    StringType(),    False),
    StructField("column_name",     StringType(),    False),
    StructField("data_type",       StringType(),    True),
    StructField("source_value",    DoubleType(),    True),
    StructField("bronze_value",    DoubleType(),    True),
    StructField("variance",        DoubleType(),    True),
    StructField("variance_pct",    DoubleType(),    True),
    StructField("status",          StringType(),    True),
    StructField("recon_timestamp", TimestampType(), True),
])

try:
    spark.table(recon_full_table)
    print(f"📦 Table {{recon_full_table}} exists")
except Exception:
    empty_df = spark.createDataFrame([], schema=recon_schema_def)
    empty_df.write.format("delta").saveAsTable(recon_full_table)
    print(f"📦 Created table {{recon_full_table}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## �🔍 Read Job Metadata

# COMMAND ----------

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
job_df  = spark.sql(f"SELECT * FROM {{job_tbl}} WHERE job_id = '{{JOB_ID}}'")

if job_df.count() == 0:
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": f"Job {{JOB_ID}} not found"}}))

job = job_df.collect()[0].asDict()
TABLE_NAME   = job["table_name"]
TABLE_SCHEMA = job["table_schema"]
FULL_TABLE   = job["full_table"]

source_config = json.loads(job.get("source_config", "{{}}") or "{{}}")
SERVER   = source_config.get("server", "")
DATABASE = source_config.get("database", "")
USERNAME = source_config.get("username", "")

target_config = json.loads(job.get("target_config", "{{}}") or "{{}}")
BRONZE_CATALOG = target_config.get("bronze_catalog", "")
TGT_SCHEMA     = target_config.get("target_schema", "")
VOLUMES_CATALOG= target_config.get("volumes_catalog", "")

# Determine bronze table name (DLT prefixes tables with bronze_)
if BRONZE_CATALOG and TGT_SCHEMA:
    BRONZE_TABLE = f"`{{BRONZE_CATALOG}}`.`{{TGT_SCHEMA}}`.`bronze_{{TABLE_NAME}}`"
else:
    BRONZE_TABLE = f"`{{target_config.get('catalog', CATALOG)}}`.`{{target_config.get('schema', SCHEMA)}}`.`bronze_{{TABLE_NAME}}`"

print(f"📋 Source: [{{TABLE_SCHEMA}}].[{{TABLE_NAME}}] on {{SERVER}}/{{DATABASE}}")
print(f"📋 Bronze: {{BRONZE_TABLE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 JDBC Connection to Source

# COMMAND ----------

encrypt = "true" if source_config.get("source_type") in ("azuresql", "synapse") else "false"
trust   = "false" if source_config.get("source_type") in ("azuresql", "synapse") else "true"

if "," in SERVER:
    _host, _port = SERVER.rsplit(",", 1)
elif ":" in SERVER:
    _host, _port = SERVER.rsplit(":", 1)
else:
    _host, _port = SERVER, "1433"

jdbc_url = f"jdbc:sqlserver://{{_host}}:{{_port}};databaseName={{DATABASE}};encrypt={{encrypt}};trustServerCertificate={{trust}}"
jdbc_props = {{
    "user":     USERNAME,
    "password": PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000",
}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Identify Numeric Columns from Source

# COMMAND ----------

# Query SQL Server INFORMATION_SCHEMA to find numeric columns
numeric_types_sql = "('int','bigint','smallint','tinyint','float','real','decimal','numeric','money','smallmoney')"
col_query = f"""(
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{{TABLE_SCHEMA}}'
      AND TABLE_NAME   = '{{TABLE_NAME}}'
      AND DATA_TYPE IN {{numeric_types_sql}}
) AS col_info"""

try:
    cols_df = spark.read.jdbc(jdbc_url, col_query, properties=jdbc_props)
    numeric_cols = [(r["COLUMN_NAME"], r["DATA_TYPE"]) for r in cols_df.collect()]
    print(f"🔢 Found {{len(numeric_cols)}} numeric columns:")
    for cn, ct in numeric_cols:
        print(f"   • {{cn}} ({{ct}})")
except Exception as e:
    print(f"❌ Failed to read column metadata: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

if not numeric_cols:
    print("⚠️ No numeric columns found — reconciliation skipped")
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": "No numeric columns"}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Compute Source Aggregates (JDBC)

# COMMAND ----------

# Build a single SQL query that computes COUNT(*) plus SUM of each numeric column
agg_exprs = ["COUNT(*) AS __row_count"]
for cn, _ in numeric_cols:
    safe_col = cn.replace("'", "''")
    agg_exprs.append(f"SUM(CAST([{{cn}}] AS FLOAT)) AS [sum_{{cn}}]")

agg_sql = ", ".join(agg_exprs)
src_query = f"(SELECT {{agg_sql}} FROM [{{TABLE_SCHEMA}}].[{{TABLE_NAME}}]) AS src_agg"

try:
    src_agg_df = spark.read.jdbc(jdbc_url, src_query, properties=jdbc_props)
    src_row = src_agg_df.collect()[0]
    src_count = int(src_row["__row_count"])
    print(f"📊 Source row count: {{src_count:,}}")
except Exception as e:
    print(f"❌ Failed to compute source aggregates: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Compute Bronze Aggregates (Delta)

# COMMAND ----------

try:
    brz_df = spark.table(BRONZE_TABLE)
    brz_count = brz_df.count()
    print(f"📊 Bronze row count: {{brz_count:,}}")

    # Compute SUM of each numeric column in Bronze
    brz_agg_exprs = [F.count("*").alias("__row_count")]
    for cn, _ in numeric_cols:
        brz_agg_exprs.append(F.sum(F.col(f"`{{cn}}`").cast("double")).alias(f"sum_{{cn}}"))

    brz_agg_df = brz_df.agg(*brz_agg_exprs)
    brz_row = brz_agg_df.collect()[0]
except Exception as e:
    print(f"❌ Failed to compute Bronze aggregates: {{e}}")
    dbutils.notebook.exit(json.dumps({{"status": "FAILED", "error": str(e)[:500]}}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Compare & Build Reconciliation Results

# COMMAND ----------

recon_ts = datetime.now()
results = []

# Row count reconciliation
count_match = "PASS" if src_count == brz_count else "FAIL"
count_variance = abs(src_count - brz_count)
results.append({{
    "recon_run_id":    RECON_RUN_ID,
    "pipeline_run_id": RUN_ID,
    "job_id":          JOB_ID,
    "source_table":    FULL_TABLE,
    "bronze_table":    BRONZE_TABLE,
    "column_name":     "__ROW_COUNT__",
    "data_type":       "count",
    "source_value":    float(src_count),
    "bronze_value":    float(brz_count),
    "variance":        float(count_variance),
    "variance_pct":    round((count_variance / src_count * 100), 4) if src_count > 0 else 0.0,
    "status":          count_match,
    "recon_timestamp": recon_ts,
}})

# Per-column SUM reconciliation
for cn, ct in numeric_cols:
    src_val = src_row[f"sum_{{cn}}"]
    brz_val = brz_row[f"sum_{{cn}}"]
    s = float(src_val) if src_val is not None else 0.0
    b = float(brz_val) if brz_val is not None else 0.0
    var = abs(s - b)
    pct = round((var / abs(s) * 100), 4) if s != 0.0 else 0.0
    status = "PASS" if var < 0.01 else ("WARN" if pct < 0.01 else "FAIL")

    results.append({{
        "recon_run_id":    RECON_RUN_ID,
        "pipeline_run_id": RUN_ID,
        "job_id":          JOB_ID,
        "source_table":    FULL_TABLE,
        "bronze_table":    BRONZE_TABLE,
        "column_name":     cn,
        "data_type":       ct,
        "source_value":    s,
        "bronze_value":    b,
        "variance":        var,
        "variance_pct":    pct,
        "status":          status,
        "recon_timestamp": recon_ts,
    }})

print(f"\\n📊 Reconciliation results: {{len(results)}} checks")
for r in results:
    icon = "✅" if r["status"] == "PASS" else ("⚠️" if r["status"] == "WARN" else "❌")
    print(f"   {{icon}} {{r['column_name']:<30}} src={{r['source_value']:>15,.2f}}  brz={{r['bronze_value']:>15,.2f}}  var={{r['variance_pct']:.4f}}%  {{r['status']}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Reconciliation Table

# COMMAND ----------

# Ensure reconciliation catalog, schema, and table exist (already done early — safe to repeat)
recon_df = spark.createDataFrame(results, schema=recon_schema_def)

# Append — each execution creates new rows with unique recon_run_id
recon_df.write.mode("append").option("mergeSchema", "true").saveAsTable(recon_full_table)

total_checks = len(results)
passed  = sum(1 for r in results if r["status"] == "PASS")
warned  = sum(1 for r in results if r["status"] == "WARN")
failed_ = sum(1 for r in results if r["status"] == "FAIL")

print(f"\\n💾 Saved {{total_checks}} reconciliation records to {{recon_full_table}}")
print(f"   ✅ PASS: {{passed}}  ⚠️ WARN: {{warned}}  ❌ FAIL: {{failed_}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

exit_payload = json.dumps({{
    "status":       "COMPLETED",
    "recon_run_id": RECON_RUN_ID,
    "job_id":       JOB_ID,
    "run_id":       RUN_ID,
    "table":        FULL_TABLE,
    "checks":       total_checks,
    "passed":       passed,
    "warned":       warned,
    "failed":       failed_,
    "recon_table":  recon_full_table,
}})

print(f"\\n✅ RECONCILIATION COMPLETE — {{FULL_TABLE}} — {{total_checks}} checks ({{passed}} pass, {{warned}} warn, {{failed_}} fail)")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EXECUTION LOG NOTEBOOK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _gen_execution_log(catalog, schema, log_catalog, log_schema, log_table, ts, log_location=""):
    """Generate the 05_Meta_ExecutionLog notebook.

    This notebook is called by the Orchestrator AFTER all jobs complete.
    It receives the per-job results JSON and the groups JSON, then writes
    a full audit-trail row per job into the logging Delta table.
    """
    _log_loc_clause = f" MANAGED LOCATION '{log_location}'" if log_location else ""
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Execution Log — Pipeline Run Audit Trail
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC This notebook saves per-job execution details to
# MAGIC `{{log_catalog}}.{{log_schema}}.{{log_table}}` as an append-only audit trail.
# MAGIC
# MAGIC **Logged per job:** job_id, job_name, stage, full_table, load_type,
# MAGIC status, rows_processed, started_at, completed_at, duration_sec, error_message
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("catalog",              "{catalog}",      "Metadata Catalog")
dbutils.widgets.text("schema",               "{schema}",       "Metadata Schema")
dbutils.widgets.text("log_catalog",          "{log_catalog}",  "Log Catalog")
dbutils.widgets.text("log_schema",           "{log_schema}",   "Log Schema")
dbutils.widgets.text("log_table",            "{log_table}",    "Log Table")
dbutils.widgets.text("results_json",         "{{}}", "Results JSON")
dbutils.widgets.text("groups_json",          "[]", "Groups JSON")
dbutils.widgets.text("orchestrator_status",  "",  "Orchestrator Status")

import json, uuid
from datetime import datetime
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, TimestampType)

CATALOG      = dbutils.widgets.get("catalog").strip()
SCHEMA       = dbutils.widgets.get("schema").strip()
LOG_CATALOG  = dbutils.widgets.get("log_catalog").strip()
LOG_SCHEMA   = dbutils.widgets.get("log_schema").strip()
LOG_TABLE    = dbutils.widgets.get("log_table").strip()
RESULTS_JSON = dbutils.widgets.get("results_json").strip()
GROUPS_JSON  = dbutils.widgets.get("groups_json").strip()
ORCH_STATUS  = dbutils.widgets.get("orchestrator_status").strip()

LOG_RUN_ID   = uuid.uuid4().hex[:12]
LOG_TS       = datetime.now()

print(f"📝 Execution Log Run: {{LOG_RUN_ID}}")
print(f"📦 Target: {{LOG_CATALOG}}.{{LOG_SCHEMA}}.{{LOG_TABLE}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Parse Execution Results

# COMMAND ----------

try:
    results_raw = json.loads(RESULTS_JSON)
except Exception:
    results_raw = []

try:
    groups = json.loads(GROUPS_JSON)
except Exception:
    groups = []

# Build group lookup for load_type
# groups can be a list of strings (group IDs) or a list of dicts
group_lookup = {{}}
for g in groups:
    if isinstance(g, dict):
        gid = g.get("group_id", "")
        group_lookup[gid] = {{
            "full_table": g.get("full_table", ""),
            "load_type":  g.get("load_type", "full"),
        }}
    else:
        # g is a plain group_id string
        group_lookup[str(g)] = {{"full_table": "", "load_type": "full"}}

# Normalise results — orchestrator sends a flat list of dicts
if isinstance(results_raw, dict):
    results_list = [results_raw]
elif isinstance(results_raw, list):
    results_list = results_raw
else:
    results_list = []

print(f"📊 Received {{len(results_list)}} job results, {{len(groups)}} groups")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Ensure Logging Table Exists

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, TimestampType)

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{{LOG_CATALOG}}`{_log_loc_clause}")
except Exception as cat_err:
    print(f"⚠️ Could not create catalog {{LOG_CATALOG}}: {{cat_err}} — assuming it exists")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{LOG_CATALOG}}`.`{{LOG_SCHEMA}}`")

log_full_table = f"`{{LOG_CATALOG}}`.`{{LOG_SCHEMA}}`.`{{LOG_TABLE}}`"

log_schema = StructType([
    StructField("log_run_id",          StringType(),    False),
    StructField("group_id",            StringType(),    False),
    StructField("full_table",          StringType(),    False),
    StructField("stage",               StringType(),    False),
    StructField("load_type",           StringType(),    True),
    StructField("status",              StringType(),    True),
    StructField("rows_processed",      LongType(),      True),
    StructField("started_at",          StringType(),    True),
    StructField("completed_at",        StringType(),    True),
    StructField("duration_sec",        DoubleType(),    True),
    StructField("error_message",       StringType(),    True),
    StructField("orchestrator_status", StringType(),    True),
    StructField("log_timestamp",       TimestampType(), True),
])

# Create empty table if it doesn't exist yet
try:
    spark.table(log_full_table)
    print(f"📦 Table {{log_full_table}} exists")
except Exception:
    empty_df = spark.createDataFrame([], schema=log_schema)
    empty_df.write.format("delta").saveAsTable(log_full_table)
    print(f"📦 Created table {{log_full_table}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔨 Build Log Rows

# COMMAND ----------

log_rows = []

# results_list is a flat list of job result dicts from the orchestrator
for entry in results_list:
    job_name   = entry.get("job", "unknown")
    status     = entry.get("status", "UNKNOWN")
    rows       = entry.get("rows", 0)
    error      = entry.get("error", "")

    # Infer stage from job name pattern
    if "Recon_" in job_name:
        stage = "reconciliation"
    elif job_name.startswith("ExtractTo_"):
        stage = "extract"
    elif "_To_bronze_" in job_name or "_To_Bronze_" in job_name:
        stage = "landing_to_bronze"
    elif "_To_silver_" in job_name or "_To_Silver_" in job_name:
        stage = "bronze_to_silver"
    else:
        stage = "unknown"

    # Try to match a group for full_table/load_type
    full_table = job_name
    load_type  = "full"
    for gid, ginfo in group_lookup.items():
        if ginfo.get("full_table", "") and ginfo["full_table"] in job_name:
            full_table = ginfo["full_table"]
            load_type  = ginfo.get("load_type", "full")
            break

    log_rows.append({{
        "log_run_id":          LOG_RUN_ID,
        "group_id":            job_name,
        "full_table":          str(full_table),
        "stage":               str(stage),
        "load_type":           str(load_type),
        "status":              str(status),
        "rows_processed":      int(rows) if rows else 0,
        "started_at":          "",
        "completed_at":        "",
        "duration_sec":        0.0,
        "error_message":       str(error)[:2000] if error else "",
        "orchestrator_status": str(ORCH_STATUS),
        "log_timestamp":       LOG_TS,
    }})

print(f"📝 Built {{len(log_rows)}} log entries")

if not log_rows:
    print("⚠️ No execution data to log")
    dbutils.notebook.exit(json.dumps({{"status": "SKIPPED", "reason": "No execution data"}}))

for lr in log_rows[:5]:
    icon = "✅" if lr["status"] == "SUCCESS" else ("⚠️" if lr["status"] == "SKIPPED" else "❌")
    print(f"   {{icon}} {{lr['full_table']}} / {{lr['stage']}} → {{lr['status']}} ({{lr['rows_processed']:,}} rows, {{lr['duration_sec']:.1f}}s)")
if len(log_rows) > 5:
    print(f"   … and {{len(log_rows) - 5}} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save to Logging Table

# COMMAND ----------

log_df = spark.createDataFrame(log_rows, schema=log_schema)
log_df.write.mode("append").option("mergeSchema", "true").saveAsTable(log_full_table)

total_logged = len(log_rows)
success_count = sum(1 for r in log_rows if r["status"] == "SUCCESS")
failed_count  = sum(1 for r in log_rows if r["status"] == "FAILED")

print(f"\\n💾 Saved {{total_logged}} execution log records to {{log_full_table}}")
print(f"   ✅ SUCCESS: {{success_count}}  ❌ FAILED: {{failed_count}}  📊 OTHER: {{total_logged - success_count - failed_count}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Summary

# COMMAND ----------

exit_payload = json.dumps({{
    "status":       "COMPLETED",
    "log_run_id":   LOG_RUN_ID,
    "total_logged": total_logged,
    "success":      success_count,
    "failed":       failed_count,
    "log_table":    log_full_table,
}})

print(f"\\n✅ EXECUTION LOG COMPLETE — {{total_logged}} entries saved to {{log_full_table}}")
dbutils.notebook.exit(exit_payload)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  5. DLT PIPELINE NOTEBOOK  (Bronze + Silver combined)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_dlt_pipeline(catalog, schema, landing_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Metadata-Driven DLT Pipeline — Bronze & Silver
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Delta Live Tables pipeline that dynamically discovers tables from
# MAGIC `wf_job_metadata` and creates Bronze + Silver layers with:
# MAGIC - **Auto Loader** (`cloudFiles`) for streaming Bronze ingestion
# MAGIC - **Expectations** for data quality enforcement
# MAGIC - Automatic dependency resolution (Silver reads from Bronze)
# MAGIC - Schema evolution & auto-optimize
# MAGIC
# MAGIC **Configuration** (set in DLT pipeline settings):
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `pipeline.catalog` | Unity Catalog for metadata tables |
# MAGIC | `pipeline.schema` | Schema for metadata tables |
# MAGIC | `pipeline.landing_path` | Base landing zone path |
# MAGIC | `pipeline.group_id` | Pipeline group filter (blank = all) |
# MAGIC ---

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
import json

# ─── Pipeline configuration (injected via DLT pipeline settings) ──────
# Note: The DLT pipeline spec's catalog/schema controls where tables are created.
# meta_catalog/meta_schema point to where wf_job_metadata lives (may differ).
META_CATALOG = spark.conf.get("pipeline.meta_catalog", "{catalog}")
META_SCHEMA  = spark.conf.get("pipeline.meta_schema", "{schema}")
LANDING_PATH = spark.conf.get("pipeline.landing_path", "{landing_path}")
GROUP_ID     = spark.conf.get("pipeline.group_id", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Discover Tables from Job Metadata

# COMMAND ----------

job_tbl = f"`{{META_CATALOG}}`.`{{META_SCHEMA}}`.wf_job_metadata"

_gf = f"AND group_id = '{{GROUP_ID}}'" if GROUP_ID else ""

# In DLT mode, jobs are stored with stage='dlt_bronze_silver' (single stage).
# In standard mode, they use 'landing_to_bronze' / 'bronze_to_silver'.
# Query for ALL matching stages so both modes work.
all_dlt_jobs = [r.asDict() for r in spark.sql(f"""
    SELECT DISTINCT table_name, full_table, target_config, load_type
    FROM {{job_tbl}}
    WHERE stage IN ('landing_to_bronze', 'bronze_to_silver', 'dlt_bronze_silver')
      AND (enabled = true OR enabled IS NULL)
      {{_gf}}
""").collect()]

# Both bronze and silver use the same job list
bronze_jobs = all_dlt_jobs
silver_jobs = all_dlt_jobs

print(f"⚡ DLT — Bronze tables: {{len(bronze_jobs)}}, Silver tables: {{len(silver_jobs)}}")

if not bronze_jobs:
    print("⚠️ WARNING: No tables found in wf_job_metadata for DLT processing!")
    print(f"   Checked stages: landing_to_bronze, bronze_to_silver, dlt_bronze_silver")
    print(f"   Metadata table: {{job_tbl}}")
    # Show what stages DO exist
    _existing = [r[0] for r in spark.sql(f"SELECT DISTINCT stage FROM {{job_tbl}}").collect()]
    print(f"   Existing stages in metadata: {{_existing}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Auto Loader + Expectations
# MAGIC
# MAGIC Each source table gets a **streaming table** that ingests new Parquet
# MAGIC files via Auto Loader with schema evolution.

# COMMAND ----------

def _make_bronze(job):
    """Factory: register a DLT streaming table for one Bronze source."""
    tbl   = job["table_name"]
    full  = job["full_table"]
    tcfg  = json.loads(job.get("target_config") or "{{}}")
    v_cat = tcfg.get("volumes_catalog", "")
    t_sch = tcfg.get("target_schema", "")
    src   = f"/Volumes/{{v_cat}}/{{t_sch}}/landing/{{tbl}}" if v_cat and t_sch else f"{{LANDING_PATH}}/{{tbl}}"

    @dlt.table(
        name=f"bronze_{{tbl}}",
        comment=f"Bronze — raw ingestion of {{full}} via Auto Loader",
        table_properties={{
            "quality": "bronze",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact":   "true",
            "pipelines.autoOptimize.managed":   "true",
        }},
    )
    @dlt.expect_or_drop("dq01_valid_landing_ts",   "__landing_ts IS NOT NULL")
    @dlt.expect("dq02_has_source_system",            "__source_system IS NOT NULL")
    @dlt.expect("dq03_has_batch_id",                 "__batch_id IS NOT NULL")
    @dlt.expect("dq04_fresh_data",                   "__landing_ts >= current_timestamp() - INTERVAL 7 DAYS")
    @dlt.expect("dq05_not_all_null",                 "NOT(__landing_ts IS NULL AND __source_system IS NULL AND __batch_id IS NULL)")
    def _inner():
        return (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaLocation", f"{{src}}/_schema")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(src)
                .withColumn("__bronze_ts",      F.current_timestamp())
                .withColumn("__source_table",   F.lit(full))
                .withColumn("__is_quarantined", F.lit(False))
        )

if not bronze_jobs:
    raise ValueError(
        "No tables found in wf_job_metadata for DLT pipeline. "
        "Ensure pipelines are created via MetadataFlow before running DLT."
    )

for _j in bronze_jobs:
    _make_bronze(_j)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Quality Enforcement & Cleansing
# MAGIC
# MAGIC Each Silver table reads from its Bronze counterpart via `dlt.read()`,
# MAGIC applies deduplication, string trimming, and quality expectations.

# COMMAND ----------

def _make_silver(job):
    """Factory: register a DLT materialized view for one Silver table."""
    tbl         = job["table_name"]
    full        = job["full_table"]
    bronze_name = f"bronze_{{tbl}}"

    @dlt.table(
        name=f"silver_{{tbl}}",
        comment=f"Silver — cleansed & validated {{full}}",
        table_properties={{
            "quality": "silver",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact":   "true",
        }},
    )
    @dlt.expect_or_drop("dq01_valid_bronze_ts",    "__bronze_ts IS NOT NULL")
    @dlt.expect("dq02_has_source_table",            "__source_table IS NOT NULL")
    @dlt.expect("dq03_bronze_freshness",            "__bronze_ts >= current_timestamp() - INTERVAL 7 DAYS")
    @dlt.expect("dq04_no_empty_source",             "length(trim(coalesce(__source_table, ''))) > 0")
    def _inner():
        df = dlt.read(bronze_name)

        # Filter quarantined rows before dropping the flag column
        df = df.filter(F.col("__is_quarantined") == False)

        # Trim all string columns (skip audit cols)
        trimmed = df
        for c in df.schema:
            if c.dataType.simpleString() == "string" and not c.name.startswith("__"):
                trimmed = trimmed.withColumn(c.name, F.trim(F.col(c.name)))

        return (
            trimmed
                .drop("__is_quarantined")
                .dropDuplicates()
                .withColumn("__silver_ts",  F.current_timestamp())
                .withColumn("__dq_status",  F.lit("passed"))
        )

for _j in silver_jobs:
    _make_silver(_j)
'''


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  6. DLT ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _gen_orchestrator_dlt(catalog, schema, landing_path, workspace_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Metadata-Driven DLT Orchestrator
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Two-phase execution:
# MAGIC 1. **Extract** — JDBC extraction via `dbutils.notebook.run()` (standard)
# MAGIC 2. **DLT Pipeline** — Bronze + Silver via Delta Live Tables REST API
# MAGIC
# MAGIC The orchestrator auto-creates the DLT pipeline on first run,
# MAGIC then triggers pipeline updates for subsequent runs.
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Widget Configuration

# COMMAND ----------

dbutils.widgets.text("group_id", "", "Pipeline Group ID (blank = run all)")
dbutils.widgets.text("load_type", "", "Load Type Override (full/incremental)")
dbutils.widgets.text("password_b64", "", "Source DB Password (base64)")
dbutils.widgets.text("catalog", "{catalog}", "Metadata Catalog")
dbutils.widgets.text("schema", "{schema}", "Metadata Schema")
dbutils.widgets.text("landing_path", "{landing_path}", "Landing Base Path")
dbutils.widgets.text("workspace_path", "{workspace_path}", "Notebook Workspace Path")

GROUP_ID       = dbutils.widgets.get("group_id").strip()
LOAD_OVERRIDE  = dbutils.widgets.get("load_type").strip()
PASSWORD_B64   = dbutils.widgets.get("password_b64").strip()
CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
LANDING_PATH   = dbutils.widgets.get("landing_path").strip()
WORKSPACE_PATH = dbutils.widgets.get("workspace_path").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 Workspace Context for REST API

# COMMAND ----------

import json, uuid, time, requests
from datetime import datetime

# Obtain host & token from the running notebook context
# Use safe fallbacks that work on both classic clusters and serverless
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Host — try spark conf first (works everywhere), then context API
try:
    HOST = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    try:
        HOST = "https://" + ctx.browserHostName().get()
    except Exception:
        HOST = "https://" + ctx.tags().apply("browserHostName")

# Token — context API with safe .getOrElse fallback
try:
    TOKEN = ctx.apiToken().getOrElse(None)
    if not TOKEN:
        TOKEN = ctx.apiToken().get()
except Exception:
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

assert TOKEN, "❌ Could not obtain API token from notebook context — check cluster permissions"
_hdrs = {{"Authorization": f"Bearer {{TOKEN}}", "Content-Type": "application/json"}}

print(f"🔗 Workspace: {{HOST}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Discover Extract Jobs

# COMMAND ----------

job_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"
run_tbl  = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_run_history"
pipe_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_pipeline_metadata"

if GROUP_ID:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}} WHERE group_id = '{{GROUP_ID}}'")
else:
    groups_df = spark.sql(f"SELECT * FROM {{pipe_tbl}}")

groups = [r.asDict() for r in groups_df.collect()]
print(f"📋 Pipeline groups to run: {{len(groups)}}")

# Collect extract jobs across all selected groups
extract_jobs = []
for g in groups:
    gid = g["group_id"]
    jobs = spark.sql(f"""
        SELECT * FROM {{job_tbl}}
        WHERE group_id = '{{gid}}' AND stage = 'extract'
          AND (enabled = true OR enabled IS NULL)
        ORDER BY job_order ASC
    """).collect()
    extract_jobs.extend([r.asDict() for r in jobs])

print(f"📋 Extract jobs: {{len(extract_jobs)}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Phase 1 — Run Extract Notebooks

# COMMAND ----------

extract_nb = f"{{WORKSPACE_PATH}}/01_Meta_Extract"
extract_results = []

for job in extract_jobs:
    job_id    = job["job_id"]
    run_id    = uuid.uuid4().hex[:12]
    load_type = LOAD_OVERRIDE or job.get("load_type") or "full"

    # Create run record
    try:
        spark.sql(f"""
            INSERT INTO {{run_tbl}} (run_id, job_id, job_name, stage, full_table,
                load_type, watermark_column, status, started_at)
            VALUES ('{{run_id}}', '{{job_id}}', '{{job["job_name"]}}', 'extract',
                '{{job["full_table"]}}', '{{load_type}}',
                '{{job.get("watermark_column","")}}', 'running', current_timestamp())
        """)
    except Exception:
        pass

    print(f"\\n  ▶ Extract: {{job['job_name']}}")
    try:
        result_json = dbutils.notebook.run(extract_nb, 3600, {{
            "job_id": job_id, "run_id": run_id,
            "load_type": load_type, "password_b64": PASSWORD_B64,
            "catalog": CATALOG, "schema": SCHEMA, "landing_path": LANDING_PATH,
        }})
        result = json.loads(result_json) if result_json else {{}}
        status = result.get("status", "UNKNOWN")
        rows   = result.get("rows", 0)
        if status in ("FAILED", "ERROR"):
            print(f"    ❌ {{job['job_name']}}: {{result.get('error','')}}")
            extract_results.append({{"job": job["job_name"], "status": "FAILED", "error": result.get("error",""), "run_id": run_id}})
        else:
            print(f"    ✅ {{job['job_name']}}: {{rows:,}} rows")
            extract_results.append({{"job": job["job_name"], "status": "OK", "rows": rows, "run_id": run_id}})
    except Exception as e:
        print(f"    ❌ {{job['job_name']}}: {{e}}")
        extract_results.append({{"job": job["job_name"], "status": "FAILED", "error": str(e)[:500], "run_id": run_id}})

extract_ok   = len([r for r in extract_results if r["status"] == "OK"])
extract_fail = len([r for r in extract_results if r["status"] == "FAILED"])
if extract_fail:
    print(f"\\n⚠️ {{extract_fail}} extract(s) failed — DLT pipeline will process remaining tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Phase 2 — Create / Update DLT Pipeline

# COMMAND ----------

DLT_NAME = f"MetadataPipeline_{{GROUP_ID}}" if GROUP_ID else "MetadataPipeline_All"
DLT_NB   = f"{{WORKSPACE_PATH}}/02_Meta_DLT_Pipeline"

# ── Determine DLT output catalog/schema ──────────────────────────
# DLT tables (bronze_*, silver_*) should go into the bronze catalog,
# NOT the metadata (admin_source) catalog.
# Read the proper target from wf_job_metadata target_config, or fall back.
try:
    _first_target = spark.sql(f"""
        SELECT target_config FROM `{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata
        WHERE target_config IS NOT NULL AND LENGTH(TRIM(target_config)) > 2
        LIMIT 1
    """).first()
    if _first_target:
        _tcfg = json.loads(_first_target[0] or "{{}}")
        DLT_CATALOG = _tcfg.get("bronze_catalog", "") or CATALOG
        DLT_SCHEMA  = _tcfg.get("target_schema", "") or SCHEMA
    else:
        DLT_CATALOG = CATALOG
        DLT_SCHEMA  = SCHEMA
except Exception:
    DLT_CATALOG = CATALOG
    DLT_SCHEMA  = SCHEMA

# Ensure the DLT output schema exists
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`")
    print(f"✅ Ensured schema exists: {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
except Exception as schema_err:
    print(f"⚠️ Could not create schema {{DLT_CATALOG}}.{{DLT_SCHEMA}}: {{schema_err}}")

print(f"📦 DLT output target: {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
print(f"📋 Metadata source:   {{CATALOG}}.{{SCHEMA}}")

pipeline_cfg = {{
    "pipeline.meta_catalog":  CATALOG,
    "pipeline.meta_schema":   SCHEMA,
    "pipeline.landing_path":  LANDING_PATH,
    "pipeline.group_id":      GROUP_ID,
}}

pipeline_spec = {{
    "name":          DLT_NAME,
    "catalog":       DLT_CATALOG,
    "schema":        DLT_SCHEMA,
    "configuration": pipeline_cfg,
    "libraries":     [{{"notebook": {{"path": DLT_NB}}}}],
    "continuous":    False,
    "development":   True,
    "channel":       "CURRENT",
    "serverless":    True,
}}

# Check for existing pipeline by name
resp = requests.get(
    f"{{HOST}}/api/2.0/pipelines",
    params={{"filter": f"name LIKE '{{DLT_NAME}}'", "max_results": 10}},
    headers=_hdrs,
)
resp.raise_for_status()
existing = [p for p in resp.json().get("statuses", []) if p["name"] == DLT_NAME]

if not existing:
    # Also search for ANY pipeline targeting the same catalog.schema
    # to avoid "table already managed by pipeline X" errors
    all_resp = requests.get(
        f"{{HOST}}/api/2.0/pipelines",
        params={{"max_results": 50}},
        headers=_hdrs,
    )
    if all_resp.ok:
        for p in all_resp.json().get("statuses", []):
            pid = p.get("pipeline_id", "")
            try:
                pd = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pid}}", headers=_hdrs)
                if pd.ok:
                    pspec = pd.json().get("spec", {{}})
                    if pspec.get("catalog") == DLT_CATALOG and pspec.get("schema") == DLT_SCHEMA:
                        print(f"⚠️ Found stale pipeline '{{p.get('name','')}}' ({{pid}}) targeting {{DLT_CATALOG}}.{{DLT_SCHEMA}}")
                        print(f"   Deleting stale pipeline to avoid ownership conflict…")
                        requests.delete(f"{{HOST}}/api/2.0/pipelines/{{pid}}", headers=_hdrs)
                        print(f"   ✅ Deleted stale pipeline {{pid}}")
            except Exception:
                pass

    # Re-check after cleanup
    resp2 = requests.get(
        f"{{HOST}}/api/2.0/pipelines",
        params={{"filter": f"name LIKE '{{DLT_NAME}}'", "max_results": 10}},
        headers=_hdrs,
    )
    if resp2.ok:
        existing = [p for p in resp2.json().get("statuses", []) if p["name"] == DLT_NAME]

if existing:
    pipeline_id = existing[0]["pipeline_id"]
    print(f"📦 Existing DLT pipeline: {{DLT_NAME}} ({{pipeline_id}})")
    # Update pipeline config
    requests.put(
        f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}",
        json=pipeline_spec,
        headers=_hdrs,
    )
else:
    print(f"📦 Creating DLT pipeline: {{DLT_NAME}}")
    cr = requests.post(f"{{HOST}}/api/2.0/pipelines", json=pipeline_spec, headers=_hdrs)
    cr.raise_for_status()
    pipeline_id = cr.json()["pipeline_id"]
    print(f"✅ Created DLT pipeline: {{pipeline_id}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Trigger DLT Pipeline Update

# COMMAND ----------

full_refresh = (LOAD_OVERRIDE or "full").lower() == "full"
print(f"🚀 Triggering DLT update (full_refresh={{full_refresh}})…")

trigger_resp = requests.post(
    f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/updates",
    json={{"full_refresh": full_refresh}},
    headers=_hdrs,
)
trigger_resp.raise_for_status()
update_id = trigger_resp.json().get("update_id", "")
print(f"📋 Update ID: {{update_id}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Poll for DLT Completion

# COMMAND ----------

terminal_states = {{"COMPLETED", "FAILED", "CANCELED"}}
dlt_status  = "WAITING"
poll_count  = 0
MAX_POLLS   = 360   # 360 × 10s = 60 min max
POLL_INTERVAL = 10  # seconds between polls

print(f"🔗 DLT Pipeline URL: {{HOST}}/#joblist/pipelines/{{pipeline_id}}")

while dlt_status not in terminal_states and poll_count < MAX_POLLS:
    time.sleep(POLL_INTERVAL)
    poll_count += 1
    try:
        pr = requests.get(f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}", headers=_hdrs)
        pr.raise_for_status()
        pipe_data = pr.json()
        latest = (pipe_data.get("latest_updates") or [{{}}])[0]
        update_state = latest.get("state", "")
        if update_state in terminal_states:
            dlt_status = update_state
        elif pipe_data.get("state") in terminal_states:
            dlt_status = pipe_data["state"]
        if poll_count % 3 == 0:
            elapsed = poll_count * POLL_INTERVAL
            print(f"  ⏳ DLT status: {{update_state or pipe_data.get('state','UNKNOWN')}} ({{elapsed}}s)")
    except Exception as e:
        print(f"  ⚠️ Poll error: {{e}}")

if poll_count >= MAX_POLLS and dlt_status not in terminal_states:
    dlt_status = "TIMEOUT"
print(f"\\n⚡ DLT pipeline finished: {{dlt_status}}")

# Fetch error details if DLT failed
if dlt_status == "FAILED":
    print("\\n❌ DLT Pipeline FAILED — fetching diagnostics...")

    # 1. Fetch update-level cause (most useful)
    if update_id:
        try:
            upd_resp = requests.get(
                f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/updates/{{update_id}}",
                headers=_hdrs,
            )
            if upd_resp.ok:
                upd = upd_resp.json().get("update", {{}})
                cause = upd.get("cause", "")
                if cause:
                    print(f"\\n📋 Update Cause: {{cause}}")
                # Check for cluster/compute errors
                cluster_id = upd.get("cluster_id", "")
                if cluster_id:
                    print(f"   Cluster: {{cluster_id}}")
        except Exception:
            pass

    # 2. Fetch pipeline events (errors, flow progress)
    try:
        ev_resp = requests.get(
            f"{{HOST}}/api/2.0/pipelines/{{pipeline_id}}/events",
            params={{"max_results": 50, "order_by": "timestamp desc"}},
            headers=_hdrs,
        )
        ev_resp.raise_for_status()
        events = ev_resp.json().get("events", [])

        # Filter for actual error events (not generic update_progress)
        error_events = [
            e for e in events
            if (e.get("level") == "ERROR" and e.get("event_type") != "update_progress")
            or (e.get("event_type") == "flow_progress" and "ERROR" in json.dumps(e.get("details", {{}})))
        ]

        if error_events:
            print("\\n❌ DLT Error Events:")
            for ev in error_events[:10]:
                etype = ev.get("event_type", "")
                msg = ev.get("message", "")
                details = ev.get("details", {{}})
                # Extract nested error messages from details
                if not msg and isinstance(details, dict):
                    msg = details.get("cause", "") or details.get("reason", "") or json.dumps(details)
                print(f"  • [{{etype}}] {{msg[:500]}}")
        else:
            # Fallback: show ALL recent events for debugging
            print("\\n⚠️ No specific error events — showing recent pipeline events:")
            for ev in events[:8]:
                etype = ev.get("event_type", "")
                msg = ev.get("message", "")
                lvl = ev.get("level", "")
                print(f"  • [{{lvl}}/{{etype}}] {{msg[:300]}}")
    except Exception as ev_err:
        print(f"\\n⚠️ Could not fetch DLT events: {{ev_err}}")

    print(f"\\n🔗 Check DLT UI: {{HOST}}/#joblist/pipelines/{{pipeline_id}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## � Phase 3 — Relocate Silver Tables to Silver Catalog

# COMMAND ----------

silver_relocated = 0
silver_failed    = 0

if dlt_status == "COMPLETED":
    # Determine the silver catalog AND schema from target_config
    SILVER_CATALOG = ""
    SILVER_SCHEMA  = ""
    try:
        _tgt_row = spark.sql(f"""
            SELECT target_config FROM `{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata
            WHERE target_config IS NOT NULL AND LENGTH(TRIM(target_config)) > 2
              AND target_config LIKE '%silver_catalog%'
            LIMIT 1
        """).first()
        if _tgt_row:
            _tgt = json.loads(_tgt_row[0] or "{{}}")
            SILVER_CATALOG = _tgt.get("silver_catalog", "")
            SILVER_SCHEMA  = _tgt.get("target_schema", "") or DLT_SCHEMA
    except Exception:
        pass

    if SILVER_CATALOG and SILVER_CATALOG != DLT_CATALOG:
        print(f"🔄 Relocating silver tables: {{DLT_CATALOG}}.{{DLT_SCHEMA}} → {{SILVER_CATALOG}}.{{SILVER_SCHEMA}}")

        # Ensure silver schema exists
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{{SILVER_CATALOG}}`.`{{SILVER_SCHEMA}}`")
        except Exception as se:
            print(f"⚠️ Could not create schema {{SILVER_CATALOG}}.{{SILVER_SCHEMA}}: {{se}}")

        # Find all silver_* tables created by DLT in the bronze catalog
        try:
            silver_tables = [r[1] for r in spark.sql(f"""
                SHOW TABLES IN `{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`
            """).collect() if r[1].startswith("silver_")]
        except Exception:
            silver_tables = []

        for stbl in silver_tables:
            try:
                src_full = f"`{{DLT_CATALOG}}`.`{{DLT_SCHEMA}}`.`{{stbl}}`"
                dst_full = f"`{{SILVER_CATALOG}}`.`{{SILVER_SCHEMA}}`.`{{stbl}}`"
                print(f"  📋 {{src_full}} → {{dst_full}}")

                # DLT creates silver as materialized views — use CTAS instead of DEEP CLONE
                spark.sql(f"CREATE OR REPLACE TABLE {{dst_full}} AS SELECT * FROM {{src_full}}")
                # Drop the materialized view from bronze catalog
                try:
                    spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {{src_full}}")
                except Exception:
                    try:
                        spark.sql(f"DROP VIEW IF EXISTS {{src_full}}")
                    except Exception:
                        spark.sql(f"DROP TABLE IF EXISTS {{src_full}}")
                silver_relocated += 1
                print(f"    ✅ Relocated {{stbl}}")
            except Exception as rel_err:
                silver_failed += 1
                print(f"    ❌ Failed to relocate {{stbl}}: {{rel_err}}")

        print(f"\\n📦 Silver relocation: {{silver_relocated}} ok / {{silver_failed}} failed")
    else:
        if not SILVER_CATALOG:
            print("ℹ️ No silver_catalog in target_config — silver tables remain in DLT catalog")
        else:
            print(f"ℹ️ Silver catalog same as DLT catalog ({{SILVER_CATALOG}}) — no relocation needed")
else:
    print("⏭️ Skipping silver relocation — DLT pipeline did not complete successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Phase 4 — Run Reconciliation

# COMMAND ----------

recon_status = "SKIPPED"
if dlt_status == "COMPLETED" and extract_results:
    try:
        recon_nb = f"{{WORKSPACE_PATH}}/04_Meta_Reconciliation"
        recon_ok_count = 0
        recon_fail_count = 0

        # Run reconciliation for each successfully extracted job
        for job_idx, job in enumerate(extract_jobs):
            if extract_results[job_idx]["status"] != "OK":
                continue
            try:
                jid   = job["job_id"]
                rid   = extract_results[job_idx].get("run_id", "")
                print(f"  📊 Reconciling: {{job['job_name']}}")
                dbutils.notebook.run(recon_nb, 1800, {{
                    "job_id": jid, "run_id": rid,
                    "password_b64": PASSWORD_B64,
                    "catalog": CATALOG, "schema": SCHEMA,
                    "landing_path": LANDING_PATH,
                }})
                recon_ok_count += 1
            except Exception as rj_err:
                recon_fail_count += 1
                print(f"    ⚠️ Recon failed for {{job['job_name']}}: {{rj_err}}")

        recon_status = f"{{recon_ok_count}} ok / {{recon_fail_count}} failed"
        print(f"  ✅ Reconciliation: {{recon_status}}")
    except Exception as recon_err:
        recon_status = "FAILED"
        print(f"  ❌ Reconciliation failed: {{recon_err}}")
else:
    print("⏭️ Skipping reconciliation — DLT pipeline did not complete successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Phase 5 — Run Execution Logging

# COMMAND ----------

log_status = "SKIPPED"
if dlt_status == "COMPLETED":
    try:
        log_nb = f"{{WORKSPACE_PATH}}/05_Meta_ExecutionLog"
        print(f"📝 Running execution logging: {{log_nb}}")

        # Build results JSON for the execution log
        _log_results = json.dumps(extract_results)
        _log_groups  = json.dumps([g.get("group_id","") for g in groups])
        _orch_status = "COMPLETED" if dlt_status == "COMPLETED" and not extract_fail else "PARTIAL"

        log_result = dbutils.notebook.run(log_nb, 1800, {{
            "catalog": CATALOG, "schema": SCHEMA,
            "results_json": _log_results,
            "groups_json": _log_groups,
            "orchestrator_status": _orch_status,
        }})
        log_status = "COMPLETED"
        print(f"  ✅ Execution logging complete")
    except Exception as log_err:
        log_status = "FAILED"
        print(f"  ❌ Execution logging failed: {{log_err}}")
else:
    print("⏭️ Skipping execution logging — DLT pipeline did not complete successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## �📊 Orchestration Summary

# COMMAND ----------

total_rows = sum(r.get("rows", 0) for r in extract_results)

print(f"\\n{{'='*60}}")
print(f"📊 DLT ORCHESTRATION COMPLETE")
print(f"{{'='*60}}")
print(f"  📥 Extracts        : {{extract_ok}} ok / {{extract_fail}} failed")
print(f"  ⚡ DLT Pipeline    : {{dlt_status}}")
print(f"  🔄 Silver Relocated: {{silver_relocated}} ok / {{silver_failed}} failed")
print(f"  📊 Reconciliation  : {{recon_status}}")
print(f"  📝 Execution Log   : {{log_status}}")
print(f"  📊 Rows (JDBC)     : {{total_rows:,}}")
print(f"  🔗 Pipeline ID     : {{pipeline_id}}")

exit_payload = json.dumps({{
    "status":          "COMPLETED" if dlt_status == "COMPLETED" and not extract_fail else "PARTIAL",
    "extract_ok":      extract_ok,
    "extract_failed":  extract_fail,
    "dlt_status":      dlt_status,
    "silver_relocated": silver_relocated,
    "recon_status":    recon_status,
    "log_status":      log_status,
    "pipeline_id":     pipeline_id,
    "total_rows":      total_rows,
}})
dbutils.notebook.exit(exit_payload)
'''
