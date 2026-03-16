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

from datetime import datetime


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_metadata_notebooks(
    catalog: str = "main",
    schema: str = "default",
    landing_path: str = "/mnt/landing",
    workspace_path: str = "/Shared/MetadataPipeline",
    pipeline_mode: str = "standard",
) -> dict:
    """
    Generate metadata-driven notebooks.
    pipeline_mode: "standard" (4 notebooks) or "dlt" (3 notebooks with DLT).
    Returns: {success, notebooks: [{name, code, description, layer, lines}], summary}
    """
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
                "code":        _gen_orchestrator(catalog, schema, landing_path, workspace_path, ts),
                "description": "Orchestrator — reads metadata, chains all stages",
                "layer":       "orchestrator",
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

def _gen_orchestrator(catalog, schema, landing_path, workspace_path, ts):
    return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Metadata-Driven Orchestrator
# MAGIC **Generated:** {ts}
# MAGIC
# MAGIC Reads pipeline metadata from Delta tables and chains:
# MAGIC   Extract → Bronze → Silver for each pipeline group.
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

GROUP_ID       = dbutils.widgets.get("group_id").strip()
LOAD_OVERRIDE  = dbutils.widgets.get("load_type").strip()
PASSWORD_B64   = dbutils.widgets.get("password_b64").strip()
CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
LANDING_PATH   = dbutils.widgets.get("landing_path").strip()
WORKSPACE_PATH = dbutils.widgets.get("workspace_path").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Discover Jobs from Metadata

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

succeeded = [r for r in results if r.get("status") in ("COMPLETED", "SUCCESS")]
failed    = [r for r in results if r.get("status") == "FAILED"]
total_rows = sum(r.get("rows", 0) for r in results)

print(f"\\n{{'='*60}}")
print(f"📊 ORCHESTRATION COMPLETE")
print(f"{{'='*60}}")
print(f"  ✅ Succeeded : {{len(succeeded)}} / {{len(results)}}")
print(f"  ❌ Failed    : {{len(failed)}} / {{len(results)}}")
print(f"  📊 Total Rows: {{total_rows:,}}")

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
CATALOG      = spark.conf.get("pipeline.catalog", "{catalog}")
SCHEMA       = spark.conf.get("pipeline.schema", "{schema}")
LANDING_PATH = spark.conf.get("pipeline.landing_path", "{landing_path}")
GROUP_ID     = spark.conf.get("pipeline.group_id", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Discover Tables from Job Metadata

# COMMAND ----------

job_tbl = f"`{{CATALOG}}`.`{{SCHEMA}}`.wf_job_metadata"

_gf = f"AND group_id = '{{GROUP_ID}}'" if GROUP_ID else ""

bronze_jobs = [r.asDict() for r in spark.sql(f"""
    SELECT DISTINCT table_name, full_table, target_config, load_type
    FROM {{job_tbl}}
    WHERE stage = 'landing_to_bronze'
      AND (enabled = true OR enabled IS NULL)
      {{_gf}}
""").collect()]

silver_jobs = [r.asDict() for r in spark.sql(f"""
    SELECT DISTINCT table_name, full_table, target_config, load_type
    FROM {{job_tbl}}
    WHERE stage = 'bronze_to_silver'
      AND (enabled = true OR enabled IS NULL)
      {{_gf}}
""").collect()]

print(f"⚡ DLT — Bronze tables: {{len(bronze_jobs)}}, Silver tables: {{len(silver_jobs)}}")

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
            extract_results.append({{"job": job["job_name"], "status": "FAILED", "error": result.get("error","")}})
        else:
            print(f"    ✅ {{job['job_name']}}: {{rows:,}} rows")
            extract_results.append({{"job": job["job_name"], "status": "OK", "rows": rows}})
    except Exception as e:
        print(f"    ❌ {{job['job_name']}}: {{e}}")
        extract_results.append({{"job": job["job_name"], "status": "FAILED", "error": str(e)[:500]}})

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

pipeline_cfg = {{
    "pipeline.catalog":      CATALOG,
    "pipeline.schema":       SCHEMA,
    "pipeline.landing_path": LANDING_PATH,
    "pipeline.group_id":     GROUP_ID,
}}

pipeline_spec = {{
    "name":          DLT_NAME,
    "catalog":       CATALOG,
    "target":        SCHEMA,
    "configuration": pipeline_cfg,
    "libraries":     [{{"notebook": {{"path": DLT_NB}}}}],
    "continuous":    False,
    "development":   True,
    "channel":       "CURRENT",
}}

# Check for existing pipeline
resp = requests.get(
    f"{{HOST}}/api/2.0/pipelines",
    params={{"filter": f"name LIKE '{{DLT_NAME}}'", "max_results": 10}},
    headers=_hdrs,
)
resp.raise_for_status()
existing = [p for p in resp.json().get("statuses", []) if p["name"] == DLT_NAME]

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
MAX_POLLS   = 240   # 240 × 15s = 60 min max

while dlt_status not in terminal_states and poll_count < MAX_POLLS:
    time.sleep(15)
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
        if poll_count % 4 == 0:
            print(f"  ⏳ DLT status: {{update_state or pipe_data.get('state','UNKNOWN')}} ({{poll_count * 15}}s)")
    except Exception as e:
        print(f"  ⚠️ Poll error: {{e}}")

if poll_count >= MAX_POLLS and dlt_status not in terminal_states:
    dlt_status = "TIMEOUT"
print(f"\\n⚡ DLT pipeline finished: {{dlt_status}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Orchestration Summary

# COMMAND ----------

total_rows = sum(r.get("rows", 0) for r in extract_results)

print(f"\\n{{'='*60}}")
print(f"📊 DLT ORCHESTRATION COMPLETE")
print(f"{{'='*60}}")
print(f"  📥 Extracts   : {{extract_ok}} ok / {{extract_fail}} failed")
print(f"  ⚡ DLT Pipeline: {{dlt_status}}")
print(f"  📊 Rows (JDBC) : {{total_rows:,}}")
print(f"  🔗 Pipeline ID : {{pipeline_id}}")

exit_payload = json.dumps({{
    "status":          "COMPLETED" if dlt_status == "COMPLETED" and not extract_fail else "PARTIAL",
    "extract_ok":      extract_ok,
    "extract_failed":  extract_fail,
    "dlt_status":      dlt_status,
    "pipeline_id":     pipeline_id,
    "total_rows":      total_rows,
}})
dbutils.notebook.exit(exit_payload)
'''
