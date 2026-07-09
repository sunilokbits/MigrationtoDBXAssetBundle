"""Workflow blueprint — AI workflow manager endpoints."""
from flask import Blueprint, request, jsonify
import json

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, get_databricks_token, get_source_password
import workflow_manager as wfm
from data_migrator import DataMigrator, _build_conn_str
from keyvault_helper import is_masked

logger = get_logger(__name__)
workflow_bp = Blueprint("workflow", __name__, url_prefix="/api/v1")


@workflow_bp.route("/workflow/list-tables", methods=["POST"])
@login_required
def wf_list_tables():
    """List source SQL Server tables for Pipeline Studio."""
    try:
        d = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server = d.get("server", "").strip()
        database = d.get("database", "").strip()
        username = d.get("username", "").strip()
        password = d.get("password", "")
        if not password or is_masked(password):
            password = get_source_password()
        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username required"}), 400
        conn_str = _build_conn_str(source_type, server, database, username, password)
        migrator = DataMigrator(conn_str, "http://placeholder", "placeholder")
        tables = migrator.list_source_tables()
        return jsonify({"success": True, "tables": tables, "total": len(tables)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/metadata/init", methods=["POST"])
@login_required
def wf_metadata_init():
    d = request.get_json() or {}
    token = d.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    return jsonify(wfm.init_metadata_flow(
        host=d.get("host", "").strip(), token=token,
        catalog=d.get("catalog", "main").strip(), schema=d.get("schema", "default").strip(),
        warehouse_id=d.get("warehouse_id", "").strip(),
    ))


@workflow_bp.route("/workflow/auto-init", methods=["POST"])
@login_required
def wf_auto_init():
    try:
        cfg = get_config()
        if not cfg:
            return jsonify({"success": False, "reason": "no_config"})
        host = (cfg.get("databricks_host") or "").strip().rstrip("/")
        token = get_databricks_token()
        catalog = (cfg.get("metadata_catalog") or "").strip()
        schema = (cfg.get("metadata_schema") or "").strip()
        if not host or not token:
            return jsonify({"success": False, "reason": "no_credentials"})
        if not catalog or not schema:
            return jsonify({"success": False, "reason": "no_metadata_location"})
        if wfm._metadata_initialized and wfm._dbr_host == host and wfm._dbr_catalog == catalog:
            return jsonify({"success": True, "already_initialized": True,
                            "catalog": catalog, "schema": schema})
        result = wfm.init_metadata_flow(host=host, token=token, catalog=catalog, schema=schema)
        if result.get("success"):
            wfm.load_metadata_from_dbr()
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@workflow_bp.route("/workflow/metadata/status", methods=["GET"])
@login_required
def wf_metadata_status():
    return jsonify(wfm.get_metadata_status())


@workflow_bp.route("/workflow/metadata/load", methods=["POST"])
@login_required
def wf_metadata_load():
    return jsonify(wfm.load_metadata_from_dbr())


@workflow_bp.route("/workflow/metadata/sync", methods=["POST"])
@login_required
def wf_metadata_sync():
    # Fix 3: async by default — dispatch background task, return task_id.
    # Callers wanting blocking behavior can pass ?mode=sync.
    mode = (request.args.get("mode") or "").lower()
    if mode == "sync":
        return jsonify(wfm.full_sync_to_dbr())
    return jsonify(wfm.start_full_sync_to_dbr())


@workflow_bp.route("/workflow/metadata/sync-status/<task_id>", methods=["GET"])
@login_required
def wf_metadata_sync_status(task_id):
    # Fix 3: poll endpoint for background full-sync tasks.
    return jsonify(wfm.get_full_sync_status(task_id))


@workflow_bp.route("/workflow/metadata/save-sources", methods=["POST"])
@login_required
def wf_metadata_save_sources():
    d = request.get_json() or {}
    return jsonify(wfm.sync_source_tables_to_dbr(
        tables=d.get("tables", []), source_config=d.get("source_config", {}),
    ))


@workflow_bp.route("/workflow/notebooks/deploy", methods=["POST"])
@login_required
def wf_deploy_notebooks():
    d = request.get_json() or {}
    token = d.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    return jsonify(wfm.deploy_metadata_notebooks(
        host=d.get("host", "").strip(), token=token,
        catalog=d.get("catalog", "main").strip(), schema=d.get("schema", "default").strip(),
        landing_path=d.get("landing_path", "/mnt/landing").strip(),
        workspace_path=d.get("workspace_path", "/Shared/MetadataPipeline").strip(),
        pipeline_mode=d.get("pipeline_mode", "standard").strip(),
        cdc_mode=d.get("cdc_mode", "watermark").strip(),
        primary_keys=d.get("primary_keys", []),
        recon_catalog=d.get("recon_catalog", "reconciliation").strip(),
        recon_schema=d.get("recon_schema", "hr").strip(),
        recon_table=d.get("recon_table", "ReconcilationDetails").strip(),
        log_catalog=d.get("log_catalog", "logging").strip(),
        log_schema=d.get("log_schema", "hr").strip(),
        log_table=d.get("log_table", "ExecutionLog").strip(),
        recon_location=d.get("recon_location", "").strip(),
        log_location=d.get("log_location", "").strip(),
    ))


@workflow_bp.route("/workflow/notebooks/status", methods=["GET"])
@login_required
def wf_notebook_status():
    return jsonify(wfm.get_notebook_status())


@workflow_bp.route("/workflow/dq-checks", methods=["GET"])
@login_required
def wf_dq_checks():
    mode = request.args.get("mode", "standard")
    checks = {
        "standard": {
            "bronze": [
                {"id": "DQ-01", "name": "Empty File Detection", "action": "skip", "desc": "Skip Bronze write when landing has 0 rows"},
                {"id": "DQ-02", "name": "Null-Key Detection", "action": "quarantine", "desc": "Flag rows where ALL data columns are null"},
                {"id": "DQ-03", "name": "Duplicate Detection", "action": "warn", "desc": "Count exact-match duplicate rows"},
                {"id": "DQ-04", "name": "Schema Drift Detection", "action": "warn", "desc": "Detect new or missing columns vs existing table"},
                {"id": "DQ-05", "name": "Quarantine Flagging", "action": "flag", "desc": "Mark invalid rows with __is_quarantined=true"},
            ],
            "silver": [
                {"id": "DQ-01", "name": "Quarantine Filter", "action": "drop", "desc": "Exclude rows flagged as quarantined in Bronze"},
                {"id": "DQ-02", "name": "All-Null Removal", "action": "drop", "desc": "Drop records where all data columns are null"},
                {"id": "DQ-03", "name": "Per-Column Null %", "action": "warn", "desc": "Flag columns exceeding 80% null threshold"},
                {"id": "DQ-04", "name": "Deduplication", "action": "drop", "desc": "Remove exact duplicate rows on data columns"},
                {"id": "DQ-05", "name": "String Trimming", "action": "fix", "desc": "Trim whitespace from all string columns"},
                {"id": "DQ-06", "name": "Empty→NULL", "action": "fix", "desc": "Convert empty strings to NULL values"},
                {"id": "DQ-07", "name": "Row Count Anomaly", "action": "warn", "desc": "Alert if row count changes >50% vs last run"},
            ],
            "common": [
                {"id": "RST", "name": "Restore Points", "action": "safety", "desc": "Auto version snapshot before each write"},
                {"id": "RBK", "name": "Auto Rollback", "action": "safety", "desc": "Revert table to previous version on write failure"},
                {"id": "MTR", "name": "DQ Metrics Table", "action": "track", "desc": "__dq_metrics with score, nulls, dupes, drift per run"},
            ],
        },
        "dlt": {
            "bronze": [
                {"id": "dq01", "name": "Valid Landing TS", "action": "expect_or_drop", "desc": "__landing_ts IS NOT NULL"},
                {"id": "dq02", "name": "Source System Present", "action": "expect", "desc": "__source_system IS NOT NULL"},
                {"id": "dq03", "name": "Batch ID Present", "action": "expect", "desc": "__batch_id IS NOT NULL"},
                {"id": "dq04", "name": "Data Freshness", "action": "expect", "desc": "Landing timestamp within 7 days"},
                {"id": "dq05", "name": "Not All Null", "action": "expect", "desc": "Not all audit columns are null simultaneously"},
            ],
            "silver": [
                {"id": "dq01", "name": "Valid Bronze TS", "action": "expect_or_drop", "desc": "__bronze_ts IS NOT NULL"},
                {"id": "dq02", "name": "Not Quarantined", "action": "expect_or_drop", "desc": "__is_quarantined = false"},
                {"id": "dq03", "name": "Source Table Present", "action": "expect", "desc": "__source_table IS NOT NULL"},
                {"id": "dq04", "name": "Bronze Freshness", "action": "expect", "desc": "Bronze timestamp within 7 days"},
                {"id": "dq05", "name": "Source Not Empty", "action": "expect", "desc": "Source table name is non-empty string"},
            ],
            "common": [
                {"id": "AL", "name": "Auto Loader", "action": "built-in", "desc": "Streaming ingestion with schema evolution"},
                {"id": "DD", "name": "Deduplication", "action": "built-in", "desc": "dropDuplicates() on all columns in Silver"},
                {"id": "TR", "name": "String Trimming", "action": "built-in", "desc": "Whitespace normalization on string columns"},
                {"id": "EL", "name": "DLT Event Log", "action": "built-in", "desc": "All expectations auto-tracked in event log"},
            ],
        },
    }
    return jsonify({"success": True, "mode": mode, "checks": checks.get(mode, checks["standard"])})


@workflow_bp.route("/workflow/pipelines/<group_id>/run-databricks", methods=["POST"])
@login_required
def wf_run_on_databricks(group_id):
    d = request.get_json() or {}
    token = d.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    password = d.get("password", "")
    if not password or is_masked(password):
        password = get_source_password()
    result = wfm.run_pipeline_on_databricks(
        group_id=group_id, host=d.get("host", "").strip(),
        token=token, cluster_id=d.get("cluster_id", "").strip(),
        load_type=d.get("load_type", "").strip(), password=password,
        workspace_path=d.get("workspace_path", "").strip(),
        catalog=d.get("catalog", "").strip(), schema=d.get("schema", "").strip(),
        landing_path=d.get("landing_path", "/mnt/landing").strip(),
        recon_catalog=d.get("recon_catalog", "reconciliation").strip(),
        recon_schema=d.get("recon_schema", "hr").strip(),
        recon_table=d.get("recon_table", "ReconcilationDetails").strip(),
        log_catalog=d.get("log_catalog", "logging").strip(),
        log_schema=d.get("log_schema", "hr").strip(),
        log_table=d.get("log_table", "ExecutionLog").strip(),
    )
    if not result.get("success"):
        logger.warning("run-databricks failed for group '%s': %s", group_id, result.get('error') or result.get('message'))
    return jsonify(result)


@workflow_bp.route("/workflow/stats", methods=["GET"])
@login_required
def wf_stats():
    return jsonify(wfm.get_dashboard_stats())


@workflow_bp.route("/workflow/clusters", methods=["GET"])
@login_required
def wf_list_clusters():
    host = request.args.get("host", "").strip()
    token = request.args.get("token", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    if not host or not token:
        return jsonify({"success": False, "error": "host and token required"})
    try:
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        result = connector.list_clusters()
        # If token was rejected (403), clear cache and retry with fresh token from KV
        if not result.get("success") and "403" in str(result.get("message", "")):
            from keyvault_helper import clear_cache
            clear_cache()
            token = get_databricks_token()
            if token:
                connector = DatabricksConnector(host, token)
                result = connector.list_clusters()
        # Normalize error key for frontend
        if not result.get("success") and "message" in result and "error" not in result:
            result["error"] = result.pop("message")
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@workflow_bp.route("/workflow/clusters/start", methods=["POST"])
@login_required
def wf_start_cluster():
    d = request.get_json() or {}
    host = d.get("host", "").strip()
    token = d.get("token", "").strip()
    cluster_id = d.get("cluster_id", "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    if not host or not token or not cluster_id:
        return jsonify({"success": False, "error": "host, token, and cluster_id required"})
    try:
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        return jsonify(connector.start_cluster(cluster_id))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@workflow_bp.route("/workflow/create-pipeline", methods=["POST"])
@login_required
def wf_create_pipeline():
    d = request.get_json() or {}
    return jsonify(wfm.create_pipeline_for_table(
        table_schema=d.get("table_schema", "dbo"), table_name=d.get("table_name", ""),
        load_type=d.get("load_type", "full"), watermark_column=d.get("watermark_column", ""),
        source_config=d.get("source_config"), target_config=d.get("target_config"),
        pipeline_mode=d.get("pipeline_mode", "standard"), cdc_mode=d.get("cdc_mode", "watermark"),
        primary_keys=d.get("primary_keys", []),
    ))


@workflow_bp.route("/workflow/create-pipelines-bulk", methods=["POST"])
@login_required
def wf_create_pipelines_bulk():
    d = request.get_json() or {}
    return jsonify(wfm.create_pipelines_bulk(
        tables=d.get("tables", []), source_config=d.get("source_config"),
        target_config=d.get("target_config"), pipeline_mode=d.get("pipeline_mode", "standard"),
        cdc_mode=d.get("cdc_mode", "watermark"), primary_keys=d.get("primary_keys", []),
    ))


@workflow_bp.route("/workflow/pipelines", methods=["GET"])
@login_required
def wf_list_pipelines():
    return jsonify(wfm.list_pipeline_groups_live())


@workflow_bp.route("/workflow/jobs", methods=["GET"])
@login_required
def wf_list_jobs():
    return jsonify(wfm.list_jobs(
        group_id=request.args.get("group_id"), stage=request.args.get("stage"),
        status=request.args.get("status"),
    ))


@workflow_bp.route("/workflow/jobs/<job_id>", methods=["GET"])
@login_required
def wf_get_job(job_id):
    return jsonify(wfm.get_job(job_id))


@workflow_bp.route("/workflow/jobs/<job_id>", methods=["PUT"])
@login_required
def wf_update_job(job_id):
    d = request.get_json() or {}
    return jsonify(wfm.update_job(job_id, d))


@workflow_bp.route("/workflow/jobs/<job_id>", methods=["DELETE"])
@login_required
def wf_delete_job(job_id):
    return jsonify(wfm.delete_job(job_id))


@workflow_bp.route("/workflow/jobs/history", methods=["GET"])
@login_required
def wf_job_history():
    table_name = request.args.get("table_name", "").strip()
    try:
        if not wfm._metadata_initialized:
            return jsonify({"success": False, "error": "MetadataFlow not initialized"})
        where = ""
        if table_name:
            where = f" WHERE table_name = {wfm._esc(table_name)}"
        sql = f"SELECT * FROM {wfm._fqn(wfm.TBL_JOBS_HISTORY)}{where} ORDER BY archived_at DESC"
        r = wfm._exec_sql(sql)
        state = r.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            return jsonify({"success": False, "error": "Query failed", "detail": r})
        cols = [c.get("name", "") for c in r.get("result", {}).get("schema", {}).get("columns", [])]
        rows = r.get("result", {}).get("data_array", [])
        history = [dict(zip(cols, row)) for row in rows]
        return jsonify({"success": True, "history": history, "total": len(history)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@workflow_bp.route("/workflow/pipelines/<group_id>", methods=["DELETE"])
@login_required
def wf_delete_pipeline(group_id):
    return jsonify(wfm.delete_pipeline_group(group_id))


@workflow_bp.route("/workflow/jobs/<job_id>/run", methods=["POST"])
@login_required
def wf_run_job(job_id):
    d = request.get_json() or {}
    return jsonify(wfm.run_job(job_id, force_full=d.get("force_full", False)))


@workflow_bp.route("/workflow/pipelines/<group_id>/run", methods=["POST"])
@login_required
def wf_run_pipeline(group_id):
    d = request.get_json() or {}
    return jsonify(wfm.run_pipeline_group(group_id, force_full=d.get("force_full", False)))


@workflow_bp.route("/workflow/pipelines/<group_id>/rerun", methods=["POST"])
@login_required
def wf_rerun_pipeline(group_id):
    return jsonify(wfm.rerun_from_failure(group_id))


@workflow_bp.route("/workflow/runs/<run_id>", methods=["GET"])
@login_required
def wf_get_run(run_id):
    return jsonify(wfm.get_run_status(run_id))


@workflow_bp.route("/workflow/runs/<run_id>/databricks-output", methods=["POST"])
@login_required
def wf_get_dbr_output(run_id):
    body = request.get_json(force=True)
    host = (body.get("host") or "").strip()
    token = (body.get("token") or "").strip()
    if not token or is_masked(token):
        token = get_databricks_token()
    if not host or not token:
        return jsonify({"success": False, "message": "Databricks host and token required"}), 400
    run_info = wfm.get_run_status(run_id)
    if not run_info.get("success"):
        return jsonify({"success": False, "message": "Run not found"}), 404
    dbr_run_id = run_info.get("run", {}).get("dbr_run_id")
    if not dbr_run_id:
        return jsonify({"success": False, "message": "No Databricks run ID associated with this run"}), 404
    from databricks_connector import DatabricksConnector
    conn = DatabricksConnector(host, token)
    return jsonify(conn.get_run_output(int(dbr_run_id)))


@workflow_bp.route("/workflow/runs", methods=["GET"])
@login_required
def wf_list_runs():
    return jsonify(wfm.list_runs(
        job_id=request.args.get("job_id"), group_id=request.args.get("group_id"),
        status=request.args.get("status"), limit=request.args.get("limit", 50, type=int),
    ))


@workflow_bp.route("/workflow/jobs/add", methods=["POST"])
@login_required
def wf_add_custom_job():
    d = request.get_json() or {}
    return jsonify(wfm.add_custom_job(
        job_name=d.get("job_name", ""), stage=d.get("stage", "extract"),
        table_schema=d.get("table_schema", "dbo"), table_name=d.get("table_name", ""),
        load_type=d.get("load_type", "full"), watermark_column=d.get("watermark_column", ""),
        group_id=d.get("group_id"),
    ))


@workflow_bp.route("/workflow/watermarks", methods=["GET"])
@login_required
def wf_watermarks():
    return jsonify(wfm.get_watermarks())


@workflow_bp.route("/workflow/watermarks/update", methods=["POST"])
@login_required
def wf_update_watermark():
    d = request.get_json() or {}
    return jsonify(wfm.update_watermark(d.get("table"), d.get("column"), d.get("value")))


@workflow_bp.route("/workflow/watermarks/reset", methods=["POST"])
@login_required
def wf_reset_watermark():
    d = request.get_json() or {}
    return jsonify(wfm.reset_watermark(d.get("table")))
