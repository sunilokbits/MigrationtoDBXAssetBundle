"""Reports blueprint — email reports, audit events, DQ summary, job reports."""
from flask import Blueprint, request, jsonify
import os, json
from datetime import datetime

from .auth import login_required
from log_config import get_logger
from config_cache import get_config
from unity_catalog_executor import UnityCatalogExecutor

logger = get_logger(__name__)
reports_bp = Blueprint("reports", __name__, url_prefix="/api/v1")

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIT_LOG_PATH = os.path.join(_BASE_DIR, "audit_events.json")
DQ_RESULTS_PATH = os.path.join(_BASE_DIR, "dq_results.json")


def _append_audit_event(event):
    events = []
    if os.path.isfile(AUDIT_LOG_PATH):
        try:
            with open(AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
                events = json.load(f)
        except Exception:
            logger.warning("Could not read audit log from %s", AUDIT_LOG_PATH)
            events = []
    events.append(event)
    with open(AUDIT_LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, default=str)


@reports_bp.route("/reports/email", methods=["POST"])
@login_required
def reports_send_email():
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    d = request.get_json() or {}
    to_addr = (d.get("to") or "").strip()
    subject = d.get("subject", "Migration Pipeline Report")
    summary = d.get("summary", {})
    jobs = d.get("jobs", [])
    filters = d.get("filters", {})
    if not to_addr:
        return jsonify({"success": False, "error": "Recipient email is required"}), 400

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user or "migration-studio@noreply.com")
    if not smtp_host:
        smtp_cfg = get_config().get("smtp", {})
        if smtp_cfg.get("host"):
            smtp_host = smtp_cfg.get("host", "")
            smtp_port = int(smtp_cfg.get("port", 587))
            smtp_user = smtp_cfg.get("user", "")
            smtp_pass = smtp_cfg.get("password", "")
            smtp_from = smtp_cfg.get("from", smtp_user or "migration-studio@noreply.com")
    if not smtp_host:
        return jsonify({"success": False, "error": "SMTP not configured."}), 400

    filter_str = ", ".join(f"{k}: {v or 'all'}" for k, v in filters.items()) if filters else "none"
    job_rows = ""
    for j in jobs[:100]:
        status_color = {"success": "#059669", "failed": "#DC2626", "running": "#D97706"}.get(j.get("status", ""), "#6366F1")
        job_rows += f"""<tr>
          <td style="padding:6px 10px;border-bottom:1px solid #E5E7EB;">{j.get('job_name','—')}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #E5E7EB;">{j.get('stage','—')}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #E5E7EB;">{j.get('table_name','—')}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #E5E7EB;text-align:center;">
            <span style="padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600;color:#fff;background:{status_color};">{j.get('status','—')}</span>
          </td>
          <td style="padding:6px 10px;border-bottom:1px solid #E5E7EB;text-align:center;">{j.get('run_count',0)}</td>
        </tr>"""

    html = f"""
    <html><body style="font-family:Arial,sans-serif;color:#1F2937;max-width:800px;margin:0 auto;">
      <div style="background:linear-gradient(135deg,#0D1526,#1E293B);color:#fff;padding:24px 28px;border-radius:12px 12px 0 0;">
        <h1 style="margin:0;font-size:20px;">Migration Pipeline Report</h1>
        <p style="margin:6px 0 0;opacity:.7;font-size:13px;">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Filters: {filter_str}</p>
      </div>
      <div style="padding:20px 28px;background:#F9FAFB;border:1px solid #E5E7EB;">
        <table style="width:100%;border-collapse:collapse;font-size:13px;background:#fff;border-radius:8px;overflow:hidden;">
          <thead><tr style="background:#F3F4F6;">
            <th style="padding:8px 10px;text-align:left;">Job Name</th>
            <th style="padding:8px 10px;text-align:left;">Stage</th>
            <th style="padding:8px 10px;text-align:left;">Table</th>
            <th style="padding:8px 10px;text-align:center;">Status</th>
            <th style="padding:8px 10px;text-align:center;">Runs</th>
          </tr></thead>
          <tbody>{job_rows if job_rows else '<tr><td colspan="5" style="padding:20px;text-align:center;color:#9CA3AF;">No jobs to report</td></tr>'}</tbody>
        </table>
      </div>
    </body></html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_from
        msg["To"] = to_addr
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as srv:
            srv.ehlo()
            if smtp_port != 25:
                srv.starttls()
            if smtp_user and smtp_pass:
                srv.login(smtp_user, smtp_pass)
            srv.sendmail(smtp_from, [to_addr], msg.as_string())
        return jsonify({"success": True, "message": f"Report emailed to {to_addr}"})
    except Exception as e:
        return jsonify({"success": False, "error": f"SMTP error: {str(e)}"}), 500


@reports_bp.route("/audit/events", methods=["GET"])
@login_required
def get_audit_events():
    if not os.path.isfile(AUDIT_LOG_PATH):
        return jsonify({"events": []})
    try:
        with open(AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
            events = json.load(f)
        return jsonify({"events": events})
    except Exception as e:
        return jsonify({"events": [], "error": str(e)})


@reports_bp.route("/audit/events", methods=["POST"])
@login_required
def add_audit_event():
    data = request.get_json(force=True)
    event = {
        "timestamp": data.get("timestamp", datetime.now().isoformat()),
        "event": data.get("event", ""),
        "category": data.get("category", "access"),
        "severity": data.get("severity", "info"),
        "user": data.get("user", "system"),
        "details": data.get("details", ""),
    }
    _append_audit_event(event)
    return jsonify({"success": True, "event": event})


@reports_bp.route("/audit/execution-logs", methods=["GET"])
@login_required
def get_audit_execution_logs():
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = cfg.get("databricks_token", "")
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "logs": [], "error": "Databricks not configured."})
    log_cfg = cfg.get("logging", {})
    log_cat = log_cfg.get("catalog", "loggingdetails")
    log_sch = log_cfg.get("schema", "hr")
    log_tbl = log_cfg.get("table", "ExecutionLog")
    uc = UnityCatalogExecutor(dbx_host, dbx_token, log_cat, log_sch)
    wh_resp = uc.list_warehouses()
    warehouses = wh_resp.get("warehouses", [])
    wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
    if not wh_id and warehouses:
        wh_id = warehouses[0].get("id")
    if not wh_id:
        return jsonify({"success": False, "logs": [], "error": "No SQL Warehouse available."})
    limit = min(int(request.args.get("limit", 200)), 2000)
    offset = max(int(request.args.get("offset", 0)), 0)
    fqn = f"`{log_cat}`.`{log_sch}`.`{log_tbl}`"
    sql = f"SELECT * FROM {fqn} ORDER BY 1 DESC LIMIT {limit} OFFSET {offset}"
    result = uc._execute_statement(sql, wh_id, wait_timeout="30s")
    if result.get("error"):
        return jsonify({"success": False, "logs": [], "error": result["error"]})
    status = result.get("status", {}).get("state", "")
    if status in ("PENDING", "RUNNING"):
        stmt_id = result.get("statement_id", "")
        if stmt_id:
            result = uc._poll_statement(stmt_id)
    if result.get("status", {}).get("error"):
        return jsonify({"success": False, "logs": [], "error": str(result["status"]["error"])})
    columns = [c.get("name", "") for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = result.get("result", {}).get("data_array", [])
    logs = []
    for row in data_array:
        obj = {}
        for i, col in enumerate(columns):
            obj[col] = row[i] if i < len(row) else None
        logs.append(obj)
    return jsonify({"success": True, "logs": logs, "columns": columns, "total": len(logs)})


@reports_bp.route("/dq/summary", methods=["GET"])
@login_required
def get_dq_summary():
    if not os.path.isfile(DQ_RESULTS_PATH):
        return jsonify({"tables": []})
    try:
        with open(DQ_RESULTS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify({"tables": data if isinstance(data, list) else data.get("tables", [])})
    except Exception as e:
        return jsonify({"tables": [], "error": str(e)})


@reports_bp.route("/dq/summary", methods=["POST"])
@login_required
def save_dq_summary():
    data = request.get_json(force=True)
    tables = data.get("tables", data if isinstance(data, list) else [])
    with open(DQ_RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(tables, f, indent=2, default=str)
    return jsonify({"success": True, "count": len(tables)})


@reports_bp.route("/reports/jobs", methods=["GET"])
@login_required
def get_reports_jobs():
    cfg = get_config()
    dbx_host = cfg.get("databricks_host", "").rstrip("/")
    dbx_token = cfg.get("databricks_token", "")
    if not dbx_host or not dbx_token:
        return jsonify({"success": False, "jobs": [], "error": "Databricks not configured."})
    catalogs = cfg.get("catalogs", {})
    meta_cat = "admin_source"
    meta_sch = "Configtables"
    for cat_name, cat_cfg in catalogs.items():
        schemas = cat_cfg.get("schemas", [])
        if "Configtables" in schemas:
            meta_cat = cat_name
            meta_sch = "Configtables"
            break
    uc = UnityCatalogExecutor(dbx_host, dbx_token, meta_cat, meta_sch)
    wh_resp = uc.list_warehouses()
    warehouses = wh_resp.get("warehouses", [])
    wh_id = next((w["id"] for w in warehouses if w.get("state") == "RUNNING"), None)
    if not wh_id and warehouses:
        wh_id = warehouses[0].get("id")
    if not wh_id:
        return jsonify({"success": False, "jobs": [], "error": "No SQL Warehouse available."})
    fqn = f"`{meta_cat}`.`{meta_sch}`.`wf_job_metadata`"
    sql = f"SELECT * FROM {fqn} ORDER BY updated_at DESC"
    result = uc._execute_statement(sql, wh_id, wait_timeout="30s")
    if result.get("error"):
        return jsonify({"success": False, "jobs": [], "error": result["error"]})
    status = result.get("status", {}).get("state", "")
    if status in ("PENDING", "RUNNING"):
        stmt_id = result.get("statement_id", "")
        if stmt_id:
            result = uc._poll_statement(stmt_id)
    if result.get("status", {}).get("error"):
        return jsonify({"success": False, "jobs": [], "error": str(result["status"]["error"])})
    columns = [c.get("name", "") for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = result.get("result", {}).get("data_array", [])
    jobs = []
    for row in data_array:
        obj = {}
        for i, col in enumerate(columns):
            obj[col] = row[i] if i < len(row) else None
        obj["run_count"] = int(obj.get("run_count") or 0)
        obj["fail_count"] = int(obj.get("fail_count") or 0)
        jobs.append(obj)
    return jsonify({"success": True, "jobs": jobs, "total": len(jobs)})
