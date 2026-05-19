"""Databricks blueprint — connection test, notebook upload, Unity Catalog."""
from flask import Blueprint, request, jsonify
import os, json

from .auth import login_required
from log_config import get_logger
from config_cache import get_config
from sp_converter import get_pyspark_code
from databricks_connector import DatabricksConnector
from unity_catalog_executor import UnityCatalogExecutor

logger = get_logger(__name__)
databricks_bp = Blueprint("databricks", __name__, url_prefix="/api/v1")


def _uc_creds(data=None):
    cfg = get_config()
    d = data or {}
    host = (d.get("host") or "").strip() or cfg.get("databricks_host", "").rstrip("/")
    token = (d.get("token") or "").strip() or cfg.get("databricks_token", "")
    catalog = (d.get("catalog") or "").strip() or "main"
    schema = (d.get("schema") or "").strip() or "default"
    return host, token, catalog, schema


@databricks_bp.route("/databricks/test-connection", methods=["POST"])
@login_required
def test_databricks_connection():
    try:
        data = request.get_json()
        host = data.get("host", "").strip()
        token = data.get("token", "").strip()
        if not host or not token:
            return jsonify({"success": False, "error": "host and token are required"}), 400
        connector = DatabricksConnector(host, token)
        conn_result = connector.test_connection()
        if conn_result["success"]:
            ws_info = connector.get_workspace_info()
            clusters = connector.list_clusters()
            conn_result["workspace_info"] = ws_info
            conn_result["clusters"] = clusters.get("clusters", [])
        return jsonify(conn_result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@databricks_bp.route("/databricks/upload-notebook", methods=["POST"])
@login_required
def upload_notebook():
    try:
        data = request.get_json()
        host = data.get("host", "").strip()
        token = data.get("token", "").strip()
        sp_name = data.get("sp_name", "").strip()
        workspace_path = data.get("workspace_path", "/Shared/Migrations").strip()
        if not all([host, token, sp_name]):
            return jsonify({"success": False, "error": "host, token, and sp_name are required"}), 400
        conversion = get_pyspark_code(sp_name)
        if not conversion["success"]:
            return jsonify(conversion), 400
        connector = DatabricksConnector(host, token)
        result = connector.upload_notebook(
            notebook_name=sp_name, python_code=conversion["pyspark_code"], path=workspace_path
        )
        result["sp_name"] = sp_name
        result["conversion_notes"] = conversion.get("conversion_notes", [])
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@databricks_bp.route("/databricks/upload-multiple", methods=["POST"])
@login_required
def upload_multiple_notebooks():
    try:
        data = request.get_json()
        host = data.get("host", "").strip()
        token = data.get("token", "").strip()
        workspace_path = data.get("workspace_path", "/Shared/Migrations").strip()
        notebooks = data.get("notebooks", [])
        if not all([host, token, notebooks]):
            return jsonify({"success": False, "error": "host, token, and notebooks are required"}), 400
        connector = DatabricksConnector(host, token)
        results = []
        for nb in notebooks:
            r = connector.upload_notebook(
                notebook_name=nb.get("name", "Notebook"),
                python_code=nb.get("code", ""), path=workspace_path
            )
            results.append({"name": nb.get("name"), "success": r.get("success"),
                            "path": r.get("path"), "error": r.get("error")})
        success_count = sum(1 for r in results if r["success"])
        return jsonify({"success": success_count > 0, "results": results,
                        "uploaded": success_count, "total": len(results)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@databricks_bp.route("/databricks/upload-helper", methods=["POST"])
@login_required
def upload_helper_notebook():
    try:
        data = request.get_json()
        host = data.get("host", "").strip()
        token = data.get("token", "").strip()
        pyspark_code = data.get("pyspark_code", "").strip()
        workspace_path = data.get("workspace_path", "/Shared/Migrations").strip()
        if not all([host, token, pyspark_code]):
            return jsonify({"success": False, "error": "host, token, and pyspark_code are required"}), 400
        connector = DatabricksConnector(host, token)
        result = connector.upload_notebook(
            notebook_name="HelperFunction", python_code=pyspark_code, path=workspace_path
        )
        result["notebook_name"] = "HelperFunction"
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@databricks_bp.route("/uc/config", methods=["GET"])
@login_required
def uc_get_config():
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    has_token = bool(cfg.get("databricks_token", ""))
    catalogs_cfg = cfg.get("catalogs", {})
    cat_schemas = []
    for cat_name, cat_cfg in catalogs_cfg.items():
        for sch in cat_cfg.get("schemas", []):
            cat_schemas.append({"catalog": cat_name, "schema": sch})
    return jsonify({"success": True, "host": host, "has_token": has_token,
                    "catalog_schemas": cat_schemas})


@databricks_bp.route("/uc/catalog-schemas", methods=["GET"])
@login_required
def uc_catalog_schemas():
    """Return catalogs managed by this app with their live schemas from Databricks."""
    import requests as _req
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = cfg.get("databricks_token", "")
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"}), 400

    headers = {"Authorization": f"Bearer {token}"}
    catalogs_cfg = cfg.get("catalogs", {})
    # Include reconciliation / logging catalogs too
    extra = {}
    for key in ("reconciliation", "logging"):
        block = cfg.get(key, {})
        if block and block.get("catalog"):
            extra[block["catalog"]] = {"schemas": [block.get("schema", "default")]}
    all_cats = {**catalogs_cfg, **extra}

    # Skip admin_source (metadata only)
    skip = {(cfg.get("metadata_catalog") or "admin_source").lower()}

    result = []
    for cat_name in all_cats:
        if cat_name.lower() in skip:
            continue
        try:
            r = _req.get(f"{host}/api/2.1/unity-catalog/schemas",
                         headers=headers, params={"catalog_name": cat_name}, timeout=10)
            if r.status_code == 200:
                schemas = [s["name"] for s in r.json().get("schemas", [])
                           if s.get("name") not in ("information_schema",)]
                result.append({"catalog": cat_name, "schemas": schemas})
            else:
                # Catalog may not exist yet — use config fallback
                fallback = all_cats[cat_name]
                if isinstance(fallback, dict):
                    result.append({"catalog": cat_name, "schemas": fallback.get("schemas", ["default"])})
        except Exception:
            fallback = all_cats[cat_name]
            if isinstance(fallback, dict):
                result.append({"catalog": cat_name, "schemas": fallback.get("schemas", ["default"])})

    return jsonify({"success": True, "catalogs": result})


@databricks_bp.route("/unity-catalog/tables", methods=["POST"])
@login_required
def list_uc_tables():
    try:
        data = request.get_json()
        host, token, catalog, schema = _uc_creds(data)
        if not host or not token:
            return jsonify({"success": False, "error": "Databricks not configured in deployconfig.json"}), 400
        executor = UnityCatalogExecutor(host, token, catalog, schema)
        result = executor.list_tables()
        result["warehouses"] = executor.list_warehouses().get("warehouses", [])
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@databricks_bp.route("/unity-catalog/preview", methods=["POST"])
@login_required
def preview_uc_table():
    try:
        data = request.get_json()
        host, token, catalog, schema = _uc_creds(data)
        table_name = data.get("table_name", "").strip()
        warehouse_id = data.get("warehouse_id", "").strip()
        if not all([host, token, table_name, warehouse_id]):
            return jsonify({"success": False, "error": "table_name and warehouse_id are required"}), 400
        executor = UnityCatalogExecutor(host, token, catalog, schema)
        return jsonify(executor.preview_table(table_name, warehouse_id))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@databricks_bp.route("/unity-catalog/execute", methods=["POST"])
@login_required
def execute_uc_table():
    try:
        data = request.get_json()
        host, token, catalog, schema = _uc_creds(data)
        table_name = data.get("table_name", "").strip()
        warehouse_id = data.get("warehouse_id", "").strip()
        execute_sql = data.get("execute_sql", "").strip()
        if not all([host, token, table_name, warehouse_id]):
            return jsonify({"success": False, "error": "table_name and warehouse_id are required"}), 400
        executor = UnityCatalogExecutor(host, token, catalog, schema)
        if table_name == '__custom__':
            if not execute_sql:
                return jsonify({"success": False, "error": "execute_sql is required for custom SQL mode"}), 400
            result = executor.execute_custom_sql(execute_sql, warehouse_id)
        else:
            result = executor.execute_table_pipeline(table_name, warehouse_id, execute_sql or None)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
