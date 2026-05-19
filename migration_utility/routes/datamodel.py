"""Data Modeling blueprint — AI-driven star/snowflake schema builder."""
from flask import Blueprint, request, jsonify
import os, json, hashlib, requests as req

from .auth import login_required
from log_config import get_logger
from config_cache import get_config
from unity_catalog_executor import UnityCatalogExecutor
import data_modeling as dm
import persistence as db

logger = get_logger(__name__)
datamodel_bp = Blueprint("datamodel", __name__, url_prefix="/api/v1")

# In-memory model cache (also persisted to SQLite via persistence.py)
_DM_MODELS = {}


def _dm_get_warehouse(host, token):
    try:
        s = req.Session()
        s.headers.update({"Authorization": f"Bearer {token}"})
        resp = s.get(f"{host}/api/2.0/sql/warehouses", timeout=15)
        if resp.status_code == 200:
            whs = resp.json().get("warehouses", [])
            running = [w for w in whs if w.get("state") == "RUNNING"]
            if running:
                return running[0]["id"]
            if whs:
                return whs[0]["id"]
    except Exception:
        logger.warning("Could not fetch warehouse list from Databricks")
        pass
    return None


# ── Sample / Demo data ────────────────────────────────────────────────────────
_SAMPLE_TABLES_META = [
    {"table_name": "fact_sales", "columns": [
        {"name": "sale_id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True},
        {"name": "customer_id", "data_type": "INT", "is_nullable": False},
        {"name": "product_id", "data_type": "INT", "is_nullable": False},
        {"name": "store_id", "data_type": "INT", "is_nullable": False},
        {"name": "order_date", "data_type": "DATE", "is_nullable": False},
        {"name": "quantity", "data_type": "INT", "is_nullable": True},
        {"name": "unit_price", "data_type": "DECIMAL(18,2)", "is_nullable": True},
        {"name": "total_amount", "data_type": "DECIMAL(18,2)", "is_nullable": True},
        {"name": "discount", "data_type": "FLOAT", "is_nullable": True},
    ]},
    {"table_name": "fact_orders", "columns": [
        {"name": "order_id", "data_type": "BIGINT", "is_nullable": False, "is_pk": True},
        {"name": "customer_id", "data_type": "INT", "is_nullable": False},
        {"name": "employee_id", "data_type": "INT", "is_nullable": False},
        {"name": "order_date", "data_type": "DATE", "is_nullable": False},
        {"name": "ship_date", "data_type": "DATE", "is_nullable": True},
        {"name": "freight", "data_type": "DECIMAL(10,2)", "is_nullable": True},
        {"name": "total_amount", "data_type": "DECIMAL(18,2)", "is_nullable": True},
    ]},
    {"table_name": "dim_customer", "columns": [
        {"name": "customer_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "first_name", "data_type": "STRING", "is_nullable": True},
        {"name": "last_name", "data_type": "STRING", "is_nullable": True},
        {"name": "email", "data_type": "STRING", "is_nullable": True},
        {"name": "phone", "data_type": "STRING", "is_nullable": True},
        {"name": "city", "data_type": "STRING", "is_nullable": True},
        {"name": "region_id", "data_type": "INT", "is_nullable": True},
    ]},
    {"table_name": "dim_product", "columns": [
        {"name": "product_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "product_name", "data_type": "STRING", "is_nullable": True},
        {"name": "category_id", "data_type": "INT", "is_nullable": True},
        {"name": "brand", "data_type": "STRING", "is_nullable": True},
        {"name": "unit_cost", "data_type": "DECIMAL(10,2)", "is_nullable": True},
    ]},
    {"table_name": "dim_store", "columns": [
        {"name": "store_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "store_name", "data_type": "STRING", "is_nullable": True},
        {"name": "city", "data_type": "STRING", "is_nullable": True},
        {"name": "state", "data_type": "STRING", "is_nullable": True},
        {"name": "region_id", "data_type": "INT", "is_nullable": True},
    ]},
    {"table_name": "dim_employee", "columns": [
        {"name": "employee_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "first_name", "data_type": "STRING", "is_nullable": True},
        {"name": "last_name", "data_type": "STRING", "is_nullable": True},
        {"name": "department_id", "data_type": "INT", "is_nullable": True},
        {"name": "hire_date", "data_type": "DATE", "is_nullable": True},
    ]},
    {"table_name": "dim_category", "columns": [
        {"name": "category_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "category_name", "data_type": "STRING", "is_nullable": True},
        {"name": "description", "data_type": "STRING", "is_nullable": True},
    ]},
    {"table_name": "dim_region", "columns": [
        {"name": "region_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "region_name", "data_type": "STRING", "is_nullable": True},
        {"name": "country", "data_type": "STRING", "is_nullable": True},
    ]},
    {"table_name": "dim_department", "columns": [
        {"name": "department_id", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "department_name", "data_type": "STRING", "is_nullable": True},
        {"name": "location", "data_type": "STRING", "is_nullable": True},
    ]},
    {"table_name": "dim_date", "columns": [
        {"name": "date_key", "data_type": "INT", "is_nullable": False, "is_pk": True},
        {"name": "full_date", "data_type": "DATE", "is_nullable": False},
        {"name": "year", "data_type": "INT", "is_nullable": True},
        {"name": "quarter", "data_type": "INT", "is_nullable": True},
        {"name": "month", "data_type": "INT", "is_nullable": True},
        {"name": "month_name", "data_type": "STRING", "is_nullable": True},
        {"name": "day_of_week", "data_type": "STRING", "is_nullable": True},
    ]},
]


def _save_model(key, model):
    """Cache model both in-memory and in SQLite."""
    _DM_MODELS[key] = model
    db.save_model(key, model)


def _get_model(key):
    """Retrieve model from in-memory cache; fallback to SQLite."""
    model = _DM_MODELS.get(key)
    if model is None:
        model = db.load_model(key)
        if model:
            _DM_MODELS[key] = model
    return model


@datamodel_bp.route("/datamodel/catalogs-schemas", methods=["GET"])
@login_required
def dm_list_catalogs_schemas():
    cfg = get_config()
    catalogs_cfg = cfg.get("catalogs", {})
    result = []
    for cat_name, cat_cfg in catalogs_cfg.items():
        for sch in cat_cfg.get("schemas", []):
            result.append({"catalog": cat_name, "schema": sch})
    return jsonify({"success": True, "catalog_schemas": result})


@datamodel_bp.route("/datamodel/sample-generate", methods=["POST"])
@login_required
def dm_sample_generate():
    d = request.get_json(force=True)
    table_names = d.get("tables", [])
    if not table_names:
        tables_meta = list(_SAMPLE_TABLES_META)
    else:
        tables_meta = [t for t in _SAMPLE_TABLES_META if t["table_name"] in table_names]
    if not tables_meta:
        return jsonify({"success": False, "error": "No matching sample tables found"})
    schema_choice = d.get("schema_choice", "auto")
    model = dm.classify_tables(tables_meta, schema_choice)
    er_json = dm.generate_er_json(model)
    ddl = dm.generate_ddl(model, "sample_catalog", "sample_schema")
    key = hashlib.md5(json.dumps([t["table_name"] for t in tables_meta], sort_keys=True).encode()).hexdigest()[:12]
    _save_model(key, model)
    return jsonify({
        "success": True, "model_id": key, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
    })


@datamodel_bp.route("/datamodel/sample-tables", methods=["GET"])
@login_required
def dm_sample_tables():
    return jsonify({"success": True, "tables": [t["table_name"] for t in _SAMPLE_TABLES_META]})


@datamodel_bp.route("/datamodel/tables", methods=["POST"])
@login_required
def dm_list_tables():
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = cfg.get("databricks_token", "")
    catalog = d.get("catalog", "").strip()
    schema = d.get("schema", "").strip()
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not catalog or not schema:
        return jsonify({"success": False, "error": "Catalog and schema required"})
    executor = UnityCatalogExecutor(host, token, catalog, schema)
    wh_id = _dm_get_warehouse(host, token)
    tables = dm.list_available_tables(executor, catalog, schema, wh_id)
    return jsonify({"success": True, "tables": tables})


@datamodel_bp.route("/datamodel/generate", methods=["POST"])
@login_required
def dm_generate_model():
    d = request.get_json(force=True)
    cfg = get_config()
    host = cfg.get("databricks_host", "").rstrip("/")
    token = cfg.get("databricks_token", "")
    catalog = d.get("catalog", "").strip()
    schema = d.get("schema", "").strip()
    table_names = d.get("tables", [])
    if not host or not token:
        return jsonify({"success": False, "error": "Databricks not configured"})
    if not table_names:
        return jsonify({"success": False, "error": "Select at least one table"})
    executor = UnityCatalogExecutor(host, token, catalog, schema)
    wh_id = _dm_get_warehouse(host, token)
    tables_meta = dm.fetch_table_metadata(executor, catalog, schema, table_names, wh_id)
    schema_choice = d.get("schema_choice", "auto")
    model = dm.classify_tables(tables_meta, schema_choice)
    er_json = dm.generate_er_json(model)
    ddl = dm.generate_ddl(model, catalog, schema)
    key = hashlib.md5(json.dumps(table_names, sort_keys=True).encode()).hexdigest()[:12]
    _save_model(key, model)
    return jsonify({
        "success": True, "model_id": key, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
    })


@datamodel_bp.route("/datamodel/edit", methods=["POST"])
@login_required
def dm_edit_model():
    d = request.get_json(force=True)
    model_id = d.get("model_id", "")
    edits = d.get("edits", {})
    model = _get_model(model_id)
    if model is None:
        return jsonify({"success": False, "error": "Model not found. Please regenerate."})
    model = dm.apply_manual_edits(model, edits)
    _save_model(model_id, model)
    er_json = dm.generate_er_json(model)
    cfg = get_config()
    catalog = d.get("catalog", next(iter(cfg.get("catalogs", {})), "main"))
    schema = d.get("schema", "default")
    ddl = dm.generate_ddl(model, catalog, schema)
    return jsonify({
        "success": True, "model_id": model_id, "schema_type": model["schema_type"],
        "facts": model["facts"], "dimensions": model["dimensions"],
        "relationships": model["relationships"], "er_json": er_json, "ddl": ddl,
    })


@datamodel_bp.route("/datamodel/ddl", methods=["POST"])
@login_required
def dm_get_ddl():
    d = request.get_json(force=True)
    model_id = d.get("model_id", "")
    catalog = d.get("catalog", "main")
    schema = d.get("schema", "default")
    model = _get_model(model_id)
    if model is None:
        return jsonify({"success": False, "error": "Model not found"})
    ddl = dm.generate_ddl(model, catalog, schema)
    return jsonify({"success": True, "ddl": ddl})
