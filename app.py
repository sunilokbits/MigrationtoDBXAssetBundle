"""
Flask Backend — SP Migration Utility
Routes for: SP → PySpark conversion, Databricks connection test, Notebook upload, Unity Catalog execution
"""

from flask import Flask, request, jsonify, render_template_string, Response
import os, json, traceback, time
from datetime import datetime

from stored_procedures    import STORED_PROCEDURES, SQL_VIEWS, SQL_UDFS, ALL_OBJECTS
from sp_converter          import get_pyspark_code, get_combined_pyspark_code, get_separate_pyspark_codes
from databricks_connector  import DatabricksConnector
from unity_catalog_executor import UnityCatalogExecutor
from data_migrator         import DataMigrator, MIGRATION_JOBS, _build_conn_str
from medallion_notebooks   import generate_all_medallion_notebooks
import self_healing_bot as healer
import workflow_manager as wfm

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Serve Main UI                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/")
def index():
    with open(os.path.join(os.path.dirname(__file__), "templates", "index.html"), encoding="utf-8") as f:
        return f.read()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  1.  List Stored Procedures                                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/stored-procedures", methods=["GET"])
def list_stored_procedures():
    procedures = [
        {
            "name"       : sp["name"],
            "description": sp["description"]
        }
        for sp in STORED_PROCEDURES.values()
    ]
    return jsonify({"success": True, "procedures": procedures})


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  2.  Get SP Source Code                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/sp-code/<sp_name>", methods=["GET"])
def get_sp_code(sp_name):
    sp = STORED_PROCEDURES.get(sp_name)
    if not sp:
        return jsonify({"success": False, "error": f"SP '{sp_name}' not found"}), 404
    return jsonify({
        "success"    : True,
        "name"       : sp["name"],
        "description": sp["description"],
        "code"       : sp["code"].strip()
    })


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  3.  Convert SP → PySpark                                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/convert", methods=["POST"])
def convert_to_pyspark():
    try:
        data    = request.get_json()
        sp_name = data.get("sp_name", "").strip()

        if not sp_name:
            return jsonify({"success": False, "error": "sp_name is required"}), 400

        result = get_pyspark_code(sp_name)
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  4.  Test Databricks Connection                                             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/databricks/test-connection", methods=["POST"])
def test_databricks_connection():
    try:
        data  = request.get_json()
        host  = data.get("host", "").strip()
        token = data.get("token", "").strip()

        if not host or not token:
            return jsonify({"success": False, "error": "host and token are required"}), 400

        connector = DatabricksConnector(host, token)
        conn_result = connector.test_connection()

        if conn_result["success"]:
            ws_info = connector.get_workspace_info()
            clusters = connector.list_clusters()
            conn_result["workspace_info"] = ws_info
            conn_result["clusters"]       = clusters.get("clusters", [])

        return jsonify(conn_result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  5.  Upload Notebook to Databricks                                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/databricks/upload-notebook", methods=["POST"])
def upload_notebook():
    try:
        data          = request.get_json()
        host          = data.get("host", "").strip()
        token         = data.get("token", "").strip()
        sp_name       = data.get("sp_name", "").strip()
        workspace_path = data.get("workspace_path", "/Shared/Migrations").strip()

        if not all([host, token, sp_name]):
            return jsonify({"success": False, "error": "host, token, and sp_name are required"}), 400

        # Get PySpark code
        conversion = get_pyspark_code(sp_name)
        if not conversion["success"]:
            return jsonify(conversion), 400

        connector = DatabricksConnector(host, token)
        result    = connector.upload_notebook(
            notebook_name=sp_name,
            python_code=conversion["pyspark_code"],
            path=workspace_path
        )
        result["sp_name"]          = sp_name
        result["conversion_notes"] = conversion.get("conversion_notes", [])
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  6.  List Unity Catalog Tables                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/unity-catalog/tables", methods=["POST"])
def list_uc_tables():
    try:
        data    = request.get_json()
        host    = data.get("host", "").strip()
        token   = data.get("token", "").strip()
        catalog = data.get("catalog", "main").strip()
        schema  = data.get("schema", "default").strip()

        if not host or not token:
            return jsonify({"success": False, "error": "host and token are required"}), 400

        executor = UnityCatalogExecutor(host, token, catalog, schema)
        result   = executor.list_tables()
        result["warehouses"] = executor.list_warehouses().get("warehouses", [])
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  7.  Preview Unity Catalog Table                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/unity-catalog/preview", methods=["POST"])
def preview_uc_table():
    try:
        data         = request.get_json()
        host         = data.get("host", "").strip()
        token        = data.get("token", "").strip()
        catalog      = data.get("catalog", "main").strip()
        schema       = data.get("schema", "default").strip()
        table_name   = data.get("table_name", "").strip()
        warehouse_id = data.get("warehouse_id", "").strip()

        if not all([host, token, table_name, warehouse_id]):
            return jsonify({"success": False, "error": "host, token, table_name, and warehouse_id are required"}), 400

        executor = UnityCatalogExecutor(host, token, catalog, schema)
        result   = executor.preview_table(table_name, warehouse_id)
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  8.  Execute Table Pipeline in Unity Catalog                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/unity-catalog/execute", methods=["POST"])
def execute_uc_table():
    try:
        data         = request.get_json()
        host         = data.get("host", "").strip()
        token        = data.get("token", "").strip()
        catalog      = data.get("catalog", "main").strip()
        schema       = data.get("schema", "default").strip()
        table_name   = data.get("table_name", "").strip()
        warehouse_id = data.get("warehouse_id", "").strip()
        execute_sql  = data.get("execute_sql", "").strip()

        if not all([host, token, table_name, warehouse_id]):
            return jsonify({"success": False, "error": "host, token, table_name, warehouse_id are required"}), 400

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


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  9.  List All SQL Objects (SPs + Views + UDFs)                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/all-objects", methods=["GET"])
def list_all_objects():
    grouped = {"stored_procedure": [], "view": [], "udf": []}
    for key, obj in ALL_OBJECTS.items():
        otype = obj.get("object_type", "stored_procedure")
        grouped[otype].append({
            "key"        : key,
            "name"       : obj.get("name", key),
            "description": obj.get("description", ""),
            "object_type": otype
        })
    return jsonify({
        "success": True,
        "grouped": grouped,
        "total"  : len(ALL_OBJECTS)
    })


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  10. Get SQL Source Code for Any Object                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/object-code/<obj_name>", methods=["GET"])
def get_object_code(obj_name):
    obj = ALL_OBJECTS.get(obj_name)
    if not obj:
        return jsonify({"success": False, "error": f"Object '{obj_name}' not found"}), 404
    return jsonify({
        "success"    : True,
        "name"       : obj.get("name", obj_name),
        "description": obj.get("description", ""),
        "object_type": obj.get("object_type", ""),
        "code"       : obj.get("code", "").strip()
    })


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  11. Convert Multiple Objects → Combined HelperFunction Notebook            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/convert-multi", methods=["POST"])
def convert_multi():
    try:
        data         = request.get_json()
        object_names = data.get("object_names", [])

        if not object_names:
            return jsonify({"success": False, "error": "object_names list is required"}), 400

        result = get_combined_pyspark_code(object_names)
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  12. Convert Objects → Separate Files (one per SP/View + HelperFunction)  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/convert-separate", methods=["POST"])
def convert_separate():
    try:
        data         = request.get_json()
        object_names = data.get("object_names", [])
        # objects_with_code: { key -> {type, code} } — sent when objects are
        # loaded from a live source DB (no pre-built template exists)
        objects_with_code = data.get("objects_with_code", {})
        if not object_names:
            return jsonify({"success": False, "error": "object_names list is required"}), 400
        result = get_separate_pyspark_codes(object_names, objects_with_code)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  13. Upload Multiple Notebooks to Databricks                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/databricks/upload-multiple", methods=["POST"])
def upload_multiple_notebooks():
    try:
        data           = request.get_json()
        host           = data.get("host", "").strip()
        token          = data.get("token", "").strip()
        workspace_path = data.get("workspace_path", "/Shared/Migrations").strip()
        notebooks      = data.get("notebooks", [])   # [{name, code}, ...]

        if not all([host, token, notebooks]):
            return jsonify({"success": False, "error": "host, token, and notebooks are required"}), 400

        connector = DatabricksConnector(host, token)
        results   = []
        for nb in notebooks:
            nb_name = nb.get("name", "Notebook")
            nb_code = nb.get("code", "")
            r = connector.upload_notebook(
                notebook_name=nb_name,
                python_code=nb_code,
                path=workspace_path
            )
            results.append({"name": nb_name, "success": r.get("success"), "path": r.get("path"), "error": r.get("error")})

        success_count = sum(1 for r in results if r["success"])
        return jsonify({
            "success": success_count > 0,
            "results": results,
            "uploaded": success_count,
            "total": len(results)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  14. Upload HelperFunction Notebook (raw code)                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/databricks/upload-helper", methods=["POST"])
def upload_helper_notebook():
    try:
        data           = request.get_json()
        host           = data.get("host", "").strip()
        token          = data.get("token", "").strip()
        pyspark_code   = data.get("pyspark_code", "").strip()
        workspace_path = data.get("workspace_path", "/Shared/Migrations").strip()

        if not all([host, token, pyspark_code]):
            return jsonify({"success": False, "error": "host, token, and pyspark_code are required"}), 400

        connector = DatabricksConnector(host, token)
        result    = connector.upload_notebook(
            notebook_name="HelperFunction",
            python_code=pyspark_code,
            path=workspace_path
        )
        result["notebook_name"] = "HelperFunction"
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Helper — build ODBC connection string                                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
def _odbc_escape(val: str) -> str:
    """Escape a value for safe use in an ODBC connection string.
    Wraps in {} braces and doubles any } inside, per ODBC spec.
    Handles passwords with special chars like ; # { } = etc."""
    return "{" + val.replace("}", "}}") + "}"

def _build_sql_conn_str(source_type, server, database, username, password):
    try:
        import pyodbc
        installed = pyodbc.drivers()
    except Exception:
        installed = []
    # Pick best available driver: prefer 18, then 17, then first available
    driver = (
        next((d for d in installed if "ODBC Driver 18 for SQL Server" in d), None) or
        next((d for d in installed if "ODBC Driver 17 for SQL Server" in d), None) or
        next((d for d in installed if "SQL Server" in d), None) or
        "ODBC Driver 17 for SQL Server"
    )
    # Escape password & username with {} braces to handle special chars (# ; { } = etc.)
    safe_pwd  = _odbc_escape(password) if password else ""
    safe_user = _odbc_escape(username) if username else ""
    base = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={safe_user};PWD={safe_pwd}"
    is_v18 = "18" in driver
    if source_type in ("azuresql", "synapse"):
        base += ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
    else:  # sqlserver, sqlmi
        if is_v18:
            base += ";Encrypt=optional;TrustServerCertificate=yes;Connection Timeout=30"
        else:
            base += ";Encrypt=no;TrustServerCertificate=yes;Connection Timeout=30"
    return base


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  15. Source DB — Test Connection                                            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/source/test-connection", methods=["POST"])
def source_test_connection():
    try:
        import pyodbc
    except ImportError:
        return jsonify({"success": False, "error": "pyodbc not installed. Run: pip install pyodbc"}), 500
    try:
        data        = request.get_json()
        source_type = data.get("source_type", "sqlserver")
        server      = data.get("server", "").strip()
        database    = data.get("database", "").strip()
        username    = data.get("username", "").strip()
        password    = data.get("password", "")

        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username are required"}), 400

        conn_str = _build_sql_conn_str(source_type, server, database, username, password)
        conn     = pyodbc.connect(conn_str, timeout=10)
        cursor   = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        row      = cursor.fetchone()
        version  = row[0].split("\n")[0].strip() if row else "Connected"
        conn.close()
        return jsonify({"success": True, "server_version": version})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  16. Source DB — Load SQL Objects (SPs + Views + UDFs)                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/source/load-objects", methods=["POST"])
def source_load_objects():
    try:
        import pyodbc
    except ImportError:
        return jsonify({"success": False, "error": "pyodbc not installed. Run: pip install pyodbc"}), 500
    try:
        data        = request.get_json()
        source_type = data.get("source_type", "sqlserver")
        server      = data.get("server", "").strip()
        database    = data.get("database", "").strip()
        username    = data.get("username", "").strip()
        password    = data.get("password", "")

        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username are required"}), 400

        conn_str = _build_sql_conn_str(source_type, server, database, username, password)
        conn     = pyodbc.connect(conn_str, timeout=15)
        cursor   = conn.cursor()

        grouped = {"stored_procedure": [], "view": [], "udf": []}

        # Stored Procedures
        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
                   name,
                   ISNULL(OBJECT_DEFINITION(object_id), '') AS code
            FROM   sys.procedures
            WHERE  is_ms_shipped = 0
            ORDER  BY name
        """)
        for row in cursor.fetchall():
            grouped["stored_procedure"].append({
                "key": row[0], "name": row[1],
                "description": "Stored procedure", "code": row[2],
                "object_type": "stored_procedure"
            })

        # Views
        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
                   name,
                   ISNULL(OBJECT_DEFINITION(object_id), '') AS code
            FROM   sys.views
            WHERE  is_ms_shipped = 0
            ORDER  BY name
        """)
        for row in cursor.fetchall():
            grouped["view"].append({
                "key": row[0], "name": row[1],
                "description": "SQL View", "code": row[2],
                "object_type": "view"
            })

        # User-Defined Functions
        cursor.execute("""
            SELECT SCHEMA_NAME(schema_id) + '.' + name AS [key],
                   name,
                   ISNULL(OBJECT_DEFINITION(object_id), '') AS code
            FROM   sys.objects
            WHERE  type IN ('FN', 'IF', 'TF')
              AND  is_ms_shipped = 0
            ORDER  BY name
        """)
        for row in cursor.fetchall():
            grouped["udf"].append({
                "key": row[0], "name": row[1],
                "description": "User-defined function", "code": row[2],
                "object_type": "udf"
            })

        conn.close()
        total = sum(len(v) for v in grouped.values())
        return jsonify({"success": True, "grouped": grouped, "total": total,
                        "source_type": source_type, "database": database})

    except Exception as e:
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗# ║  DATA MIGRATION ENDPOINTS                                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@app.route("/api/migrate/list-tables", methods=["POST"])
def migrate_list_tables():
    """List all base tables in the source database."""
    try:
        d            = request.get_json()
        source_type  = d.get("source_type", "sqlserver")
        server       = d.get("server", "").strip()
        database     = d.get("database", "").strip()
        username     = d.get("username", "").strip()
        password     = d.get("password", "")
        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username required"}), 400
        conn_str  = _build_conn_str(source_type, server, database, username, password)
        migrator  = DataMigrator(conn_str, "http://placeholder", "placeholder")
        tables    = migrator.list_source_tables()
        return jsonify({"success": True, "tables": tables, "total": len(tables)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/migrate/describe-table", methods=["POST"])
def migrate_describe_table():
    """Return column schema + row count for a source table."""
    try:
        d           = request.get_json()
        source_type = d.get("source_type", "sqlserver")
        server      = d.get("server", "").strip()
        database    = d.get("database", "").strip()
        username    = d.get("username", "").strip()
        password    = d.get("password", "")
        schema      = d.get("schema", "dbo").strip()
        table       = d.get("table", "").strip()
        conn_str    = _build_conn_str(source_type, server, database, username, password)
        migrator    = DataMigrator(conn_str, "http://placeholder", "placeholder")
        desc        = migrator.describe_source_table(schema, table)
        return jsonify({"success": True, **desc})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/migrate/warehouses", methods=["POST"])
def migrate_list_warehouses():
    """List SQL Warehouses in the target Databricks workspace."""
    try:
        d     = request.get_json()
        host  = d.get("host", "").strip()
        token = d.get("token", "").strip()
        if not host or not token:
            return jsonify({"success": False, "error": "host and token required"}), 400
        uc    = UnityCatalogExecutor(host, token)
        return jsonify(uc.list_warehouses())
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/migrate/start", methods=["POST"])
def migrate_start():
    """Start a background parallel migration job. Returns job_id immediately."""
    import threading, uuid
    try:
        d            = request.get_json()
        source_type  = d.get("source_type", "sqlserver")
        server       = d.get("server", "").strip()
        database     = d.get("database", "").strip()
        username     = d.get("username", "").strip()
        password     = d.get("password", "")
        dbx_host     = (d.get("host") or d.get("dbx_host") or "").strip()
        dbx_token    = (d.get("token") or d.get("dbx_token") or "").strip()
        catalog      = d.get("catalog", "main").strip()
        schema       = d.get("schema", "default").strip()
        warehouse_id = d.get("warehouse_id", "").strip()
        tables       = d.get("tables", [])        # [{schema, table}, ...]
        max_workers  = int(d.get("max_workers", 3))
        load_mode    = d.get("load_mode", "full").strip()   # "full" or "incremental"

        if not all([server, database, username, dbx_host, dbx_token,
                    warehouse_id, tables]):
            return jsonify({"success": False, "error": "Missing required fields"}), 400

        job_id = uuid.uuid4().hex
        MIGRATION_JOBS[job_id] = {
            "status":     "queued",
            "started_at": datetime.now().isoformat(),
            "total":      len(tables),
            "done":       0,
            "failed":     0,
            "results":    [],
            "logs":       {},
            "load_mode":  load_mode,
        }

        conn_str = _build_conn_str(source_type, server, database, username, password)
        migrator = DataMigrator(conn_str, dbx_host, dbx_token, catalog, schema)

        def _bg():
            migrator.migrate_tables_parallel(tables, warehouse_id, job_id,
                                             max_workers, load_mode)

        threading.Thread(target=_bg, daemon=True).start()
        return jsonify({"success": True, "job_id": job_id,
                        "message": f"Migration started for {len(tables)} tables"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e),
                        "trace": traceback.format_exc()}), 500


@app.route("/api/migrate/status/<job_id>", methods=["GET"])
def migrate_status(job_id):
    """Poll migration job status + per-table logs."""
    job = MIGRATION_JOBS.get(job_id)
    if not job:
        return jsonify({"success": False, "error": "Job not found"}), 404
    # Ensure logs is always a flat list (backwards-compat if dict)
    raw_logs = job.get("logs", [])
    if isinstance(raw_logs, dict):
        flat = []
        for tname, lines in raw_logs.items():
            flat.extend(f"[{tname}] {l}" for l in (lines or []))
        raw_logs = flat
    out = {k: v for k, v in job.items() if k != "logs"}
    out["logs"] = raw_logs
    return jsonify({"success": True, **out})


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  MEDALLION ARCHITECTURE ENDPOINTS                                           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@app.route("/api/medallion/generate", methods=["POST"])
def medallion_generate():
    """Generate Medallion notebooks (Landing, Bronze, Silver, Orchestrator)."""
    try:
        d            = request.get_json()
        source_type  = d.get("source_type", "sqlserver")
        server       = d.get("server", "").strip()
        database     = d.get("database", "").strip()
        username     = d.get("username", "").strip()
        tables       = d.get("tables", [])
        catalog      = d.get("catalog", "main").strip()
        schema       = d.get("schema", "default").strip()
        landing_path = d.get("landing_path", "/mnt/landing").strip()
        workspace_path = d.get("workspace_path", "/Shared/Medallion").strip()

        # Multi-catalog support
        volumes_catalog = d.get("volumes_catalog", "").strip()
        bronze_catalog  = d.get("bronze_catalog", "").strip()
        silver_catalog  = d.get("silver_catalog", "").strip()
        target_schema   = d.get("target_schema", "").strip()

        if not tables:
            return jsonify({"success": False, "error": "tables list is required"}), 400

        result = generate_all_medallion_notebooks(
            source_type=source_type,
            server=server,
            database=database,
            username=username,
            tables=tables,
            catalog=catalog,
            schema=schema,
            landing_path=landing_path,
            workspace_path=workspace_path,
            volumes_catalog=volumes_catalog,
            bronze_catalog=bronze_catalog,
            silver_catalog=silver_catalog,
            target_schema=target_schema,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/medallion/deploy", methods=["POST"])
def medallion_deploy():
    """Deploy all Medallion notebooks to Databricks workspace."""
    try:
        d              = request.get_json()
        host           = d.get("host", "").strip()
        token          = d.get("token", "").strip()
        workspace_path = d.get("workspace_path", "/Shared/Medallion").strip()
        notebooks      = d.get("notebooks", [])   # [{name, code}, ...]

        if not all([host, token, notebooks]):
            return jsonify({"success": False, "error": "host, token, and notebooks are required"}), 400

        connector = DatabricksConnector(host, token)
        results   = []
        for nb in notebooks:
            nb_name = nb.get("name", "Notebook")
            nb_code = nb.get("code", "")
            r = connector.upload_notebook(
                notebook_name=nb_name,
                python_code=nb_code,
                path=workspace_path,
            )
            results.append({
                "name": nb_name,
                "success": r.get("success"),
                "path": r.get("notebook_path") or r.get("path"),
                "url": r.get("workspace_url"),
                "error": r.get("error") or r.get("message") if not r.get("success") else None,
            })

        ok = sum(1 for r in results if r["success"])
        return jsonify({
            "success": ok > 0,
            "results": results,
            "uploaded": ok,
            "total": len(results),
            "workspace_path": workspace_path,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/medallion/run-pipeline", methods=["POST"])
def medallion_run_pipeline():
    """Submit the Orchestrator notebook as a one-time Databricks job run."""
    try:
        d              = request.get_json()
        host           = d.get("host", "").strip()
        token          = d.get("token", "").strip()
        workspace_path = d.get("workspace_path", "/Shared/Medallion").strip()
        cluster_id     = d.get("cluster_id", "").strip()
        load_type      = d.get("load_type", "full").strip()
        password       = d.get("password", "")

        if not host or not token:
            return jsonify({"success": False, "error": "host and token are required"}), 400

        import base64
        pwd_b64 = base64.b64encode((password or "").encode("utf-8")).decode("ascii")

        connector = DatabricksConnector(host, token)
        result = connector.run_notebook(
            notebook_path=f"{workspace_path}/00_Orchestrator",
            cluster_id=cluster_id or None,
            params={"load_type": load_type, "password_b64": pwd_b64},
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SELF-HEALING BOT ENDPOINTS                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@app.route("/api/healer/health-check", methods=["POST"])
def healer_health_check():
    """Run comprehensive system health check."""
    try:
        d = request.get_json() or {}
        host  = d.get("host", "").strip()
        token = d.get("token", "").strip()
        connector = DatabricksConnector(host, token) if host and token else None
        source_config = {
            "source_type": d.get("source_type", "sqlserver"),
            "server":      d.get("server", "").strip(),
            "database":    d.get("database", "").strip(),
            "username":    d.get("username", "").strip(),
            "password":    d.get("password", ""),
        } if d.get("server") else None
        result = healer.run_health_check(connector=connector, host=host,
                                          token=token, source_config=source_config)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/healer/diagnose", methods=["POST"])
def healer_diagnose():
    """Diagnose an error message and recommend healing action."""
    try:
        d = request.get_json() or {}
        error_text = d.get("error_text", "").strip()
        context    = d.get("context", {})
        if not error_text:
            return jsonify({"success": False, "error": "error_text is required"}), 400
        result = healer.diagnose_error(error_text, context)
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/healer/heal", methods=["POST"])
def healer_heal():
    """Execute a specific healing action."""
    try:
        d = request.get_json() or {}
        action  = d.get("action", "notify")
        host    = d.get("host", "").strip()
        token   = d.get("token", "").strip()
        context = d.get("context", {})
        connector = DatabricksConnector(host, token) if host and token else None
        result = healer.execute_heal(action, connector=connector, context=context)
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/healer/monitor/start", methods=["POST"])
def healer_monitor_start():
    """Start monitoring a Databricks job run."""
    try:
        d = request.get_json() or {}
        run_id    = d.get("run_id")
        host      = d.get("host", "").strip()
        token     = d.get("token", "").strip()
        auto_heal = d.get("auto_heal", True)
        if not run_id:
            return jsonify({"success": False, "error": "run_id is required"}), 400
        connector = DatabricksConnector(host, token) if host and token else None
        result = healer.start_monitor(int(run_id), connector=connector, auto_heal=auto_heal)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/healer/monitor/check/<monitor_id>", methods=["POST"])
def healer_monitor_check(monitor_id):
    """Check and update monitor status."""
    try:
        d = request.get_json() or {}
        host  = d.get("host", "").strip()
        token = d.get("token", "").strip()
        connector = DatabricksConnector(host, token) if host and token else None
        result = healer.check_monitor(monitor_id, connector=connector)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/healer/monitors", methods=["GET"])
def healer_list_monitors():
    return jsonify({"success": True, "monitors": healer.list_monitors()})


@app.route("/api/healer/monitor/stop/<monitor_id>", methods=["POST"])
def healer_monitor_stop(monitor_id):
    return jsonify(healer.stop_monitor(monitor_id))


@app.route("/api/healer/restore-point", methods=["POST"])
def healer_create_restore_point():
    """Create a named restore point."""
    try:
        d = request.get_json() or {}
        key      = d.get("key", f"rp_{int(time.time())}")
        metadata = d.get("metadata", {})
        result = healer.create_restore_point(key, metadata)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/healer/restore-points", methods=["GET"])
def healer_list_restore_points():
    return jsonify({"success": True, "restore_points": healer.list_restore_points()})


@app.route("/api/healer/restore-point/<key>", methods=["DELETE"])
def healer_delete_restore_point(key):
    return jsonify(healer.delete_restore_point(key))


@app.route("/api/healer/rules", methods=["GET"])
def healer_get_rules():
    return jsonify({"success": True, "rules": healer.get_rules()})


@app.route("/api/healer/rules/toggle", methods=["POST"])
def healer_toggle_rule():
    d = request.get_json() or {}
    return jsonify(healer.toggle_rule(d.get("rule_id"), d.get("enabled", True)))


@app.route("/api/healer/rules/add", methods=["POST"])
def healer_add_rule():
    d = request.get_json() or {}
    return jsonify(healer.add_rule(
        name=d.get("name", "Custom Rule"),
        category=d.get("category", "GENERIC_ERROR"),
        action=d.get("action", "retry"),
        max_retries=d.get("max_retries", 3),
        description=d.get("description", ""),
    ))


@app.route("/api/healer/history", methods=["GET"])
def healer_history():
    limit    = request.args.get("limit", 50, type=int)
    severity = request.args.get("severity", None)
    return jsonify({"success": True, "history": healer.get_history(limit, severity)})


@app.route("/api/healer/history/clear", methods=["POST"])
def healer_clear_history():
    return jsonify(healer.clear_history())


@app.route("/api/healer/stats", methods=["GET"])
def healer_stats():
    return jsonify({"success": True, **healer.get_stats()})


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  AI Integration — Workflow Manager                                           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# ── MetadataFlow — Initialise Delta tables in Databricks ──
@app.route("/api/workflow/metadata/init", methods=["POST"])
def wf_metadata_init():
    d = request.get_json() or {}
    return jsonify(wfm.init_metadata_flow(
        host         = d.get("host", "").strip(),
        token        = d.get("token", "").strip(),
        catalog      = d.get("catalog", "main").strip(),
        schema       = d.get("schema", "default").strip(),
        warehouse_id = d.get("warehouse_id", "").strip(),
    ))

# ── MetadataFlow — Check metadata status ──
@app.route("/api/workflow/metadata/status", methods=["GET"])
def wf_metadata_status():
    return jsonify(wfm.get_metadata_status())

# ── MetadataFlow — Load metadata from Databricks into memory ──
@app.route("/api/workflow/metadata/load", methods=["POST"])
def wf_metadata_load():
    return jsonify(wfm.load_metadata_from_dbr())

# ── MetadataFlow — Full sync in-memory → Databricks ──
@app.route("/api/workflow/metadata/sync", methods=["POST"])
def wf_metadata_sync():
    return jsonify(wfm.full_sync_to_dbr())

# ── MetadataFlow — Save discovered source tables to Databricks ──
@app.route("/api/workflow/metadata/save-sources", methods=["POST"])
def wf_metadata_save_sources():
    d = request.get_json() or {}
    return jsonify(wfm.sync_source_tables_to_dbr(
        tables        = d.get("tables", []),
        source_config = d.get("source_config", {}),
    ))

# ── MetadataFlow — Deploy metadata-driven notebooks to Databricks ──
@app.route("/api/workflow/notebooks/deploy", methods=["POST"])
def wf_deploy_notebooks():
    d = request.get_json() or {}
    return jsonify(wfm.deploy_metadata_notebooks(
        host           = d.get("host", "").strip(),
        token          = d.get("token", "").strip(),
        catalog        = d.get("catalog", "main").strip(),
        schema         = d.get("schema", "default").strip(),
        landing_path   = d.get("landing_path", "/mnt/landing").strip(),
        workspace_path = d.get("workspace_path", "/Shared/MetadataPipeline").strip(),
        pipeline_mode  = d.get("pipeline_mode", "standard").strip(),
    ))

# ── MetadataFlow — Check notebook deployment status ──
@app.route("/api/workflow/notebooks/status", methods=["GET"])
def wf_notebook_status():
    return jsonify(wfm.get_notebook_status())

# ── MetadataFlow — Data Quality checks catalog ──
@app.route("/api/workflow/dq-checks", methods=["GET"])
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

# ── MetadataFlow — Run pipeline on Databricks (real execution) ──
@app.route("/api/workflow/pipelines/<group_id>/run-databricks", methods=["POST"])
def wf_run_on_databricks(group_id):
    d = request.get_json() or {}
    result = wfm.run_pipeline_on_databricks(
        group_id       = group_id,
        host           = d.get("host", "").strip(),
        token          = d.get("token", "").strip(),
        cluster_id     = d.get("cluster_id", "").strip(),
        load_type      = d.get("load_type", "").strip(),
        password       = d.get("password", ""),
        workspace_path = d.get("workspace_path", "").strip(),
        catalog        = d.get("catalog", "").strip(),
        schema         = d.get("schema", "").strip(),
        landing_path   = d.get("landing_path", "/mnt/landing").strip(),
    )
    if not result.get("success"):
        print(f"[WARN] run-databricks failed for group '{group_id}': {result.get('error') or result.get('message')}")
    return jsonify(result)

# ── Dashboard Stats ──
@app.route("/api/workflow/stats", methods=["GET"])
def wf_stats():
    return jsonify(wfm.get_dashboard_stats())

# ── MetadataFlow — List Databricks clusters ──
@app.route("/api/workflow/clusters", methods=["GET"])
def wf_list_clusters():
    host  = request.args.get("host", "").strip()
    token = request.args.get("token", "").strip()
    if not host or not token:
        return jsonify({"success": False, "error": "host and token required"})
    try:
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        result = connector.list_clusters()
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ── Start a Databricks cluster ──
@app.route("/api/workflow/clusters/start", methods=["POST"])
def wf_start_cluster():
    d = request.get_json() or {}
    host  = d.get("host", "").strip()
    token = d.get("token", "").strip()
    cluster_id = d.get("cluster_id", "").strip()
    if not host or not token or not cluster_id:
        return jsonify({"success": False, "error": "host, token, and cluster_id required"})
    try:
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        result = connector.start_cluster(cluster_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ── Create Pipeline (3 jobs for a table) ──
@app.route("/api/workflow/create-pipeline", methods=["POST"])
def wf_create_pipeline():
    d = request.get_json() or {}
    return jsonify(wfm.create_pipeline_for_table(
        table_schema    = d.get("table_schema", "dbo"),
        table_name      = d.get("table_name", ""),
        load_type       = d.get("load_type", "full"),
        watermark_column= d.get("watermark_column", ""),
        source_config   = d.get("source_config"),
        target_config   = d.get("target_config"),
    ))

# ── Bulk Create Pipelines ──
@app.route("/api/workflow/create-pipelines-bulk", methods=["POST"])
def wf_create_pipelines_bulk():
    d = request.get_json() or {}
    return jsonify(wfm.create_pipelines_bulk(
        tables         = d.get("tables", []),
        source_config  = d.get("source_config"),
        target_config  = d.get("target_config"),
    ))

# ── List Pipeline Groups ──
@app.route("/api/workflow/pipelines", methods=["GET"])
def wf_list_pipelines():
    return jsonify(wfm.list_pipeline_groups())

# ── List Jobs ──
@app.route("/api/workflow/jobs", methods=["GET"])
def wf_list_jobs():
    return jsonify(wfm.list_jobs(
        group_id = request.args.get("group_id"),
        stage    = request.args.get("stage"),
        status   = request.args.get("status"),
    ))

# ── Get Single Job ──
@app.route("/api/workflow/jobs/<job_id>", methods=["GET"])
def wf_get_job(job_id):
    return jsonify(wfm.get_job(job_id))

# ── Update Job ──
@app.route("/api/workflow/jobs/<job_id>", methods=["PUT"])
def wf_update_job(job_id):
    d = request.get_json() or {}
    return jsonify(wfm.update_job(job_id, d))

# ── Delete Job ──
@app.route("/api/workflow/jobs/<job_id>", methods=["DELETE"])
def wf_delete_job(job_id):
    return jsonify(wfm.delete_job(job_id))

# ── Delete Entire Pipeline Group ──
@app.route("/api/workflow/pipelines/<group_id>", methods=["DELETE"])
def wf_delete_pipeline(group_id):
    return jsonify(wfm.delete_pipeline_group(group_id))

# ── Run a Single Job ──
@app.route("/api/workflow/jobs/<job_id>/run", methods=["POST"])
def wf_run_job(job_id):
    d = request.get_json() or {}
    return jsonify(wfm.run_job(job_id, force_full=d.get("force_full", False)))

# ── Run Entire Pipeline Group ──
@app.route("/api/workflow/pipelines/<group_id>/run", methods=["POST"])
def wf_run_pipeline(group_id):
    d = request.get_json() or {}
    return jsonify(wfm.run_pipeline_group(group_id, force_full=d.get("force_full", False)))

# ── Rerun From Failure ──
@app.route("/api/workflow/pipelines/<group_id>/rerun", methods=["POST"])
def wf_rerun_pipeline(group_id):
    return jsonify(wfm.rerun_from_failure(group_id))

# ── Get Run Status ──
@app.route("/api/workflow/runs/<run_id>", methods=["GET"])
def wf_get_run(run_id):
    return jsonify(wfm.get_run_status(run_id))

# ── Get Databricks Run Output (on-demand) ──
@app.route("/api/workflow/runs/<run_id>/databricks-output", methods=["POST"])
def wf_get_dbr_output(run_id):
    """Fetch notebook output from Databricks for a run that has a dbr_run_id."""
    body = request.get_json(force=True)
    host  = (body.get("host") or "").strip()
    token = (body.get("token") or "").strip()
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
    result = conn.get_run_output(int(dbr_run_id))
    return jsonify(result)

# ── List Runs ──
@app.route("/api/workflow/runs", methods=["GET"])
def wf_list_runs():
    return jsonify(wfm.list_runs(
        job_id   = request.args.get("job_id"),
        group_id = request.args.get("group_id"),
        status   = request.args.get("status"),
        limit    = request.args.get("limit", 50, type=int),
    ))

# ── Add Custom Job ──
@app.route("/api/workflow/jobs/add", methods=["POST"])
def wf_add_custom_job():
    d = request.get_json() or {}
    return jsonify(wfm.add_custom_job(
        job_name        = d.get("job_name", ""),
        stage           = d.get("stage", "extract"),
        table_schema    = d.get("table_schema", "dbo"),
        table_name      = d.get("table_name", ""),
        load_type       = d.get("load_type", "full"),
        watermark_column= d.get("watermark_column", ""),
        group_id        = d.get("group_id"),
    ))

# ── Watermarks ──
@app.route("/api/workflow/watermarks", methods=["GET"])
def wf_watermarks():
    return jsonify(wfm.get_watermarks())

@app.route("/api/workflow/watermarks/update", methods=["POST"])
def wf_update_watermark():
    d = request.get_json() or {}
    return jsonify(wfm.update_watermark(d.get("table"), d.get("column"), d.get("value")))

@app.route("/api/workflow/watermarks/reset", methods=["POST"])
def wf_reset_watermark():
    d = request.get_json() or {}
    return jsonify(wfm.reset_watermark(d.get("table")))


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Deploy Config — Settings (deployconfig.json)                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
DEPLOY_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "deployconfig.json")

@app.route("/api/deploy-config", methods=["GET"])
def get_deploy_config():
    if not os.path.isfile(DEPLOY_CONFIG_PATH):
        return jsonify({"success": True, "config": None, "message": "No config file found"})
    try:
        with open(DEPLOY_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return jsonify({"success": True, "config": cfg})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/deploy-config", methods=["POST"])
def save_deploy_config():
    try:
        cfg = request.get_json()
        if not cfg:
            return jsonify({"success": False, "error": "No configuration data provided"}), 400
        with open(DEPLOY_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        return jsonify({"success": True, "message": "Configuration saved to deployconfig.json"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Deploy Infrastructure — runs AutoInfraCreation                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/deploy-infra", methods=["POST"])
def deploy_infrastructure():
    """Read deployconfig.json → run infra setup. All config read from file."""
    try:
        from AutoInfraCreation import run_all_api

        # Load saved config
        if not os.path.isfile(DEPLOY_CONFIG_PATH):
            return jsonify({"success": False, "error": "No deployconfig.json found. Save settings first."}), 400

        with open(DEPLOY_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        # Map UI field names → AutoInfraCreation field names
        cfg.setdefault("access_connector", cfg.get("access_connector", ""))
        cfg.setdefault("external_locations", {})
        cfg.setdefault("catalogs", {})
        cfg.setdefault("folders", [])

        result = run_all_api(cfg)
        return jsonify(result)

    except Exception as e:
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Deploy Infrastructure — SSE Streaming (real-time progress)                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
@app.route("/api/deploy-infra-stream")
def deploy_infrastructure_stream():
    """SSE endpoint — streams step-by-step infrastructure deployment progress.

    All configuration (including Databricks creds) is read from deployconfig.json.
    """
    import importlib
    import AutoInfraCreation
    importlib.reload(AutoInfraCreation)          # pick up any code changes
    from AutoInfraCreation import run_all_streaming

    if not os.path.isfile(DEPLOY_CONFIG_PATH):
        def _err():
            yield 'data: ' + json.dumps({"event":"done","success":False,"summary":"No deployconfig.json found. Save settings first."}) + '\n\n'
        return Response(_err(), mimetype='text/event-stream')

    with open(DEPLOY_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    cfg.setdefault("access_connector", cfg.get("access_connector", ""))
    cfg.setdefault("external_locations", {})
    cfg.setdefault("catalogs", {})
    cfg.setdefault("folders", [])

    def generate():
        try:
            for evt in run_all_streaming(cfg):
                yield 'data: ' + json.dumps(evt) + '\n\n'
        except Exception as e:
            yield 'data: ' + json.dumps({"event":"done","success":False,"summary":str(e)[:500]}) + '\n\n'

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Run Server                                                                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
if __name__ == "__main__":
    print("\n" + "="*65)
    print("  SQL -> Databricks Migration Utility")
    print("  URL : http://localhost:5000")
    print("="*65 + "\n")
    app.run(host="0.0.0.0", port=5000, debug=True)
