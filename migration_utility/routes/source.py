"""Source DB blueprint — test connection, load SQL objects."""
from flask import Blueprint, request, jsonify

from .auth import login_required
from log_config import get_logger
from sql_pool import get_connection

logger = get_logger(__name__)
source_bp = Blueprint("source", __name__, url_prefix="/api/v1")


@source_bp.route("/source/test-connection", methods=["POST"])
@login_required
def source_test_connection():
    try:
        data = request.get_json(silent=True) or {}
        source_type = data.get("source_type", "sqlserver")
        server = (data.get("server") or "").strip()
        database = (data.get("database") or "").strip()
        username = (data.get("username") or "").strip()
        password = data.get("password", "")
        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username are required"}), 400
        try:
            conn = get_connection(source_type, server, database, username, password, timeout=10)
        except Exception as ce:
            msg = str(ce)
            hint = ""
            low = msg.lower()
            if "im002" in low or "data source name" in low or "driver" in low:
                hint = " — ODBC Driver 17/18 for SQL Server is not installed on this machine."
            elif "login failed" in low or "18456" in low:
                hint = " — Username/password rejected by SQL Server."
            elif "timeout" in low or "08001" in low or "could not open" in low:
                hint = " — Cannot reach server. Check firewall, server name, and that your IP is allow-listed in Azure SQL."
            elif "tls" in low or "ssl" in low or "certificate" in low:
                hint = " — TLS/SSL handshake failed. Try Encrypt=yes;TrustServerCertificate=yes."
            logger.error("Source connection failed: %s", msg)
            return jsonify({"success": False, "error": msg + hint}), 200
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            version = row[0].split("\n")[0].strip() if row else "Connected"
        finally:
            try: conn.close()
            except Exception: pass
        return jsonify({"success": True, "server_version": version})
    except Exception as e:
        logger.exception("Unhandled error in test-connection")
        return jsonify({"success": False, "error": "Unexpected server error: " + str(e)}), 200


@source_bp.route("/source/load-objects", methods=["POST"])
@login_required
def source_load_objects():
    try:
        data = request.get_json()
        source_type = data.get("source_type", "sqlserver")
        server = data.get("server", "").strip()
        database = data.get("database", "").strip()
        username = data.get("username", "").strip()
        password = data.get("password", "")
        if not all([server, database, username]):
            return jsonify({"success": False, "error": "server, database and username are required"}), 400
        conn = get_connection(source_type, server, database, username, password)
        cursor = conn.cursor()
        grouped = {"stored_procedure": [], "view": [], "udf": []}

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
        logger.exception("Failed to load source objects")
        return jsonify({"success": False, "error": str(e)}), 500
