"""
Flask Backend — SP Migration Utility (thin orchestrator)
All route logic lives in routes/*.py blueprints.
"""

import os, sys

# Ensure sibling modules are importable regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, redirect, request, jsonify
from flask_compress import Compress
from log_config import setup_logging, get_logger

# Initialise structured logging before anything else
setup_logging()
logger = get_logger(__name__)

# ── Blueprints ────────────────────────────────────────────────────────────────
from routes.auth       import auth_bp
from routes.pages      import pages_bp
from routes.convert    import convert_bp
from routes.databricks import databricks_bp
from routes.source     import source_bp
from routes.healer     import healer_bp
from routes.workflow   import workflow_bp
from routes.scheduler  import scheduler_bp, start_scheduler
from routes.reports    import reports_bp
from routes.schema     import schema_bp
from routes.settings   import settings_bp
from routes.datamodel  import datamodel_bp
from routes.admin      import admin_bp
from routes.discovery  import discovery_bp
from persistence       import init_db

# ── App factory ───────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["COMPRESS_MIMETYPES"] = [
    "text/html", "text/css", "text/javascript",
    "application/javascript", "application/json",
]
app.config["COMPRESS_MIN_SIZE"] = 512
Compress(app)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "migration-studio-secret-change-me")

# Register all blueprints
app.register_blueprint(auth_bp)         # /login, /logout, /api/v1/auth/*
app.register_blueprint(pages_bp)        # /, /help, /bom
app.register_blueprint(convert_bp)      # /api/v1/stored-procedures, /api/v1/convert, ...
app.register_blueprint(databricks_bp)   # /api/v1/databricks/*, /api/v1/uc/*, /api/v1/unity-catalog/*
app.register_blueprint(source_bp)       # /api/v1/source/*
app.register_blueprint(healer_bp)       # /api/v1/healer/*
app.register_blueprint(workflow_bp)     # /api/v1/workflow/*
app.register_blueprint(scheduler_bp)    # /api/v1/scheduler/*
app.register_blueprint(reports_bp)      # /api/v1/reports/*, /api/v1/audit/*, /api/v1/dq/*
app.register_blueprint(schema_bp)       # /api/v1/schema/*, /api/v1/recon/*
app.register_blueprint(settings_bp)     # /api/v1/deploy-config, /api/v1/deploy-infra, ...
app.register_blueprint(datamodel_bp)    # /api/v1/datamodel/*
app.register_blueprint(admin_bp)        # /api/v1/admin/users
app.register_blueprint(discovery_bp)    # /api/v1/discovery/*

# ── Backward-compatible redirect: /api/* → /api/v1/* ─────────────────────────
@app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def api_compat_redirect(subpath):
    """Redirect old /api/* calls to /api/v1/* so existing clients don't break."""
    dest = f"/api/v1/{subpath}"
    if request.query_string:
        dest += f"?{request.query_string.decode()}"
    return redirect(dest, code=307)

# Initialise SQLite persistence on startup
init_db()

# ── Static asset caching ──────────────────────────────────────────────────────
@app.after_request
def add_cache_headers(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

# ── Global error handlers ─────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"success": False, "error": "Resource not found"}), 404

@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"success": False, "error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal_error(e):
    logger.exception("Unhandled 500 error")
    return jsonify({"success": False, "error": "Internal server error"}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.exception("Unhandled exception: %s", e)
    return jsonify({"success": False, "error": "Internal server error"}), 500


# ============================================================================
#  Run Server
# ============================================================================
if __name__ == "__main__":
    logger.info("=" * 65)
    logger.info("  SQL -> Databricks Migration Utility")
    logger.info("  URL : http://localhost:5000")
    logger.info("=" * 65)
    # Start background scheduler only once:
    # In debug mode, Flask's reloader spawns a child process (WERKZEUG_RUN_MAIN=true).
    # We only start the scheduler in that child to avoid double-execution.
    # In production (debug=False), WERKZEUG_RUN_MAIN is never set, so we always start.
    app.debug = True
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=True)
