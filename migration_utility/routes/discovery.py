"""Discovery blueprint — scan, analyse, export SQL object inventory."""
from flask import Blueprint, request, jsonify, Response

from .auth import login_required
from log_config import get_logger
import discovery_agent as da
import data_profiler as dp

logger = get_logger(__name__)
discovery_bp = Blueprint("discovery", __name__, url_prefix="/api/v1")

# ── In-memory scan cache ──
_discovery_cache = {}  # { report: {...}, analyses: [...], graph: {...} }
_profile_cache = {}    # { table_name: profile_dict }


@discovery_bp.route("/discovery/scan", methods=["POST"])
@login_required
def discovery_scan():
    """Run a discovery scan against live DB, static objects, or both."""
    global _discovery_cache
    try:
        data = request.get_json(silent=True) or {}
        source = data.get("source", "static")  # "live", "static", "both"
        source_config = data.get("source_config", {})

        analyses = []

        if source in ("static", "both"):
            analyses.extend(da.scan_static_objects())

        if source in ("live", "both"):
            required = ("server", "database", "username")
            if not all(source_config.get(k) for k in required):
                return jsonify({"success": False, "error": "server, database, username required for live scan"}), 400
            live = da.scan_live_source(source_config)
            # Merge: avoid duplicates by name
            existing = {a["name"].lower() for a in analyses}
            for a in live:
                if a["name"].lower() not in existing:
                    analyses.append(a)
                    existing.add(a["name"].lower())

        # Sort by complexity score descending
        analyses.sort(key=lambda a: a["complexity_score"], reverse=True)

        graph = da.build_dependency_graph(analyses)
        report = da.generate_discovery_report(analyses, graph)

        _discovery_cache = {
            "report": report,
            "analyses": analyses,
            "graph": graph,
        }

        return jsonify({"success": True, "report": report})

    except Exception as e:
        logger.exception("Discovery scan failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/results", methods=["GET"])
@login_required
def discovery_results():
    """Return cached scan results."""
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results. Run a scan first."})
    return jsonify({"success": True, "report": _discovery_cache["report"]})


@discovery_bp.route("/discovery/object/<name>", methods=["GET"])
@login_required
def discovery_object(name):
    """Return detailed analysis for a single object."""
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    for a in _discovery_cache.get("analyses", []):
        if a["name"] == name:
            return jsonify({"success": True, "object": a})
    return jsonify({"success": False, "error": f"Object '{name}' not found"}), 404


@discovery_bp.route("/discovery/dependency-graph", methods=["GET"])
@login_required
def discovery_graph():
    """Return dependency graph JSON for D3.js."""
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    return jsonify({"success": True, "graph": _discovery_cache.get("graph", {})})


@discovery_bp.route("/discovery/export/html", methods=["GET"])
@login_required
def discovery_export_html():
    """Download self-contained HTML report."""
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    html = da.generate_html_report(_discovery_cache["report"])
    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": "attachment; filename=discovery_report.html"},
    )


@discovery_bp.route("/discovery/export/bom", methods=["GET"])
@login_required
def discovery_export_bom():
    """Download BOM as CSV."""
    if not _discovery_cache:
        return jsonify({"success": False, "error": "No scan results."})
    csv_str = da.generate_bom_csv(_discovery_cache.get("analyses", []))
    return Response(
        csv_str,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=discovery_bom.csv"},
    )


# ─────────────────────────────────────────────────────────────────────────────
#  DATA PROFILE endpoints
# ─────────────────────────────────────────────────────────────────────────────
@discovery_bp.route("/discovery/profile/tables", methods=["POST"])
@login_required
def discovery_profile_tables():
    """List tables that can be profiled (demo or live)."""
    try:
        data = request.get_json(silent=True) or {}
        mode = data.get("mode", "demo")
        src_cfg = data.get("source_config", {}) or {}
        tables = dp.list_profilable_tables(source_config=src_cfg, mode=mode)
        return jsonify({"success": True, "tables": tables, "mode": mode})
    except Exception as e:
        logger.exception("Profile table list failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/profile/<table>", methods=["POST"])
@login_required
def discovery_profile_table(table):
    """Return column-level profile for a single table."""
    try:
        data = request.get_json(silent=True) or {}
        mode = data.get("mode", "demo")
        src_cfg = data.get("source_config", {}) or {}

        if mode == "live":
            required = ("server", "database")
            if not all(src_cfg.get(k) for k in required):
                return jsonify({"success": False, "error": "server and database required for live profile"}), 400
            schema = data.get("schema", "dbo")
            prof = dp.profile_table_live(src_cfg, table, schema=schema)
        else:
            prof = dp.profile_table_demo(table)
            if not prof:
                return jsonify({"success": False, "error": f"Table '{table}' not in demo set"}), 404

        _profile_cache[table] = prof
        return jsonify({"success": True, "profile": prof})
    except Exception as e:
        logger.exception("Profile failed")
        return jsonify({"success": False, "error": str(e)}), 500


@discovery_bp.route("/discovery/profile/<table>/rules", methods=["GET"])
@login_required
def discovery_profile_rules(table):
    """Return the flattened list of suggested DQ rules for a profiled table."""
    prof = _profile_cache.get(table)
    if not prof:
        return jsonify({"success": False, "error": "No profile cached — profile the table first"}), 404
    rules = []
    for col in prof.get("columns", []):
        rules.extend(col.get("suggested_rules", []))
    return jsonify({"success": True, "rules": rules, "table": table})
