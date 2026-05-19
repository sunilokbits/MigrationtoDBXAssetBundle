"""Admin blueprint — user management & role-based access control."""
from flask import Blueprint, request, jsonify, session
import json, os, re, threading

from .auth import login_required
from log_config import get_logger
from werkzeug.security import generate_password_hash

logger = get_logger(__name__)
admin_bp = Blueprint("admin", __name__, url_prefix="/api/v1/admin")

_USERS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "users.json"
)
_lock = threading.Lock()

VALID_ROLES = ("Admin", "Developer", "Viewer")
_USERNAME_RE = re.compile(r"^[a-z0-9_.\-]{2,32}$")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _admin_required(f):
    """Decorator: logged-in user must have Admin role."""
    from functools import wraps

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return jsonify({"success": False, "error": "Not authenticated"}), 401
        if session.get("role") != "Admin":
            return jsonify({"success": False, "error": "Admin access required"}), 403
        return f(*args, **kwargs)

    return decorated


def _load_users() -> dict:
    with open(_USERS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_users(users: dict):
    with open(_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)


# ── List users (no passwords) ────────────────────────────────────────────────
@admin_bp.route("/users", methods=["GET"])
@login_required
@_admin_required
def list_users():
    users = _load_users()
    result = []
    for uname, udata in users.items():
        result.append({
            "username": uname,
            "display_name": udata.get("display_name", uname),
            "role": udata.get("role", "Viewer"),
        })
    return jsonify({"success": True, "users": result})


# ── Create user ───────────────────────────────────────────────────────────────
@admin_bp.route("/users", methods=["POST"])
@login_required
@_admin_required
def create_user():
    data = request.get_json(silent=True) or {}
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    display_name = data.get("display_name", "").strip()
    role = data.get("role", "Viewer")

    if not username or not _USERNAME_RE.match(username):
        return jsonify({"success": False, "error": "Username must be 2-32 chars: lowercase letters, digits, _ . -"}), 400
    if not password or len(password) < 6:
        return jsonify({"success": False, "error": "Password must be at least 6 characters"}), 400
    if role not in VALID_ROLES:
        return jsonify({"success": False, "error": f"Role must be one of: {', '.join(VALID_ROLES)}"}), 400
    if not display_name:
        display_name = username.title()

    with _lock:
        users = _load_users()
        if username in users:
            return jsonify({"success": False, "error": f"User '{username}' already exists"}), 409
        users[username] = {
            "password": generate_password_hash(password),
            "role": role,
            "display_name": display_name,
        }
        _save_users(users)

    logger.info("User '%s' created with role '%s' by admin '%s'", username, role, session.get("user"))
    return jsonify({"success": True, "username": username, "role": role, "display_name": display_name}), 201


# ── Update user (role / display_name / password) ─────────────────────────────
@admin_bp.route("/users/<username>", methods=["PUT"])
@login_required
@_admin_required
def update_user(username):
    username = username.strip().lower()
    data = request.get_json(silent=True) or {}

    with _lock:
        users = _load_users()
        if username not in users:
            return jsonify({"success": False, "error": f"User '{username}' not found"}), 404

        # Prevent demoting the last admin
        new_role = data.get("role")
        if new_role and new_role not in VALID_ROLES:
            return jsonify({"success": False, "error": f"Role must be one of: {', '.join(VALID_ROLES)}"}), 400
        if new_role and new_role != "Admin" and users[username]["role"] == "Admin":
            admin_count = sum(1 for u in users.values() if u["role"] == "Admin")
            if admin_count <= 1:
                return jsonify({"success": False, "error": "Cannot remove the last Admin. Promote another user first."}), 400

        if new_role:
            users[username]["role"] = new_role
        if "display_name" in data and data["display_name"].strip():
            users[username]["display_name"] = data["display_name"].strip()
        if "password" in data and data["password"]:
            if len(data["password"]) < 6:
                return jsonify({"success": False, "error": "Password must be at least 6 characters"}), 400
            users[username]["password"] = generate_password_hash(data["password"])

        _save_users(users)

    logger.info("User '%s' updated by admin '%s'", username, session.get("user"))
    return jsonify({"success": True, "username": username, "role": users[username]["role"],
                     "display_name": users[username]["display_name"]})


# ── Delete user ───────────────────────────────────────────────────────────────
@admin_bp.route("/users/<username>", methods=["DELETE"])
@login_required
@_admin_required
def delete_user(username):
    username = username.strip().lower()

    with _lock:
        users = _load_users()
        if username not in users:
            return jsonify({"success": False, "error": f"User '{username}' not found"}), 404

        # Prevent deleting yourself
        if username == session.get("user"):
            return jsonify({"success": False, "error": "Cannot delete your own account"}), 400

        # Prevent deleting the last admin
        if users[username]["role"] == "Admin":
            admin_count = sum(1 for u in users.values() if u["role"] == "Admin")
            if admin_count <= 1:
                return jsonify({"success": False, "error": "Cannot delete the last Admin account"}), 400

        del users[username]
        _save_users(users)

    logger.info("User '%s' deleted by admin '%s'", username, session.get("user"))
    return jsonify({"success": True})


@admin_bp.route("/roles", methods=["GET"])
@login_required
@_admin_required
def list_roles():
    return jsonify({
        "success": True,
        "roles": [
            {"name": "Admin", "description": "Full access including user management"},
            {"name": "Developer", "description": "Access to migration and development tools"},
            {"name": "Viewer", "description": "Read-only access to dashboards and reports"},
        ],
    })


@admin_bp.route("/users/<username>/reset-password", methods=["POST"])
@login_required
@_admin_required
def reset_password(username):
    username = username.strip().lower()
    data = request.get_json(silent=True) or {}
    new_pw = data.get("new_password", "")
    if len(new_pw) < 6:
        return jsonify({"success": False, "error": "Password must be at least 6 characters"}), 400

    with _lock:
        users = _load_users()
        if username not in users:
            return jsonify({"success": False, "error": f"User '{username}' not found"}), 404
        users[username]["password"] = generate_password_hash(new_pw)
        _save_users(users)

    logger.info("Password reset for '%s' by admin '%s'", username, session.get("user"))
    return jsonify({"success": True})
