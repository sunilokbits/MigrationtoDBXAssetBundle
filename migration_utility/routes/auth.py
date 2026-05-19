"""Auth blueprint — login, logout, session check."""
from flask import Blueprint, request, jsonify, session, redirect, url_for
from functools import wraps
import os, json
from werkzeug.security import check_password_hash

auth_bp = Blueprint("auth", __name__)

_USERS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "users.json")
# Fallback: check same directory as app.py
if not os.path.isfile(_USERS_FILE):
    _USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "users.json")
if not os.path.isfile(_USERS_FILE):
    _USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.json")


def _load_users():
    with open(_USERS_FILE, encoding="utf-8") as f:
        return json.load(f)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            # API endpoints expect JSON, not an HTML redirect
            if request.path.startswith("/api/"):
                return jsonify({"success": False, "error": "Session expired — please log in again."}), 401
            return redirect(url_for("auth.login_page"))
        return f(*args, **kwargs)
    return decorated


@auth_bp.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "GET":
        if "user" in session:
            return redirect(url_for("pages.index"))
        from .pages import _serve
        return _serve("login.html")
    data = request.get_json(silent=True) or {}
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    users = _load_users()
    user = users.get(username)
    if not user or not check_password_hash(user["password"], password):
        return jsonify({"success": False, "error": "Invalid username or password"}), 401
    session["user"] = username
    session["role"] = user["role"]
    session["display_name"] = user["display_name"]
    return jsonify({"success": True, "role": user["role"], "display_name": user["display_name"]})


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login_page"))


@auth_bp.route("/api/v1/auth/me")
@login_required
def auth_me():
    return jsonify({
        "user": session["user"],
        "role": session["role"],
        "display_name": session["display_name"]
    })
