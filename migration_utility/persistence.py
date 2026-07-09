"""
SQLite persistence layer for in-memory state that was previously lost on restart.
Stores: MIGRATION_JOBS, _DM_MODELS (data-modeling cache).
"""
import sqlite3, json, os, threading

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Use persistent /home on Azure App Service; fall back to app dir locally
_PERSISTENT_DIR = "/home/migration_data" if os.path.isdir("/home") and os.access("/home", os.W_OK) else _BASE_DIR
os.makedirs(_PERSISTENT_DIR, exist_ok=True)
_DB_PATH = os.path.join(_PERSISTENT_DIR, "migration_state.db")
_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Return a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS migration_jobs (
            job_id   TEXT PRIMARY KEY,
            payload  TEXT NOT NULL,
            updated  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS dm_models (
            model_id TEXT PRIMARY KEY,
            payload  TEXT NOT NULL,
            updated  TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


# ── Migration Jobs ────────────────────────────────────────────────────────────

def save_job(job_id: str, data: dict):
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO migration_jobs (job_id, payload, updated) VALUES (?, ?, datetime('now'))",
        (job_id, json.dumps(data, default=str)),
    )
    conn.commit()


def load_job(job_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT payload FROM migration_jobs WHERE job_id = ?", (job_id,)).fetchone()
    return json.loads(row[0]) if row else None


def load_all_jobs() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT job_id, payload FROM migration_jobs").fetchall()
    return {r[0]: json.loads(r[1]) for r in rows}


def delete_job(job_id: str):
    conn = _get_conn()
    conn.execute("DELETE FROM migration_jobs WHERE job_id = ?", (job_id,))
    conn.commit()


# ── Data Models ───────────────────────────────────────────────────────────────

def save_model(model_id: str, data: dict):
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO dm_models (model_id, payload, updated) VALUES (?, ?, datetime('now'))",
        (model_id, json.dumps(data, default=str)),
    )
    conn.commit()


def load_model(model_id: str) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT payload FROM dm_models WHERE model_id = ?", (model_id,)).fetchone()
    return json.loads(row[0]) if row else None


def load_all_models() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT model_id, payload FROM dm_models").fetchall()
    return {r[0]: json.loads(r[1]) for r in rows}
