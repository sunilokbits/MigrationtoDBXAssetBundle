"""Shared SQL Server helpers — connection string builder + connection pooling.

Consolidates the duplicate _build_sql_conn_str / _odbc_escape functions that
were copy-pasted across source.py, schema.py, and data_migrator.py.
Uses pyodbc connection pooling to avoid creating a fresh TCP connection on
every request.
"""
import pyodbc
from log_config import get_logger

logger = get_logger(__name__)

# ── Enable pyodbc's built-in connection pooling ──────────────────────────────
pyodbc.pooling = True


def _odbc_escape(val: str) -> str:
    """Escape braces for ODBC connection-string values."""
    return "{" + val.replace("}", "}}") + "}"


def _detect_driver() -> str:
    """Return the best available ODBC driver name (cached after first call)."""
    if not hasattr(_detect_driver, "_cached"):
        try:
            installed = pyodbc.drivers()
        except Exception:
            logger.warning("pyodbc.drivers() failed — falling back to default driver")
            installed = []
        _detect_driver._cached = (
            next((d for d in installed if "ODBC Driver 18 for SQL Server" in d), None)
            or next((d for d in installed if "ODBC Driver 17 for SQL Server" in d), None)
            or next((d for d in installed if "SQL Server" in d), None)
            or "ODBC Driver 17 for SQL Server"
        )
    return _detect_driver._cached


def build_sql_conn_str(source_type: str, server: str, database: str,
                       username: str, password: str) -> str:
    """Build a pyodbc connection string for SQL Server / Azure SQL / Synapse."""
    driver = _detect_driver()
    safe_pwd = _odbc_escape(password) if password else ""
    safe_user = _odbc_escape(username) if username else ""
    base = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={safe_user};PWD={safe_pwd}"
    is_v18 = "18" in driver
    if source_type in ("azuresql", "synapse"):
        base += ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
    else:
        if is_v18:
            base += ";Encrypt=optional;TrustServerCertificate=yes;Connection Timeout=30"
        else:
            base += ";Encrypt=no;TrustServerCertificate=yes;Connection Timeout=30"
    return base


def get_connection(source_type: str, server: str, database: str,
                   username: str, password: str, timeout: int = 15):
    """Return a pooled pyodbc connection (pyodbc.pooling handles reuse)."""
    conn_str = build_sql_conn_str(source_type, server, database, username, password)
    return pyodbc.connect(conn_str, timeout=timeout)
