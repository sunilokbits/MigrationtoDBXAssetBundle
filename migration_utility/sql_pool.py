"""Shared SQL Server helpers — connection string builder + connection pooling.

Consolidates the duplicate _build_sql_conn_str / _odbc_escape functions that
were copy-pasted across source.py, schema.py, and data_migrator.py.
Uses pyodbc connection pooling to avoid creating a fresh TCP connection on
every request.
"""
import pyodbc
import threading
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
            next((d for d in installed if "ODBC Driver 17 for SQL Server" in d), None)
            or next((d for d in installed if "ODBC Driver 18 for SQL Server" in d), None)
            or next((d for d in installed if "SQL Server" in d), None)
            or "ODBC Driver 17 for SQL Server"
        )
    return _detect_driver._cached


def build_sql_conn_str(source_type: str, server: str, database: str,
                       username: str, password: str, timeout: int = 15) -> str:
    """Build a pyodbc connection string for SQL Server / Azure SQL / Synapse."""
    driver = _detect_driver()
    safe_pwd = _odbc_escape(password) if password else ""
    safe_user = _odbc_escape(username) if username else ""
    # Auto-append FQDN for Azure SQL if user only provided the server name
    if source_type in ("azuresql", "synapse") and "." not in server:
        server = server + ".database.windows.net"
    base = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={safe_user};PWD={safe_pwd}"
    is_v18 = "18" in driver
    if source_type in ("azuresql", "synapse"):
        base += f";Encrypt=yes;TrustServerCertificate=no;Connection Timeout={timeout}"
    else:
        if is_v18:
            base += f";Encrypt=optional;TrustServerCertificate=yes;Connection Timeout={timeout}"
        else:
            base += f";Encrypt=no;TrustServerCertificate=yes;Connection Timeout={timeout}"
    return base


def get_connection(source_type: str, server: str, database: str,
                   username: str, password: str, timeout: int = 15):
    """Return a pooled pyodbc connection with a hard thread-based timeout.
    
    On Linux/unixODBC, pyodbc's timeout parameter and ODBC Connection Timeout
    are sometimes ignored during DNS resolution or TCP handshake. We wrap the
    connect call in a thread to guarantee it returns within `timeout` seconds.
    
    Handles Azure SQL Serverless auto-pause (error 40925/40613) by retrying up
    to 5 times with increasing delays to allow the database to resume (~30-60s).
    """
    import time as _time
    conn_str = build_sql_conn_str(source_type, server, database, username, password, timeout=timeout)

    max_retries = 5
    retry_delays = [3, 5, 10, 15, 20]  # total wait: ~53s + connect attempts

    for attempt in range(max_retries + 1):
        result = [None]
        error = [None]

        def _connect():
            try:
                result[0] = pyodbc.connect(conn_str, timeout=timeout)
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=_connect, daemon=True)
        t.start()
        t.join(timeout=timeout + 5)  # give 5s grace beyond ODBC timeout

        if t.is_alive():
            if attempt < max_retries:
                logger.info("Connection attempt %d/%d timed out, retrying...", attempt + 1, max_retries)
                _time.sleep(retry_delays[attempt])
                continue
            raise Exception(f"Connection timed out after {timeout}s — cannot reach server '{server}'. Check firewall rules and that the App Service IPs are allow-listed.")

        if error[0]:
            err_msg = str(error[0]).lower()
            # 40925 = database paused (serverless), 40613 = database unavailable
            is_resuming = ("40925" in err_msg or "40613" in err_msg or
                          "not currently available" in err_msg or
                          "current state" in err_msg or
                          "is not accessible" in err_msg)
            if is_resuming and attempt < max_retries:
                logger.info("Database is resuming from pause (attempt %d/%d), retrying in %ds...",
                            attempt + 1, max_retries, retry_delays[attempt])
                _time.sleep(retry_delays[attempt])
                continue
            raise error[0]

        return result[0]

        return result[0]
