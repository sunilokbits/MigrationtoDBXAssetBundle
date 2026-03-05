"""
Data Migrator — Bulk migrates SQL source tables → Databricks Unity Catalog.

Strategy (fastest path):
  1. pyodbc → read source schema + data in chunks
  2. CSV assembled in-memory (StringIO)
  3. Upload to Databricks DBFS staging via create/addblock/close REST API
  4. COPY INTO target Delta table (Unity Catalog)
  5. Parallel execution across tables using threading.Semaphore

Fallback (if COPY INTO fails):
  Batch INSERT VALUES (500 rows/statement)
"""

import io, csv, base64, time, threading, uuid, traceback
from datetime import datetime
import requests
import pyodbc

# ── SQL Server → Delta Lake type map ──────────────────────────────────────────
_TYPE_MAP = {
    "int":               "INT",
    "bigint":            "BIGINT",
    "smallint":          "SMALLINT",
    "tinyint":           "TINYINT",
    "bit":               "BOOLEAN",
    "float":             "DOUBLE",
    "real":              "FLOAT",
    "money":             "DECIMAL(19,4)",
    "smallmoney":        "DECIMAL(10,4)",
    "varchar":           "STRING",
    "nvarchar":          "STRING",
    "char":              "STRING",
    "nchar":             "STRING",
    "text":              "STRING",
    "ntext":             "STRING",
    "datetime":          "TIMESTAMP",
    "datetime2":         "TIMESTAMP",
    "smalldatetime":     "TIMESTAMP",
    "date":              "DATE",
    "time":              "STRING",
    "uniqueidentifier":  "STRING",
    "binary":            "BINARY",
    "varbinary":         "BINARY",
    "image":             "BINARY",
    "xml":               "STRING",
    "decimal":           "DECIMAL",
    "numeric":           "DECIMAL",
    "geography":         "STRING",
    "geometry":          "STRING",
    "hierarchyid":       "STRING",
    "sql_variant":       "STRING",
}

# ── In-memory job registry (reset on server restart) ─────────────────────────
MIGRATION_JOBS: dict = {}


def _map_delta_type(sql_type: str, precision=None, scale=None) -> str:
    base = sql_type.lower().split("(")[0].strip()
    dt = _TYPE_MAP.get(base, "STRING")
    if dt == "DECIMAL" and precision:
        s = scale or 0
        dt = f"DECIMAL({precision},{s})"
    return dt


def _build_conn_str(source_type: str, server: str, database: str,
                    username: str, password: str) -> str:
    """Build pyodbc connection string with best available driver."""
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    for preferred in ["ODBC Driver 18 for SQL Server",
                      "ODBC Driver 17 for SQL Server"]:
        if preferred in drivers:
            driver = preferred
            break
    else:
        driver = drivers[0] if drivers else "ODBC Driver 17 for SQL Server"

    encrypt = "yes" if source_type in ("azuresql", "synapse") else ("optional" if "18" in driver else "no")
    trust   = "no"  if source_type in ("azuresql", "synapse") else "yes"
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={username};PWD={password};"
        f"Encrypt={encrypt};TrustServerCertificate={trust};Connection Timeout=15;"
    )


# ─────────────────────────────────────────────────────────────────────────────
class DataMigrator:
    """Migrate SQL source tables to Databricks Unity Catalog Delta tables."""

    CHUNK_SIZE   = 5_000              # rows read per pyodbc fetchmany()
    INSERT_BATCH = 500                # rows per INSERT VALUES fallback
    DBFS_STAGING = "/tmp/mig_staging" # DBFS base path (cleaned up after use)

    def __init__(self, conn_str: str, dbx_host: str, token: str,
                 catalog: str = "main", schema: str = "default"):
        self.conn_str = conn_str
        self.host     = dbx_host.rstrip("/")
        self.token    = token
        self.catalog  = catalog
        self.schema   = schema
        self._sess    = requests.Session()
        self._sess.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        })

    # ── Databricks Statement Execution API ────────────────────────────────────
    def _exec_sql(self, sql: str, warehouse_id: str, timeout: str = "50s") -> dict:
        payload = {
            "statement":      sql,
            "warehouse_id":   warehouse_id,
            "catalog":        self.catalog,
            "schema":         self.schema,
            "wait_timeout":   timeout,
            "on_wait_timeout": "CONTINUE",
        }
        r = self._sess.post(f"{self.host}/api/2.0/sql/statements",
                            json=payload, timeout=60)
        data = r.json() if r.status_code == 200 else {"error": r.text[:300]}
        sid  = data.get("statement_id")
        return self._poll_sql(sid) if sid else data

    def _poll_sql(self, sid: str) -> dict:
        for _ in range(120):
            r = self._sess.get(f"{self.host}/api/2.0/sql/statements/{sid}",
                               timeout=15)
            if r.status_code != 200:
                return {"error": f"Poll {r.status_code}"}
            d  = r.json()
            st = d.get("status", {}).get("state", "")
            if st in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                return d
            time.sleep(2)
        return {"error": "Statement timed out"}

    # ── DBFS upload (supports any file size via create/addblock/close) ─────────
    def _dbfs_upload(self, path: str, data_bytes: bytes):
        """Upload bytes to DBFS. Returns (True, '') on success or (False, error_msg) on failure."""
        try:
            r = self._sess.post(f"{self.host}/api/2.0/dbfs/create",
                                json={"path": path, "overwrite": True}, timeout=15)
            if r.status_code != 200:
                return False, f"dbfs/create HTTP {r.status_code}: {r.text[:300]}"
            handle    = r.json().get("handle")
            offset    = 0
            BLOCK     = 1024 * 1024   # 1 MB
            while offset < len(data_bytes):
                chunk   = data_bytes[offset:offset + BLOCK]
                encoded = base64.b64encode(chunk).decode()
                r2 = self._sess.post(f"{self.host}/api/2.0/dbfs/add-block",
                                     json={"handle": handle, "data": encoded},
                                     timeout=30)
                if r2.status_code != 200:
                    return False, f"dbfs/add-block HTTP {r2.status_code}: {r2.text[:300]}"
                offset += BLOCK
            r3 = self._sess.post(f"{self.host}/api/2.0/dbfs/close",
                                 json={"handle": handle}, timeout=15)
            if r3.status_code != 200:
                return False, f"dbfs/close HTTP {r3.status_code}: {r3.text[:300]}"
            return True, ""
        except Exception as ex:
            return False, str(ex)

    def _dbfs_delete(self, path: str):
        try:
            self._sess.post(f"{self.host}/api/2.0/dbfs/delete",
                            json={"path": path, "recursive": False}, timeout=10)
        except Exception:
            pass

    # ── Source introspection ──────────────────────────────────────────────────
    def list_source_tables(self) -> list:
        sql = """
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME,
                   (SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA
                      AND c.TABLE_NAME  = t.TABLE_NAME) AS col_count,
                   ISNULL(p.rows, 0) AS row_estimate
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.partitions p
                   ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                  AND p.index_id IN (0, 1)
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, p.rows
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
        """
        with pyodbc.connect(self.conn_str, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            return [
                {
                    "schema":      r[0],
                    "table":       r[1],
                    "full_name":   f"{r[0]}.{r[1]}",
                    "col_count":   r[2],
                    "row_estimate": r[3],
                }
                for r in cur.fetchall()
            ]

    def describe_source_table(self, schema: str, table: str) -> dict:
        col_sql = """
            SELECT COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        with pyodbc.connect(self.conn_str, timeout=15) as conn:
            cur = conn.cursor()
            cur.execute(col_sql, schema, table)
            cols = [
                {
                    "name":       r[0],
                    "sql_type":   r[1],
                    "max_len":    r[2],
                    "precision":  r[3],
                    "scale":      r[4],
                    "nullable":   r[5] == "YES",
                    "delta_type": _map_delta_type(r[1], r[3], r[4]),
                }
                for r in cur.fetchall()
            ]
            cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            row_count = cur.fetchone()[0]
        return {"columns": cols, "row_count": row_count}

    # ── Single table migration ────────────────────────────────────────────────
    def migrate_table(self, src_schema: str, src_table: str,
                      warehouse_id: str, on_progress=None) -> dict:
        start     = time.time()
        log: list = []
        dbfs_path = f"{self.DBFS_STAGING}/{src_table}_{uuid.uuid4().hex[:8]}.csv"

        def step(msg: str):
            log.append(msg)
            if on_progress:
                on_progress(msg)

        safe_src    = f"[{src_schema}].[{src_table}]"
        target      = f"`{self.catalog}`.`{self.schema}`.`{src_table}`"

        try:
            # ── 1. Describe source ────────────────────────────────────────────
            step(f"Describing {safe_src}…")
            desc      = self.describe_source_table(src_schema, src_table)
            cols      = desc["columns"]
            row_count = desc["row_count"]
            step(f"  {len(cols)} columns, {row_count:,} rows")

            # ── 2. Create/Verify Delta table ──────────────────────────────────
            nl = ",\n  "
            col_defs = nl.join(
                f'`{c["name"]}` {c["delta_type"]}'
                + ("" if c["nullable"] else " NOT NULL")
                for c in cols
            )
            create_sql = (
                f"CREATE TABLE IF NOT EXISTS {target} (\n  {col_defs}\n) "
                "USING DELTA "
                'TBLPROPERTIES ("delta.autoOptimize.optimizeWrite"="true",'
                '"delta.autoOptimize.autoCompact"="true")'
            )
            step(f"Creating Delta table {target}…")
            res   = self._exec_sql(create_sql, warehouse_id)
            state = res.get("status", {}).get("state", "UNKNOWN")
            if state not in ("SUCCEEDED",):
                err_msg = (res.get("status", {}) or {}).get(
                    "error", {}).get("message", state) or state
                return {"success": False, "error": f"CREATE TABLE: {err_msg}", "log": log}
            step("  Table ready")

            # ── 3. Read source → CSV in-memory ────────────────────────────────
            step(f"Reading source in chunks of {self.CHUNK_SIZE:,}…")
            col_names = [c["name"] for c in cols]
            buf       = io.StringIO()
            writer    = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            writer.writerow(col_names)
            total_read = 0

            with pyodbc.connect(self.conn_str, timeout=60) as conn:
                conn.timeout = 0      # no timeout for large reads
                cur = conn.cursor()
                cur.execute(f"SELECT * FROM {safe_src}")
                while True:
                    rows = cur.fetchmany(self.CHUNK_SIZE)
                    if not rows:
                        break
                    for row in rows:
                        writer.writerow(
                            ["" if v is None else str(v) for v in row]
                        )
                    total_read += len(rows)
                    if total_read % 20_000 == 0:
                        step(f"  Read {total_read:,} / {row_count:,} rows…")

            csv_bytes = buf.getvalue().encode("utf-8")
            step(f"  {total_read:,} rows → {len(csv_bytes)/1024:.1f} KB")

            # ── 4. Upload to DBFS ─────────────────────────────────────────────
            step("Uploading to DBFS…")
            dbfs_ok, dbfs_err = self._dbfs_upload(dbfs_path, csv_bytes)
            if not dbfs_ok:
                step(f"  DBFS unavailable ({dbfs_err}) — using batch INSERT fallback")
                # ── Direct batch INSERT (no DBFS needed) ──────────────────────
                col_names_safe = ", ".join(f"`{c}`" for c in col_names)
                buf.seek(0)
                reader   = csv.reader(buf)
                next(reader)   # skip header
                batch: list = []
                inserted    = 0

                def _esc(v):
                    return "NULL" if v == "" else "'" + v.replace("'", "''") + "'"

                def _flush(b):
                    vals = ", ".join(
                        "(" + ", ".join(_esc(v) for v in r) + ")"
                        for r in b
                    )
                    self._exec_sql(
                        f"INSERT INTO {target} ({col_names_safe}) VALUES {vals}",
                        warehouse_id
                    )

                for row_vals in reader:
                    batch.append(row_vals)
                    if len(batch) >= self.INSERT_BATCH:
                        _flush(batch)
                        inserted += len(batch)
                        batch     = []
                        step(f"  Inserted {inserted:,} rows…")
                if batch:
                    _flush(batch)
                    inserted += len(batch)
                step(f"  Inserted {inserted:,} rows via batch INSERT")
                elapsed = time.time() - start
                rps     = int(inserted / max(elapsed, 0.1))
                step(f"Done — {inserted:,} rows in {elapsed:.1f}s  ({rps:,} rows/sec)")
                return {
                    "success":   True,
                    "table":     src_table,
                    "rows":      inserted,
                    "columns":   len(cols),
                    "elapsed_s": round(elapsed, 2),
                    "rows_sec":  rps,
                    "log":       log,
                }
            step("  Upload done")

            # ── 5. COPY INTO ──────────────────────────────────────────────────
            step(f"Running COPY INTO…")
            copy_sql = (
                f"COPY INTO {target} "
                f"FROM 'dbfs:{dbfs_path}' "
                "FILEFORMAT = CSV "
                "FORMAT_OPTIONS ('header'='true','inferSchema'='false',"
                "'nullValue'='','escape'='\"','quote'='\"') "
                "COPY_OPTIONS ('mergeSchema'='true')"
            )
            res2   = self._exec_sql(copy_sql, warehouse_id, timeout="50s")
            state2 = res2.get("status", {}).get("state", "UNKNOWN")

            if state2 == "SUCCEEDED":
                data_arr = res2.get("result", {}).get("data_array", [[str(total_read)]])
                copied   = data_arr[0][0] if data_arr and data_arr[0] else total_read
                step(f"  COPY INTO: {copied} rows loaded")
            else:
                # ── Fallback: batch INSERT VALUES ─────────────────────────────
                err2 = ((res2.get("status") or {}).get("error") or {}).get(
                    "message", state2)
                step(f"  COPY INTO {state2}: {err2} — using batch INSERT fallback")
                buf.seek(0)
                reader     = csv.reader(buf)
                next(reader)          # skip header
                safe_cols  = ", ".join(f"`{c}`" for c in col_names)
                batch: list = []
                inserted    = 0

                def _flush_batch(b):
                    def _esc(v):
                        return "NULL" if v == "" else "'" + v.replace("'", "''") + "'"
                    vals = ", ".join(
                        "(" + ", ".join(_esc(v) for v in row) + ")"
                        for row in b
                    )
                    self._exec_sql(
                        f"INSERT INTO {target} ({safe_cols}) VALUES {vals}",
                        warehouse_id
                    )

                for row_vals in reader:
                    batch.append(row_vals)
                    if len(batch) >= self.INSERT_BATCH:
                        _flush_batch(batch)
                        inserted += len(batch)
                        batch    = []
                        step(f"  Inserted {inserted:,} rows…")
                if batch:
                    _flush_batch(batch)
                    inserted += len(batch)
                step(f"  Inserted {inserted:,} rows via batch INSERT")
                total_read = inserted

            # ── 6. Cleanup ────────────────────────────────────────────────────
            self._dbfs_delete(dbfs_path)
            elapsed  = time.time() - start
            rps      = int(total_read / max(elapsed, 0.1))
            step(f"Done — {total_read:,} rows in {elapsed:.1f}s  ({rps:,} rows/sec)")
            return {
                "success":   True,
                "table":     src_table,
                "rows":      total_read,
                "columns":   len(cols),
                "elapsed_s": round(elapsed, 2),
                "rows_sec":  rps,
                "log":       log,
            }

        except Exception as exc:
            self._dbfs_delete(dbfs_path)
            step(f"ERROR: {exc}")
            return {
                "success": False,
                "table":   src_table,
                "error":   str(exc),
                "trace":   traceback.format_exc(),
                "log":     log,
            }

    # ── Parallel multi-table migration ────────────────────────────────────────
    def migrate_tables_parallel(self, tables: list, warehouse_id: str,
                                job_id: str, max_workers: int = 3):
        """
        Migrate tables in parallel (max_workers at a time).
        Progress is written to MIGRATION_JOBS[job_id].
        """
        job            = MIGRATION_JOBS[job_id]
        job["status"]  = "running"
        job["total"]   = len(tables)
        job["done"]    = 0
        job["failed"]  = 0
        job["results"] = {}   # keyed by "schema.table" for frontend Object.entries()
        job["logs"]    = []   # flat list of strings
        sem = threading.Semaphore(max_workers)
        log_lock = threading.Lock()

        # Pre-populate every table as queued so the UI shows names immediately
        for t in tables:
            full = f"{t.get('schema','dbo')}.{t.get('table','')}"
            job["results"][full] = {"status": "queued", "pct": 0, "rows_copied": 0}

        def _run_one(tbl):
            tname  = tbl.get("table", "")
            schema = tbl.get("schema", "dbo")
            full   = f"{schema}.{tname}"
            job["results"][full] = {"status": "running", "pct": 0, "rows_copied": 0}
            with sem:
                def _prog(msg):
                    with log_lock:
                        job["logs"].append(f"[{full}] {msg}")
                result = self.migrate_table(schema, tname, warehouse_id, _prog)
                result["status"]      = "done" if result.get("success") else "failed"
                result["pct"]         = 100
                result["rows_copied"] = result.get("rows", 0)
                job["results"][full]  = result
                if result["success"]:
                    job["done"]   += 1
                else:
                    job["failed"] += 1
                    with log_lock:
                        job["logs"].append(f"[{full}] ✕ FAILED: {result.get('error','unknown error')}")
                        trace = result.get("trace", "")
                        if trace:
                            # emit each traceback line individually for readability
                            for tline in trace.strip().splitlines():
                                job["logs"].append(f"[{full}]   {tline}")

        threads = [
            threading.Thread(target=_run_one, args=(t,), daemon=True)
            for t in tables
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        job["status"]      = "done"
        job["finished_at"] = datetime.now().isoformat()
