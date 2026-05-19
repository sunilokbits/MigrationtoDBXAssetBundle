"""
Unity Catalog Table Executor
Validates, previews, and executes PySpark notebooks targeting Unity Catalog tables.
Uses Databricks SQL Warehouse (Statement Execution API) to run queries.
"""

import requests
import json
import time
from datetime import datetime


class UnityCatalogExecutor:
    """Execute and validate tables in Databricks Unity Catalog."""

    POLL_INTERVAL = 3   # seconds between status polls
    MAX_POLLS     = 40  # max polls before timeout
    _DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

    def __init__(self, host: str, token: str, catalog: str = "main", schema: str = "default"):
        self.host    = host.rstrip("/")
        self.token   = token
        self.catalog = catalog
        self.schema  = schema
        self._aad_token = None
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    # ── AAD fallback ──────────────────────────────────────────────────────────
    def _try_aad_fallback(self) -> bool:
        """Attempt to get an Azure AD token for Databricks and update session headers."""
        if self._aad_token:
            return True
        try:
            from azure.identity import DefaultAzureCredential
            cred = DefaultAzureCredential()
            tok = cred.get_token(f"{self._DATABRICKS_RESOURCE_ID}/.default")
            self._aad_token = tok.token
            self.session.headers["Authorization"] = f"Bearer {self._aad_token}"
            return True
        except Exception:
            return False

    # ── Statement Execution API ───────────────────────────────────────────────
    def _execute_statement(self, sql: str, warehouse_id: str, wait_timeout: str = "30s") -> dict:
        """Submit a SQL statement to a SQL Warehouse and return results."""
        payload = {
            "statement"       : sql,
            "warehouse_id"    : warehouse_id,
            "catalog"         : self.catalog,
            "schema"          : self.schema,
            "wait_timeout"    : wait_timeout,
            "on_wait_timeout" : "CONTINUE"
        }
        resp = self.session.post(
            f"{self.host}/api/2.0/sql/statements",
            json=payload,
            timeout=60
        )
        # If PAT returns 401/403, try AAD token automatically
        if resp.status_code in (401, 403) and self._try_aad_fallback():
            resp = self.session.post(
                f"{self.host}/api/2.0/sql/statements",
                json=payload,
                timeout=60
            )
        return resp.json() if resp.status_code == 200 else {"error": resp.text[:300], "status_code": resp.status_code}

    def _poll_statement(self, statement_id: str) -> dict:
        """Poll until statement finishes execution."""
        for _ in range(self.MAX_POLLS):
            resp = self.session.get(
                f"{self.host}/api/2.0/sql/statements/{statement_id}",
                timeout=15
            )
            if resp.status_code != 200:
                return {"error": f"Poll error {resp.status_code}"}

            data   = resp.json()
            status = data.get("status", {}).get("state", "")

            if status in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                return data

            time.sleep(self.POLL_INTERVAL)

        return {"error": "Statement timed out after polling"}

    # ── List SQL Warehouses ───────────────────────────────────────────────────
    def list_warehouses(self) -> dict:
        """List available SQL Warehouses. Falls back to AAD token if PAT fails."""
        try:
            resp = self.session.get(f"{self.host}/api/2.0/sql/warehouses", timeout=15)
            # If PAT returns 401/403, try AAD token automatically
            if resp.status_code in (401, 403) and self._try_aad_fallback():
                resp = self.session.get(f"{self.host}/api/2.0/sql/warehouses", timeout=15)
            if resp.status_code == 200:
                whs = resp.json().get("warehouses", [])
                return {
                    "success"   : True,
                    "warehouses": [
                        {
                            "id"   : w.get("id"),
                            "name" : w.get("name"),
                            "state": w.get("state"),
                            "size" : w.get("cluster_size","N/A"),
                            "type" : w.get("warehouse_type","N/A")
                        }
                        for w in whs
                    ]
                }
            return {"success": False, "message": resp.text[:200]}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── List Unity Catalog Tables ─────────────────────────────────────────────
    def list_tables(self) -> dict:
        """List tables in target catalog.schema."""
        try:
            url  = f"{self.host}/api/2.1/unity-catalog/tables"
            params = {"catalog_name": self.catalog, "schema_name": self.schema}
            resp = self.session.get(url, params=params, timeout=15)
            # If PAT returns 401/403, try AAD token automatically
            if resp.status_code in (401, 403) and self._try_aad_fallback():
                resp = self.session.get(url, params=params, timeout=15)

            if resp.status_code == 200:
                tables = resp.json().get("tables", [])
                return {
                    "success": True,
                    "catalog": self.catalog,
                    "schema" : self.schema,
                    "tables" : [
                        {
                            "table_name"  : t.get("name"),
                            "table_type"  : t.get("table_type", "N/A"),
                            "data_source" : t.get("data_source_format", "N/A"),
                            "row_count"   : t.get("properties", {}).get("numRows", "N/A"),
                            "created_at"  : t.get("created_at"),
                            "updated_at"  : t.get("updated_at")
                        }
                        for t in tables
                    ]
                }
            # Fallback: use INFORMATION_SCHEMA
            return self._list_tables_via_sql()
        except Exception as e:
            return {"success": False, "message": str(e)}

    def _list_tables_via_sql(self) -> dict:
        """Fallback: list tables via SHOW TABLES SQL."""
        try:
            resp = self.session.get(f"{self.host}/api/2.0/sql/warehouses", timeout=10)
            whs  = resp.json().get("warehouses", []) if resp.status_code == 200 else []
            running_wh = next((w for w in whs if w.get("state") == "RUNNING"), whs[0] if whs else None)

            if not running_wh:
                return {"success": False, "message": "No SQL Warehouse available"}

            result = self._execute_statement(
                f"SHOW TABLES IN `{self.catalog}`.`{self.schema}`",
                running_wh["id"]
            )
            return {"success": True, "sql_result": result, "fallback": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Get Table Schema ──────────────────────────────────────────────────────
    def describe_table(self, table_name: str, warehouse_id: str) -> dict:
        """Return schema and stats for a Unity Catalog table."""
        try:
            sql  = f"DESCRIBE EXTENDED `{self.catalog}`.`{self.schema}`.`{table_name}`"
            data = self._execute_statement(sql, warehouse_id)

            stmt_id = data.get("statement_id")
            if not stmt_id:
                return {"success": False, "message": data.get("error", "No statement ID returned")}

            result = self._poll_statement(stmt_id)
            status = result.get("status", {}).get("state", "UNKNOWN")

            if status == "SUCCEEDED":
                rows = (result.get("result", {}).get("data_array", []))
                return {
                    "success"   : True,
                    "table"     : f"{self.catalog}.{self.schema}.{table_name}",
                    "columns"   : rows,
                    "statement_id": stmt_id
                }
            return {"success": False, "message": f"Describe failed: {status}", "detail": result}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Preview Table Data ────────────────────────────────────────────────────
    def preview_table(self, table_name: str, warehouse_id: str, limit: int = 20) -> dict:
        """Run SELECT TOP N on a Unity Catalog table and return preview data."""
        try:
            sql  = f"SELECT * FROM `{self.catalog}`.`{self.schema}`.`{table_name}` LIMIT {limit}"
            data = self._execute_statement(sql, warehouse_id)

            stmt_id = data.get("statement_id")
            if not stmt_id:
                return {"success": False, "message": data.get("error", str(data))}

            result = self._poll_statement(stmt_id)
            status = result.get("status", {}).get("state", "UNKNOWN")

            if status == "SUCCEEDED":
                manifest = result.get("manifest", {})
                columns  = [col.get("name") for col in manifest.get("schema", {}).get("columns", [])]
                rows     = result.get("result", {}).get("data_array", [])
                row_count_resp = self._execute_statement(
                    f"SELECT COUNT(*) AS cnt FROM `{self.catalog}`.`{self.schema}`.`{table_name}`",
                    warehouse_id
                )
                count_id = row_count_resp.get("statement_id")
                total_rows = "N/A"
                if count_id:
                    count_result = self._poll_statement(count_id)
                    if count_result.get("status", {}).get("state") == "SUCCEEDED":
                        total_rows = count_result.get("result", {}).get("data_array", [["N/A"]])[0][0]

                return {
                    "success"   : True,
                    "table"     : f"{self.catalog}.{self.schema}.{table_name}",
                    "columns"   : columns,
                    "rows"      : rows,
                    "preview_rows": len(rows),
                    "total_rows": total_rows,
                    "statement_id": stmt_id
                }
            return {"success": False, "message": f"Preview failed: {status}", "detail": result}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Execute Custom SQL (ad-hoc query / DML) ───────────────────────────────
    def execute_custom_sql(self, sql: str, warehouse_id: str) -> dict:
        """Execute an ad-hoc SQL statement and return columns + rows for SELECT,
        or a simple success/fail for DDL / DML."""
        try:
            resp = self._execute_statement(sql.strip(), warehouse_id, wait_timeout="50s")
            sid  = resp.get("statement_id")
            if not sid:
                return {"success": False, "message": resp.get("error", str(resp)[:300])}

            result = self._poll_statement(sid)
            state  = result.get("status", {}).get("state", "UNKNOWN")

            if state == "SUCCEEDED":
                manifest = result.get("manifest", {})
                columns  = [col.get("name") for col in manifest.get("schema", {}).get("columns", [])]
                rows     = result.get("result", {}).get("data_array", [])
                return {
                    "success"     : True,
                    "sql_type"    : "query" if columns else "statement",
                    "columns"     : columns,
                    "rows"        : rows,
                    "row_count"   : len(rows),
                    "statement_id": sid
                }
            else:
                err_msg = (result.get("status", {}).get("error", {}) or {}).get("message", f"Statement {state}")
                return {"success": False, "message": err_msg, "state": state}

        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Execute Full Table Pipeline ───────────────────────────────────────────
    def execute_table_pipeline(self, table_name: str, warehouse_id: str, execute_sql: str = None) -> dict:
        """Execute a full pipeline and validate the output table in Unity Catalog."""
        try:
            steps   = []
            overall = True

            # Step 1: Check if table exists
            check_sql = f"""
                SELECT COUNT(*) AS cnt
                FROM `{self.catalog}`.INFORMATION_SCHEMA.TABLES
                WHERE table_name = '{table_name}'
                  AND table_schema = '{self.schema}'
            """
            resp = self._execute_statement(check_sql, warehouse_id)
            sid  = resp.get("statement_id")
            exists = False
            if sid:
                r = self._poll_statement(sid)
                if r.get("status",{}).get("state") == "SUCCEEDED":
                    cnt = int(r.get("result",{}).get("data_array",[[0]])[0][0])
                    exists = cnt > 0

            steps.append({
                "step"   : "Table Existence Check",
                "status" : "PASS" if exists else "INFO",
                "detail" : f"Table `{table_name}` {'exists' if exists else 'will be created'}"
            })

            # Step 2: Run custom SQL if provided
            if execute_sql:
                resp2 = self._execute_statement(execute_sql.strip(), warehouse_id, wait_timeout="50s")
                sid2  = resp2.get("statement_id")
                if sid2:
                    r2     = self._poll_statement(sid2)
                    state2 = r2.get("status",{}).get("state","UNKNOWN")
                    steps.append({
                        "step"  : "Custom SQL Execution",
                        "status": "PASS" if state2 == "SUCCEEDED" else "FAIL",
                        "detail": f"State: {state2}"
                    })
                    if state2 != "SUCCEEDED":
                        overall = False

            # Step 3: Validate table row count
            preview = self.preview_table(table_name, warehouse_id, limit=5)
            if preview["success"]:
                steps.append({
                    "step"       : "Table Validation",
                    "status"     : "PASS",
                    "detail"     : f"Total rows: {preview['total_rows']} | Columns: {len(preview['columns'])}",
                    "columns"    : preview["columns"],
                    "sample_rows": preview["rows"][:5]
                })
            else:
                steps.append({
                    "step"  : "Table Validation",
                    "status": "WARN",
                    "detail": preview.get("message","Table not yet available")
                })

            # Step 4: OPTIMIZE the table (Delta)
            opt_resp = self._execute_statement(
                f"OPTIMIZE `{self.catalog}`.`{self.schema}`.`{table_name}`",
                warehouse_id
            )
            opt_sid = opt_resp.get("statement_id")
            if opt_sid:
                opt_r = self._poll_statement(opt_sid)
                opt_s = opt_r.get("status",{}).get("state","UNKNOWN")
                steps.append({
                    "step"  : "Delta OPTIMIZE",
                    "status": "PASS" if opt_s == "SUCCEEDED" else "WARN",
                    "detail": f"Optimize state: {opt_s}"
                })

            return {
                "success"   : overall,
                "table"     : f"{self.catalog}.{self.schema}.{table_name}",
                "steps"     : steps,
                "executed_at": str(datetime.now())
            }

        except Exception as e:
            return {"success": False, "message": str(e)}
