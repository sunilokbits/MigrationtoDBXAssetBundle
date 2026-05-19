"""
Databricks Connection Manager
Tests connectivity, uploads notebooks, and runs jobs via the Databricks REST API
"""

import requests
import json
import time
import os
import base64
from datetime import datetime


class DatabricksConnector:
    """Manages connections to a Databricks workspace via REST API."""

    _DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

    def __init__(self, host: str, token: str):
        self.host  = host.rstrip("/")
        self.token = token
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

    # ── Connection Test ───────────────────────────────────────────────────────
    def test_connection(self) -> dict:
        """Verify token and host by calling the clusters list API. Falls back to AAD if PAT fails."""
        try:
            url = f"{self.host}/api/2.0/clusters/list"
            resp = self.session.get(url, timeout=15)

            # If PAT returns 401/403, try AAD token automatically
            if resp.status_code in (401, 403):
                if self._try_aad_fallback():
                    resp = self.session.get(url, timeout=15)

            if resp.status_code == 200:
                data   = resp.json()
                clusters = data.get("clusters", [])
                running  = [c for c in clusters if c.get("state") == "RUNNING"]
                return {
                    "success"       : True,
                    "message"       : "Connection Successful",
                    "workspace_host": self.host,
                    "total_clusters": len(clusters),
                    "running_clusters": len(running),
                    "cluster_names" : [c.get("cluster_name","N/A") for c in clusters[:5]],
                    "timestamp"     : str(datetime.now())
                }

            elif resp.status_code == 401:
                return {"success": False, "message": "Authentication Failed — Invalid Token", "status_code": 401}
            elif resp.status_code == 403:
                return {"success": False, "message": "Authorization Failed — Insufficient Permissions", "status_code": 403}
            else:
                return {"success": False, "message": f"API Error {resp.status_code}: {resp.text[:200]}", "status_code": resp.status_code}

        except requests.exceptions.ConnectionError:
            return {"success": False, "message": f"Cannot reach host: {self.host} — check URL / network"}
        except requests.exceptions.Timeout:
            return {"success": False, "message": "Connection timed out after 15 seconds"}
        except Exception as e:
            return {"success": False, "message": f"Unexpected error: {str(e)}"}

    # ── Workspace Info ────────────────────────────────────────────────────────
    def get_workspace_info(self) -> dict:
        """Return basic workspace metadata."""
        try:
            me_resp   = self.session.get(f"{self.host}/api/2.0/preview/scim/v2/Me", timeout=10)
            dbfs_resp = self.session.get(f"{self.host}/api/2.0/dbfs/list?path=/", timeout=10)

            me   = me_resp.json()   if me_resp.status_code   == 200 else {}
            dbfs = dbfs_resp.json() if dbfs_resp.status_code == 200 else {}

            return {
                "success"   : True,
                "user_name" : me.get("userName", "N/A"),
                "user_email": me.get("emails", [{}])[0].get("value", "N/A") if me.get("emails") else "N/A",
                "dbfs_root" : [f.get("path") for f in dbfs.get("files", [])[:5]],
                "host"      : self.host
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Delete Notebook ─────────────────────────────────────────────────────
    def delete_notebook(self, notebook_path: str) -> dict:
        """Delete a single notebook from Databricks workspace (non-recursive)."""
        try:
            resp = self.session.post(
                f"{self.host}/api/2.0/workspace/delete",
                json={"path": notebook_path, "recursive": False},
                timeout=15
            )
            if resp.status_code in (200, 404):
                return {"success": True, "path": notebook_path}
            return {"success": False, "error": f"Delete failed ({resp.status_code}): {resp.text[:200]}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Upload Notebook ───────────────────────────────────────────────────────
    def upload_notebook(self, notebook_name: str, python_code: str, path: str = "/Shared/Migrations") -> dict:
        """Upload a Python notebook to the Databricks workspace."""
        try:
            # Ensure path exists
            self.session.post(
                f"{self.host}/api/2.0/workspace/mkdirs",
                json={"path": path},
                timeout=10
            )

            notebook_path = f"{path}/{notebook_name}"
            encoded_code  = base64.b64encode(python_code.encode("utf-8")).decode("utf-8")

            payload = {
                "path"      : notebook_path,
                "language"  : "PYTHON",
                "content"   : encoded_code,
                "overwrite" : True,
                "format"    : "SOURCE"
            }

            resp = self.session.post(
                f"{self.host}/api/2.0/workspace/import",
                json=payload,
                timeout=30
            )

            if resp.status_code == 200:
                return {
                    "success"       : True,
                    "message"       : f"Notebook uploaded successfully",
                    "notebook_path" : notebook_path,
                    "workspace_url" : f"{self.host}/#workspace{notebook_path}",
                    "lines_uploaded": len(python_code.splitlines())
                }
            else:
                return {
                    "success": False,
                    "message": f"Upload failed ({resp.status_code}): {resp.text[:300]}"
                }

        except Exception as e:
            return {"success": False, "message": f"Upload error: {str(e)}"}

    # ── Run Notebook via Job ──────────────────────────────────────────────────
    def run_notebook(self, notebook_path: str, cluster_id: str = None, params: dict = None) -> dict:
        """Submit a one-time run for a notebook in Databricks.

        If cluster_id is provided, uses that cluster directly.
        Otherwise tries serverless first, then falls back to any running
        cluster or a new classic cluster.
        """
        try:
            nb_name = notebook_path.rsplit("/", 1)[-1]
            task_key = nb_name.replace(" ", "_")[:100]

            task = {
                "run_name": f"MigrationStudio_{nb_name}",
                "tasks": [{
                    "task_key": task_key,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": params or {}
                    },
                }],
            }

            # ── If a cluster was explicitly selected, use it directly ──
            if cluster_id:
                task["tasks"][0]["existing_cluster_id"] = cluster_id
                resp = self.session.post(
                    f"{self.host}/api/2.1/jobs/runs/submit",
                    json=task,
                    timeout=30,
                )
                if resp.status_code == 200:
                    return self._parse_run_response(resp)
                return {"success": False, "message": f"Run submit failed: {resp.text[:300]}"}

            # ── No cluster selected — try serverless first ──
            serverless_task = {**task}
            serverless_task["tasks"] = [{**task["tasks"][0], "environment_key": "Default"}]
            serverless_task["environments"] = [{
                "environment_key": "Default",
                "spec": {"client": "1"}
            }]
            resp = self.session.post(
                f"{self.host}/api/2.1/jobs/runs/submit",
                json=serverless_task,
                timeout=30,
            )
            if resp.status_code == 200:
                return self._parse_run_response(resp)

            # If serverless is not available, fall back to classic compute
            err_text = resp.text[:400]
            is_serverless_rejection = any(
                kw in err_text.lower()
                for kw in ("serverless", "environment_key", "not supported", "not enabled", "environment")
            )
            if not is_serverless_rejection:
                return {"success": False, "message": f"Run submit failed: {resp.text[:300]}"}

            # ── Attempt 2: Find any running cluster ──
            clusters_resp = self.session.get(f"{self.host}/api/2.0/clusters/list", timeout=10)
            if clusters_resp.status_code == 200:
                running = [c for c in clusters_resp.json().get("clusters", [])
                           if c.get("state") == "RUNNING"]
                if running:
                    cluster_id = running[0]["cluster_id"]

            if cluster_id:
                task["tasks"][0]["existing_cluster_id"] = cluster_id
            else:
                task["tasks"][0]["new_cluster"] = {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id":  "Standard_DS3_v2",
                    "num_workers":   2,
                }

            resp = self.session.post(
                f"{self.host}/api/2.1/jobs/runs/submit",
                json=task,
                timeout=30,
            )
            if resp.status_code == 200:
                return self._parse_run_response(resp)
            else:
                return {"success": False, "message": f"Run submit failed: {resp.text[:300]}"}

        except Exception as e:
            return {"success": False, "message": f"Run error: {str(e)}"}

    # ── Helper: parse a successful runs/submit response ───────────────────────
    def _parse_run_response(self, resp) -> dict:
        run_id = resp.json().get("run_id")
        return {
            "success":      True,
            "run_id":       run_id,
            "message":      "Notebook run submitted",
            "run_url":      f"{self.host}/#job/{run_id}/run/{run_id}",
            "submitted_at": str(datetime.now()),
        }

    # ── Get Run Status ────────────────────────────────────────────────────────
    def get_run_status(self, run_id: int) -> dict:
        """Poll job run status."""
        try:
            resp = self.session.get(
                f"{self.host}/api/2.1/jobs/runs/get?run_id={run_id}",
                timeout=10
            )
            if resp.status_code == 200:
                data   = resp.json()
                state  = data.get("state", {})
                return {
                    "success"     : True,
                    "run_id"      : run_id,
                    "life_cycle"  : state.get("life_cycle_state", "UNKNOWN"),
                    "result_state": state.get("result_state", ""),
                    "state_message": state.get("state_message", ""),
                    "start_time"  : data.get("start_time"),
                    "end_time"    : data.get("end_time")
                }
            return {"success": False, "message": f"Status fetch failed: {resp.text[:200]}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Get Run Output ───────────────────────────────────────────────────────
    def get_run_output(self, run_id: int) -> dict:
        """Fetch notebook output and error details for a completed run.

        For tasks-based runs, iterates over child tasks to find notebook
        output and error traces at the task level.
        """
        try:
            # First get the run details to find task run ids
            run_resp = self.session.get(
                f"{self.host}/api/2.1/jobs/runs/get?run_id={run_id}",
                timeout=15,
            )
            run_data = run_resp.json() if run_resp.status_code == 200 else {}
            tasks_list = run_data.get("tasks", [])

            # Collect task-level error info
            task_summaries = []
            task_errors = []
            task_nb_result = ""
            for t in tasks_list:
                ts = t.get("state", {})
                t_run_id = t.get("run_id")
                t_error = ""
                t_trace = ""
                t_nb_result = ""

                # Fetch per-task output if we have a task run_id
                if t_run_id:
                    try:
                        t_resp = self.session.get(
                            f"{self.host}/api/2.1/jobs/runs/get-output?run_id={t_run_id}",
                            timeout=15,
                        )
                        if t_resp.status_code == 200:
                            t_data = t_resp.json()
                            t_nb_out = t_data.get("notebook_output", {})
                            t_nb_result = t_nb_out.get("result", "") or ""
                            t_error = t_data.get("error", "") or ""
                            t_trace = t_data.get("error_trace", "") or ""
                    except Exception:
                        pass

                task_summaries.append({
                    "task_key":     t.get("task_key", ""),
                    "life_cycle":   ts.get("life_cycle_state", ""),
                    "result_state": ts.get("result_state", ""),
                    "state_message": ts.get("state_message", ""),
                })
                if t_error or t_trace:
                    task_errors.append(f"[{t.get('task_key', 'task')}] {t_error}")
                    if t_trace:
                        task_errors.append(t_trace[:1500])
                if t_nb_result and not task_nb_result:
                    task_nb_result = t_nb_result

            # Also try top-level output (works for single-task runs)
            resp = self.session.get(
                f"{self.host}/api/2.1/jobs/runs/get-output?run_id={run_id}",
                timeout=15,
            )
            top_error = ""
            top_trace = ""
            top_nb_result = ""
            if resp.status_code == 200:
                data = resp.json()
                nb_output = data.get("notebook_output", {})
                top_nb_result = nb_output.get("result", "") or ""
                top_error = data.get("error", "") or ""
                top_trace = data.get("error_trace", "") or ""

            # Merge: prefer task-level detail, fall back to top-level
            error = "\n".join(task_errors) if task_errors else top_error
            error_trace = "\n".join(task_errors) if task_errors else top_trace
            nb_result = task_nb_result or top_nb_result

            metadata = run_data
            state = metadata.get("state", {})

            return {
                "success":       True,
                "run_id":        run_id,
                "notebook_result": nb_result[:2000] if nb_result else "",
                "notebook_truncated": False,
                "error":         error[:2000] if error else "",
                "error_trace":   error_trace[:2000] if error_trace else "",
                "life_cycle":    state.get("life_cycle_state", ""),
                "result_state":  state.get("result_state", ""),
                "state_message": state.get("state_message", ""),
                "tasks":         task_summaries,
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── List Clusters ─────────────────────────────────────────────────────────
    def list_clusters(self) -> dict:
        """Return available clusters."""
        try:
            resp = self.session.get(f"{self.host}/api/2.0/clusters/list", timeout=10)
            if resp.status_code == 200:
                clusters = resp.json().get("clusters", [])
                return {
                    "success" : True,
                    "clusters": [
                        {
                            "cluster_id"  : c.get("cluster_id"),
                            "cluster_name": c.get("cluster_name"),
                            "state"       : c.get("state"),
                            "spark_version": c.get("spark_version"),
                            "num_workers" : c.get("num_workers", 0)
                        }
                        for c in clusters
                    ]
                }
            return {"success": False, "message": resp.text[:200]}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # ── Start Cluster ─────────────────────────────────────────────────────────────────
    def start_cluster(self, cluster_id: str) -> dict:
        """Start a terminated Databricks cluster."""
        try:
            resp = self.session.post(
                f"{self.host}/api/2.0/clusters/start",
                json={"cluster_id": cluster_id},
                timeout=15,
            )
            if resp.status_code == 200:
                return {"success": True, "message": "Cluster start initiated"}
            return {"success": False, "message": resp.text[:300]}
        except Exception as e:
            return {"success": False, "message": str(e)}
