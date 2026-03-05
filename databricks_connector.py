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

    def __init__(self, host: str, token: str):
        self.host  = host.rstrip("/")
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    # ── Connection Test ───────────────────────────────────────────────────────
    def test_connection(self) -> dict:
        """Verify token and host by calling the clusters list API."""
        try:
            url = f"{self.host}/api/2.0/clusters/list"
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
        """Submit a one-time run for a notebook in Databricks."""
        try:
            # If no cluster_id, get first available running cluster
            if not cluster_id:
                clusters_resp = self.session.get(f"{self.host}/api/2.0/clusters/list", timeout=10)
                if clusters_resp.status_code == 200:
                    running = [c for c in clusters_resp.json().get("clusters", [])
                               if c.get("state") == "RUNNING"]
                    if running:
                        cluster_id = running[0]["cluster_id"]

            task = {
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": params or {}
                }
            }

            if cluster_id:
                task["existing_cluster_id"] = cluster_id
            else:
                task["new_cluster"] = {
                    "spark_version"  : "14.3.x-scala2.12",
                    "node_type_id"   : "Standard_DS3_v2",
                    "num_workers"    : 2,
                    "spark_conf"     : {"spark.databricks.cluster.profile": "serverless"}
                }

            resp = self.session.post(
                f"{self.host}/api/2.1/jobs/runs/submit",
                json=task,
                timeout=30
            )

            if resp.status_code == 200:
                run_id = resp.json().get("run_id")
                return {
                    "success"         : True,
                    "run_id"          : run_id,
                    "message"         : "Notebook run submitted",
                    "run_url"         : f"{self.host}/#job/{run_id}/run/{run_id}",
                    "submitted_at"    : str(datetime.now())
                }
            else:
                return {"success": False, "message": f"Run submit failed: {resp.text[:300]}"}

        except Exception as e:
            return {"success": False, "message": f"Run error: {str(e)}"}

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
