"""
AutoInfraCreation — Automated Unity Catalog Infrastructure Setup
Creates Azure Storage, Access Connector, External Locations, Volume, and Catalogs
for an external Unity Catalog on Azure Databricks.

Usage:
    python AutoInfraCreation.py                 # interactive — prompts for credentials
    python AutoInfraCreation.py --auto          # uses env-vars for all credentials
"""

import os
import sys
import json
import time
import shutil
import argparse
import subprocess
from datetime import datetime


# ═══════════════════════════════════════════════════════════════════════════════
#  Configuration — edit these to match your environment
# ═══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # Azure
    "subscription_id"    : "97d4958e-c22e-451a-ad4c-09493a58a851",
    "region"             : "centralindia",
    "resource_group"     : "azdb_sunilpoc",          # same as workspace RG

    # Storage Account
    "storage_account"    : "sqltodatabrciksmig",
    "container"          : "datalake",
    "folders"            : [
        "dev/landing",
        "dev/uc-managed/bronze",
        "dev/uc-managed/silver",
    ],

    # Access Connector
    "access_connector"   : "sqltodatabrciks_access_mig",

    # Databricks workspace
    "databricks_host"    : os.getenv("DATABRICKS_HOST", ""),   # e.g. https://adb-xxx.azuredatabricks.net
    "databricks_token"   : os.getenv("DATABRICKS_TOKEN", ""),

    # External Locations
    "external_locations" : {
        "landing_loc_mig"       : "abfss://datalake@sqltodatabrciksmig.dfs.core.windows.net/dev/landing",
        "dev_managed_root_mig"  : "abfss://datalake@sqltodatabrciksmig.dfs.core.windows.net/dev/uc-managed",
    },

    # Volume
    "volume_name"        : "landing_volume",
    "volume_catalog"     : "dev_volumes",
    "volume_schema"      : "default",
    "volume_path"        : "abfss://datalake@sqltodatabrciksmig.dfs.core.windows.net/dev/landing",

    # Catalogs  →  catalog_name : managed_location
    "catalogs"           : {
        "dev_volumes" : "abfss://datalake@sqltodatabrciksmig.dfs.core.windows.net/dev/landing",
        "bronze"      : "abfss://datalake@sqltodatabrciksmig.dfs.core.windows.net/dev/uc-managed/bronze",
        "silver"      : "abfss://datalake@sqltodatabrciksmig.dfs.core.windows.net/dev/uc-managed/silver",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _log(msg, level="INFO"):
    print(f"[{_ts()}] [{level}] {msg}")


def _find_az():
    """Locate the Azure CLI executable (handles Windows .cmd/.bat extension)."""
    # Try 'az' directly first
    az = shutil.which("az")
    if az:
        return az
    # On Windows, az may be az.cmd (MSI) or az.bat (pip install)
    if sys.platform == "win32":
        for ext in ("az.cmd", "az.bat"):
            az = shutil.which(ext)
            if az:
                return az
        # Check inside the current Python env's Scripts dir (pip-installed az)
        scripts_dir = os.path.join(sys.prefix, "Scripts")
        for name in ("az.bat", "az.cmd"):
            candidate = os.path.join(scripts_dir, name)
            if os.path.isfile(candidate):
                return candidate
        # Common MSI install locations
        for candidate in [
            os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"),
            os.path.expandvars(r"%ProgramFiles%\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"),
            os.path.expandvars(r"%LOCALAPPDATA%\Programs\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"),
        ]:
            if os.path.isfile(candidate):
                return candidate
    return None

_AZ_PATH = _find_az()


def _run_az(args: list, check: bool = True) -> dict:
    """Run an `az` CLI command and return parsed JSON output."""
    if not _AZ_PATH:
        msg = ("Azure CLI ('az') not found in PATH. "
               "Install from https://aka.ms/installazurecli and restart the terminal/server.")
        _log(msg, "ERROR")
        if check:
            raise RuntimeError(msg)
        return {}

    cmd = [_AZ_PATH] + args + ["-o", "json"]
    _log(f"az {' '.join(args[:6])}…")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300,
                                stdin=subprocess.DEVNULL,          # prevent interactive prompts
                                shell=(sys.platform == "win32"))   # shell=True on Windows for .cmd
    except FileNotFoundError:
        msg = "Azure CLI ('az') not found. Install from https://aka.ms/installazurecli"
        _log(msg, "ERROR")
        if check:
            raise RuntimeError(msg)
        return {}
    except subprocess.TimeoutExpired:
        msg = "az command timed out after 300 seconds"
        _log(msg, "ERROR")
        if check:
            raise RuntimeError(msg)
        return {}

    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
        if check:
            _log(f"AZ CLI error: {err[:500]}", "ERROR")
            raise RuntimeError(err[:500])
        _log(f"AZ CLI warning (rc={result.returncode}): {err[:300]}", "WARN")
        return {}
    try:
        return json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        return {"raw": result.stdout.strip()[:500]}


def _databricks_api(method, path, cfg, payload=None):
    """Call Databricks REST API. Returns (success:bool, data:dict)."""
    import requests

    url = f"{cfg['databricks_host'].rstrip('/')}{path}"
    headers = {
        "Authorization": f"Bearer {cfg['databricks_token']}",
        "Content-Type":  "application/json",
    }
    resp = requests.request(method, url, headers=headers, json=payload, timeout=60)
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:500]}

    ok = 200 <= resp.status_code < 300
    if not ok:
        _log(f"Databricks API {resp.status_code}: {json.dumps(body)[:300]}", "ERROR")
    return ok, body


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 1 — Create Storage Account + Container + Folders
# ═══════════════════════════════════════════════════════════════════════════════

def create_storage(cfg):
    _log("═══ Step 1: Storage Account + Container + Folders ═══")

    sub  = cfg["subscription_id"]
    rg   = cfg["resource_group"]
    sa   = cfg["storage_account"]
    loc  = cfg["region"]
    ctr  = cfg["container"]

    # 1a — Storage account (HNS enabled for ADLS Gen2)
    _log(f"Creating storage account '{sa}' in '{loc}'…")
    _run_az([
        "storage", "account", "create",
        "--name", sa,
        "--resource-group", rg,
        "--location", loc,
        "--sku", "Standard_LRS",
        "--kind", "StorageV2",
        "--hns", "true",                     # hierarchical namespace → ADLS Gen2
        "--subscription", sub,
    ], check=False)
    _log(f"Storage account '{sa}' ready.")

    # Verify it exists
    sa_info = _run_az([
        "storage", "account", "show",
        "--name", sa,
        "--resource-group", rg,
        "--subscription", sub,
    ], check=False)
    if not sa_info.get("id"):
        raise RuntimeError(f"Storage account '{sa}' not found after create. Check RG '{rg}' exists and you have permissions.")

    # 1b — Container
    _log(f"Creating container '{ctr}'…")
    _run_az([
        "storage", "container", "create",
        "--name", ctr,
        "--account-name", sa,
        "--auth-mode", "login",
        "--subscription", sub,
    ], check=False)
    _log(f"Container '{ctr}' ready.")

    # 1c — Folders (virtual directories — create zero-byte marker blobs)
    for folder in cfg["folders"]:
        _log(f"Creating folder '{folder}'…")
        _run_az([
            "storage", "fs", "directory", "create",
            "--name", folder,
            "--file-system", ctr,
            "--account-name", sa,
            "--auth-mode", "login",
        ], check=False)
    _log("All folders created.")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 2 — Create Access Connector + Role Assignment
# ═══════════════════════════════════════════════════════════════════════════════

def create_access_connector(cfg):
    _log("═══ Step 2: Access Connector + Role Assignment ═══")

    sub  = cfg["subscription_id"]
    rg   = cfg["resource_group"]
    loc  = cfg["region"]
    ac   = cfg["access_connector"]
    sa   = cfg["storage_account"]

    # 2a — Create Access Connector (idempotent — check=False so "already exists" doesn't abort)
    _log(f"Creating Access Connector '{ac}'…")
    _run_az([
        "databricks", "access-connector", "create",
        "--name", ac,
        "--resource-group", rg,
        "--location", loc,
        "--identity-type", "SystemAssigned",
        "--subscription", sub,
    ], check=False)
    _log(f"Access Connector '{ac}' create request done.")

    # 2b — Always fetch via `show` to get the ID and principal (works even if already existed)
    _log(f"Fetching Access Connector details…")
    ac_info = _run_az([
        "databricks", "access-connector", "show",
        "--name", ac,
        "--resource-group", rg,
        "--subscription", sub,
    ], check=False)

    connector_id = ac_info.get("id", "")
    principal_id = ac_info.get("identity", {}).get("principalId", "")

    if not connector_id:
        _log("Could not retrieve Access Connector ID! Check that the resource group and connector name are correct.", "ERROR")
        _log(f"  az response: {json.dumps(ac_info)[:400]}", "ERROR")
        raise RuntimeError(
            f"Access Connector '{ac}' not found in RG '{rg}'. "
            f"Verify the resource group exists and 'az databricks' extension is installed "
            f"(`az extension add --name databricks`)."
        )

    _log(f"Access Connector ID: {connector_id}")

    if principal_id:
        _log(f"Access Connector principal ID: {principal_id}")
        # 2c — Assign "Storage Blob Data Contributor" on the storage account
        storage_scope = (
            f"/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Storage/storageAccounts/{sa}"
        )
        _log("Assigning Storage Blob Data Contributor role…")
        _run_az([
            "role", "assignment", "create",
            "--assignee-object-id", principal_id,
            "--assignee-principal-type", "ServicePrincipal",
            "--role", cfg.get("role_assignment", "Storage Blob Data Contributor"),
            "--scope", storage_scope,
            "--subscription", sub,
        ], check=False)   # may already exist — treat as warning
        _log("Role assignment complete.")
    else:
        _log("No principalId found — role assignment skipped (connector may not have SystemAssigned identity)", "WARN")

    return connector_id


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 3 — Register Storage Credential in Unity Catalog
# ═══════════════════════════════════════════════════════════════════════════════

def create_storage_credential(cfg, connector_id):
    """Register the Azure Access Connector as a Storage Credential in Unity Catalog."""
    _log("═══ Step 3: Register Storage Credential ═══")

    cred_name = cfg.get("storage_credential_name") or cfg["access_connector"]

    if not connector_id:
        _log("No connector_id provided — cannot create storage credential.", "ERROR")
        raise RuntimeError("Missing connector_id for storage credential")

    _log(f"Registering storage credential '{cred_name}' with connector: {connector_id}")
    payload = {
        "name": cred_name,
        "azure_managed_identity": {
            "access_connector_id": connector_id,
        },
        "comment": f"Auto-created from Access Connector {cfg['access_connector']}",
    }
    ok, body = _databricks_api(
        "POST",
        "/api/2.1/unity-catalog/storage-credentials",
        cfg,
        payload,
    )
    if ok:
        _log(f"Storage credential '{cred_name}' registered.")
    elif "already exists" in json.dumps(body).lower():
        _log(f"Storage credential '{cred_name}' already exists — skipping.", "WARN")
    else:
        _log(f"Failed to create storage credential: {body}", "ERROR")
        raise RuntimeError(f"Storage credential creation failed: {body}")

    return cred_name


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 4 — Create External Locations  (Databricks Unity Catalog API)
# ═══════════════════════════════════════════════════════════════════════════════

def create_external_locations(cfg, credential_name):
    _log("═══ Step 4: External Locations ═══")

    for loc_name, url in cfg["external_locations"].items():
        _log(f"Creating external location '{loc_name}' → {url}")
        payload = {
            "name"            : loc_name,
            "url"             : url,
            "credential_name" : credential_name,
        }
        ok, body = _databricks_api(
            "POST",
            "/api/2.1/unity-catalog/external-locations",
            cfg,
            payload,
        )
        if ok:
            _log(f"External location '{loc_name}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"External location '{loc_name}' already exists — skipping.", "WARN")
        else:
            _log(f"Failed to create external location '{loc_name}': {body}", "ERROR")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 5 — Create Catalogs  (Databricks Unity Catalog API)
# ═══════════════════════════════════════════════════════════════════════════════

def create_catalogs(cfg):
    _log("═══ Step 5: Catalogs ═══")

    for catalog_name, cat_cfg in cfg["catalogs"].items():
        # Support both old format (string) and new format ({location, schemas})
        if isinstance(cat_cfg, str):
            storage_root = cat_cfg
            schemas = ["default"]
        else:
            storage_root = cat_cfg.get("location", "")
            schemas = cat_cfg.get("schemas", ["default"]) or ["default"]

        _log(f"Creating catalog '{catalog_name}' → {storage_root}")

        # Use storage_root (top-level field) — required when metastore has no default root
        payload = {
            "name"         : catalog_name,
            "comment"      : f"Auto-created catalog for {catalog_name}",
            "storage_root" : storage_root,
        }
        ok, body = _databricks_api(
            "POST",
            "/api/2.1/unity-catalog/catalogs",
            cfg,
            payload,
        )
        if ok:
            _log(f"Catalog '{catalog_name}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"Catalog '{catalog_name}' already exists — skipping.", "WARN")
        else:
            _log(f"Failed to create catalog '{catalog_name}': {body}", "ERROR")
            continue

        # Create all specified schemas
        for schema_name in schemas:
            schema_payload = {
                "name"        : schema_name,
                "catalog_name": catalog_name,
                "comment"     : f"Schema {schema_name}",
            }
            ok2, body2 = _databricks_api(
                "POST",
                "/api/2.1/unity-catalog/schemas",
                cfg,
                schema_payload,
            )
            if ok2:
                _log(f"  Schema '{catalog_name}.{schema_name}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{catalog_name}.{schema_name}' already exists — OK.", "WARN")
            else:
                _log(f"  Failed to create schema '{catalog_name}.{schema_name}': {body2}", "ERROR")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 5 — Create Volume  (Databricks Unity Catalog API)
# ═══════════════════════════════════════════════════════════════════════════════

def create_volume(cfg):
    _log("═══ Step 6: Volume ═══")

    vol_name  = cfg["volume_name"]
    catalog   = cfg["volume_catalog"]
    schema    = cfg["volume_schema"]
    vol_path  = cfg["volume_path"]

    # Auto-create the schema if it doesn't exist
    _log(f"Ensuring schema '{catalog}.{schema}' exists…")
    schema_payload = {
        "name"        : schema,
        "catalog_name": catalog,
        "comment"     : f"Schema {schema}",
    }
    sok, sbody = _databricks_api(
        "POST",
        "/api/2.1/unity-catalog/schemas",
        cfg,
        schema_payload,
    )
    if sok:
        _log(f"Schema '{catalog}.{schema}' created.")
    elif "already exists" in json.dumps(sbody).lower():
        _log(f"Schema '{catalog}.{schema}' already exists — OK.", "WARN")
    else:
        _log(f"Failed to create schema '{catalog}.{schema}': {sbody}", "ERROR")
        _log("Volume creation may fail if the schema doesn't exist.", "WARN")

    _log(f"Creating volume '{catalog}.{schema}.{vol_name}' → {vol_path}")
    payload = {
        "name"            : vol_name,
        "catalog_name"    : catalog,
        "schema_name"     : schema,
        "volume_type"     : "EXTERNAL",
        "storage_location": vol_path,
        "comment"         : "Landing zone external volume",
    }
    ok, body = _databricks_api(
        "POST",
        "/api/2.1/unity-catalog/volumes",
        cfg,
        payload,
    )
    if ok:
        _log(f"Volume '{vol_name}' created.")
    elif "already exists" in json.dumps(body).lower():
        _log(f"Volume '{vol_name}' already exists — skipping.", "WARN")
    else:
        _log(f"Failed to create volume: {body}", "ERROR")


# ═══════════════════════════════════════════════════════════════════════════════
#  Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_all(cfg):
    """Execute every infra step in order."""
    _log("╔════════════════════════════════════════════════════════════════╗")
    _log("║  Unity Catalog Infrastructure — Automated Setup              ║")
    _log("╚════════════════════════════════════════════════════════════════╝")
    _log(f"Subscription : {cfg['subscription_id']}")
    _log(f"Region       : {cfg['region']}")
    _log(f"Storage Acct : {cfg['storage_account']}")
    _log("")

    # Ensure correct subscription
    _run_az(["account", "set", "--subscription", cfg["subscription_id"]])

    # Step 1 — Azure Storage
    create_storage(cfg)

    # Step 2 — Access Connector + RBAC
    connector_id = create_access_connector(cfg)

    # Step 3-6 — Databricks Unity Catalog (requires Databricks credentials)
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        # Step 3 — Register Storage Credential
        cred_name = create_storage_credential(cfg, connector_id)
        # Step 4 — External Locations
        create_external_locations(cfg, cred_name)
        # Step 5 — Catalogs
        create_catalogs(cfg)
        # Step 6 — Volume
        create_volume(cfg)
    else:
        _log("DATABRICKS_HOST / DATABRICKS_TOKEN not set — skipping Databricks API steps (3-6).", "WARN")
        _log("Set env-vars and re-run, or create these objects manually in the Databricks UI.")

    _log("")
    _log("╔════════════════════════════════════════════════════════════════╗")
    _log("║  Infrastructure setup complete                               ║")
    _log("╚════════════════════════════════════════════════════════════════╝")


# ═══════════════════════════════════════════════════════════════════════════════
#  API-Callable Orchestrator  (step-by-step results, no sys.exit)
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_api(cfg):
    """Execute every infra step and return structured results with logs.

    Returns dict:
        success : bool
        steps   : [{step, name, status, message, logs}]
        summary : str
    """
    import io, contextlib

    steps = []

    def _run_step(step_num, name, fn, *args, **kwargs):
        """Run a single step, capturing stdout/stderr and exceptions."""
        buf = io.StringIO()
        entry = {"step": step_num, "name": name, "status": "running", "message": "", "logs": ""}
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                result = fn(*args, **kwargs)
            entry["status"] = "success"
            entry["message"] = f"{name} completed successfully"
            entry["logs"] = buf.getvalue()
            return result
        except Exception as e:
            entry["status"] = "error"
            entry["message"] = str(e)[:500]
            entry["logs"] = buf.getvalue()
            return None
        finally:
            steps.append(entry)

    # Step 0 — Set subscription
    _run_step(0, "Set Azure Subscription", _run_az,
              ["account", "set", "--subscription", cfg["subscription_id"]])

    # Step 1 — Storage
    _run_step(1, "Create Storage Account + Container + Folders", create_storage, cfg)

    # Step 2 — Access Connector
    connector_id = _run_step(2, "Create Access Connector + RBAC", create_access_connector, cfg)

    # Steps 3-6 require Databricks credentials
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        # Gate: If connector_id is missing, we cannot proceed with steps 3-6
        if not connector_id:
            msg = ("Access Connector ID not available (Step 2 failed). "
                   "Cannot create Storage Credential, External Locations, Catalogs, or Volume. "
                   "Fix Step 2 errors and retry.")
            for skip_step, skip_name in [
                (3, "Register Storage Credential"),
                (4, "Create External Locations"),
                (5, "Create Unity Catalogs"),
                (6, "Create Volume"),
            ]:
                steps.append({"step": skip_step, "name": skip_name,
                              "status": "skipped", "message": msg, "logs": ""})
        else:
            cred_name = _run_step(3, "Register Storage Credential in Unity Catalog",
                                  create_storage_credential, cfg, connector_id)
            if cred_name:
                _run_step(4, "Create External Locations",
                          create_external_locations, cfg, cred_name)
            else:
                steps.append({"step": 4, "name": "Create External Locations",
                              "status": "skipped",
                              "message": "Storage Credential not available (Step 3 failed)",
                              "logs": ""})
            _run_step(5, "Create Unity Catalogs", create_catalogs, cfg)
            _run_step(6, "Create Volume", create_volume, cfg)
    else:
        steps.append({"step": 3, "name": "Databricks API Steps (3-6)",
                      "status": "skipped",
                      "message": "No databricks_host/databricks_token — skipped Storage Credential, External Locations, Catalogs & Volume",
                      "logs": ""})

    failed = [s for s in steps if s["status"] == "error"]
    return {
        "success": len(failed) == 0,
        "steps":   steps,
        "summary": f"{len(steps)} steps executed, {len(failed)} failed" if failed
                   else f"All {len(steps)} steps completed successfully",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Streaming Orchestrator  (yields JSON events per step — for SSE)
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_streaming(cfg):
    """Generator: yields one JSON-serialisable dict per step as it completes.

    Each yielded dict has:
        event : "step" | "done"
        step  : int
        name  : str
        status: "running" | "success" | "error" | "skipped"
        message: str
        logs  : str
    Final yield has event="done" with summary info.
    """
    import io, contextlib

    all_steps = []

    def _do_step(step_num, name, fn, *args, **kwargs):
        buf = io.StringIO()
        entry = {"event": "step", "step": step_num, "name": name,
                 "status": "success", "message": "", "logs": ""}
        result = None
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                result = fn(*args, **kwargs)
            entry["status"] = "success"
            entry["message"] = f"{name} completed successfully"
        except Exception as e:
            entry["status"] = "error"
            entry["message"] = str(e)[:500]
            result = None
        entry["logs"] = buf.getvalue()
        all_steps.append(entry)
        return entry, result

    # Step 0 — Set subscription
    yield {"event": "step", "step": 0, "name": "Set Azure Subscription",
           "status": "running", "message": "Setting subscription…", "logs": ""}
    entry, _ = _do_step(0, "Set Azure Subscription", _run_az,
                        ["account", "set", "--subscription", cfg["subscription_id"]])
    yield entry

    # Step 1 — Storage
    yield {"event": "step", "step": 1, "name": "Create Storage Account + Container + Folders",
           "status": "running", "message": "Creating storage…", "logs": ""}
    entry, _ = _do_step(1, "Create Storage Account + Container + Folders", create_storage, cfg)
    yield entry

    # Step 2 — Access Connector
    yield {"event": "step", "step": 2, "name": "Create Access Connector + RBAC",
           "status": "running", "message": "Creating access connector…", "logs": ""}
    entry, connector_id = _do_step(2, "Create Access Connector + RBAC", create_access_connector, cfg)
    yield entry

    # Steps 3-6 require Databricks credentials
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        if not connector_id:
            msg = ("Access Connector ID not available (Step 2 failed). "
                   "Cannot create Storage Credential, External Locations, Catalogs, or Volume.")
            for skip_step, skip_name in [
                (3, "Register Storage Credential"), (4, "Create External Locations"),
                (5, "Create Unity Catalogs"), (6, "Create Volume"),
            ]:
                skip_entry = {"event": "step", "step": skip_step, "name": skip_name,
                              "status": "skipped", "message": msg, "logs": ""}
                all_steps.append(skip_entry)
                yield skip_entry
        else:
            # Step 3
            yield {"event": "step", "step": 3, "name": "Register Storage Credential",
                   "status": "running", "message": "Registering credential…", "logs": ""}
            entry, cred_name = _do_step(3, "Register Storage Credential",
                                        create_storage_credential, cfg, connector_id)
            yield entry

            # Step 4
            if cred_name:
                yield {"event": "step", "step": 4, "name": "Create External Locations",
                       "status": "running", "message": "Creating external locations…", "logs": ""}
                entry, _ = _do_step(4, "Create External Locations",
                                    create_external_locations, cfg, cred_name)
                yield entry
            else:
                skip_entry = {"event": "step", "step": 4, "name": "Create External Locations",
                              "status": "skipped",
                              "message": "Storage Credential not available (Step 3 failed)", "logs": ""}
                all_steps.append(skip_entry)
                yield skip_entry

            # Step 5
            yield {"event": "step", "step": 5, "name": "Create Unity Catalogs",
                   "status": "running", "message": "Creating catalogs…", "logs": ""}
            entry, _ = _do_step(5, "Create Unity Catalogs", create_catalogs, cfg)
            yield entry

            # Step 6
            yield {"event": "step", "step": 6, "name": "Create Volume",
                   "status": "running", "message": "Creating volume…", "logs": ""}
            entry, _ = _do_step(6, "Create Volume", create_volume, cfg)
            yield entry
    else:
        skip_entry = {"event": "step", "step": 3, "name": "Databricks API Steps (3-6)",
                      "status": "skipped",
                      "message": "No databricks_host/databricks_token — skipped", "logs": ""}
        all_steps.append(skip_entry)
        yield skip_entry

    failed = [s for s in all_steps if s["status"] == "error"]
    yield {
        "event": "done",
        "success": len(failed) == 0,
        "steps": all_steps,
        "summary": f"{len(all_steps)} steps executed, {len(failed)} failed" if failed
                   else f"All {len(all_steps)} steps completed successfully",
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def _prompt_credentials(cfg):
    """Prompt for Databricks host/token if they are not already set."""
    if not cfg["databricks_host"]:
        cfg["databricks_host"] = input("Enter Databricks workspace URL (e.g. https://adb-xxx.azuredatabricks.net): ").strip()
    if not cfg["databricks_token"]:
        cfg["databricks_token"] = input("Enter Databricks PAT: ").strip()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unity Catalog Infra Setup")
    parser.add_argument("--auto", action="store_true", help="Skip prompts — use env-vars only")
    args = parser.parse_args()

    cfg = dict(CONFIG)

    if not args.auto:
        _prompt_credentials(cfg)

    run_all(cfg)
