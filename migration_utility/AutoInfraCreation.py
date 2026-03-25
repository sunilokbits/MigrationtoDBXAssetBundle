"""
AutoInfraCreation — Automated Unity Catalog Infrastructure Setup
Creates Azure Storage, Access Connector, External Locations, Volume, and Catalogs
for an external Unity Catalog on Azure Databricks.

Uses Azure Python SDK (no Azure CLI dependency).

Usage:
    python AutoInfraCreation.py                 # interactive — prompts for credentials
    python AutoInfraCreation.py --auto          # uses env-vars for all credentials
"""

import os
import sys
import json
import time
import uuid
import argparse
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


# Cache credential so the browser prompt only appears once per session
_CACHED_CREDENTIAL = None

def _get_azure_credential():
    """Return an Azure credential.

    Tries DefaultAzureCredential first (env-vars, managed-identity, VS Code,
    Azure CLI if installed).  If that fails, falls back to
    InteractiveBrowserCredential which opens a browser window for login.
    """
    global _CACHED_CREDENTIAL
    if _CACHED_CREDENTIAL is not None:
        return _CACHED_CREDENTIAL

    from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential

    try:
        cred = DefaultAzureCredential()
        # Force a token request to verify it works
        cred.get_token("https://management.azure.com/.default")
        _log("Authenticated via DefaultAzureCredential.")
        _CACHED_CREDENTIAL = cred
        return cred
    except Exception:
        _log("DefaultAzureCredential unavailable — opening browser for interactive login…", "WARN")

    cred = InteractiveBrowserCredential()
    cred.get_token("https://management.azure.com/.default")  # triggers browser
    _log("Authenticated via interactive browser login.")
    _CACHED_CREDENTIAL = cred
    return cred


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
#  Step 0 — Verify Azure Credentials  (Python SDK — no CLI needed)
# ═══════════════════════════════════════════════════════════════════════════════

def set_subscription(cfg):
    """Verify Azure credentials and subscription access via Python SDK."""
    from azure.mgmt.resource import ResourceManagementClient

    sub = cfg["subscription_id"]
    _log(f"Authenticating to Azure (subscription: {sub})…")
    credential = _get_azure_credential()
    # Verify we can access the subscription by listing resource groups
    rm_client = ResourceManagementClient(credential, sub)
    rg_name = cfg["resource_group"]
    try:
        rg = rm_client.resource_groups.get(rg_name)
        _log(f"Verified: Resource Group '{rg_name}' exists in '{rg.location}'")
    except Exception as e:
        if "not found" in str(e).lower() or "could not be found" in str(e).lower():
            _log(f"Resource Group '{rg_name}' not found — creating in '{cfg['region']}'…")
            rm_client.resource_groups.create_or_update(rg_name, {"location": cfg["region"]})
            _log(f"Resource Group '{rg_name}' created.")
        else:
            raise
    _log("Azure authentication successful.")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 1 — Create Storage Account + Container + Folders  (Python SDK)
# ═══════════════════════════════════════════════════════════════════════════════

def create_storage(cfg):
    _log("═══ Step 1: Storage Account + Container + Folders ═══")

    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.storage.models import (
        StorageAccountCreateParameters, Sku, Kind,
    )
    from azure.storage.filedatalake import DataLakeServiceClient

    sub  = cfg["subscription_id"]
    rg   = cfg["resource_group"]
    sa   = cfg["storage_account"]
    loc  = cfg["region"]
    ctr  = cfg["container"]
    credential = _get_azure_credential()

    # 1a — Storage account (HNS enabled for ADLS Gen2)
    _log(f"Creating storage account '{sa}' in '{loc}'…")
    storage_client = StorageManagementClient(credential, sub)
    try:
        existing = storage_client.storage_accounts.get_properties(rg, sa)
        _log(f"Storage account '{sa}' already exists — OK.", "WARN")
    except Exception:
        # Create the account
        params = StorageAccountCreateParameters(
            sku=Sku(name="Standard_LRS"),
            kind=Kind.STORAGE_V2,
            location=loc,
            is_hns_enabled=True,  # hierarchical namespace → ADLS Gen2
        )
        poller = storage_client.storage_accounts.begin_create(rg, sa, params)
        poller.result()  # wait for completion
        _log(f"Storage account '{sa}' created.")

    # Verify it exists
    try:
        sa_info = storage_client.storage_accounts.get_properties(rg, sa)
        _log(f"Storage account '{sa}' verified (id: {sa_info.id[:80]}…)")
    except Exception as e:
        raise RuntimeError(
            f"Storage account '{sa}' not found after create. "
            f"Check RG '{rg}' exists and you have permissions. Error: {e}"
        )

    # 1b & 1c — Container + Folders using DataLake SDK
    account_url = f"https://{sa}.dfs.core.windows.net"
    datalake_client = DataLakeServiceClient(account_url=account_url, credential=credential)

    _log(f"Creating container (filesystem) '{ctr}'…")
    try:
        fs_client = datalake_client.create_file_system(ctr)
        _log(f"Container '{ctr}' created.")
    except Exception as e:
        if "already exists" in str(e).lower() or "ContainerAlreadyExists" in str(e):
            _log(f"Container '{ctr}' already exists — OK.", "WARN")
            fs_client = datalake_client.get_file_system_client(ctr)
        else:
            raise

    # 1c — Folders (directories in ADLS)
    for folder in cfg.get("folders", []):
        _log(f"Creating folder '{folder}'…")
        try:
            dir_client = fs_client.create_directory(folder)
            _log(f"  Folder '{folder}' created.")
        except Exception as e:
            if "already exists" in str(e).lower() or "PathAlreadyExists" in str(e):
                _log(f"  Folder '{folder}' already exists — OK.", "WARN")
            else:
                _log(f"  Failed to create folder '{folder}': {e}", "ERROR")
    _log("All folders created.")


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 2 — Create Access Connector + Role Assignment  (Python SDK)
# ═══════════════════════════════════════════════════════════════════════════════

def create_access_connector(cfg):
    _log("═══ Step 2: Access Connector + Role Assignment ═══")

    from azure.mgmt.databricks import AzureDatabricksManagementClient
    from azure.mgmt.authorization import AuthorizationManagementClient

    sub  = cfg["subscription_id"]
    rg   = cfg["resource_group"]
    loc  = cfg["region"]
    ac   = cfg["access_connector"]
    sa   = cfg["storage_account"]
    credential = _get_azure_credential()

    # 2a — Create Access Connector via azure-mgmt-databricks
    _log(f"Creating Access Connector '{ac}'…")
    dbr_client = AzureDatabricksManagementClient(credential, sub)
    connector_body = {
        "location": loc,
        "identity": {"type": "SystemAssigned"},
    }
    try:
        poller = dbr_client.access_connectors.begin_create_or_update(rg, ac, connector_body)
        ac_result = poller.result()
        _log(f"Access Connector '{ac}' created/updated.")
    except Exception as e:
        _log(f"Access Connector create error: {e}", "ERROR")
        # Try to fetch it if it already exists
        try:
            ac_result = dbr_client.access_connectors.get(rg, ac)
            _log(f"Access Connector '{ac}' already exists — using it.", "WARN")
        except Exception as e2:
            raise RuntimeError(
                f"Access Connector '{ac}' not found in RG '{rg}'. Error: {e2}"
            )

    connector_id = ac_result.id
    principal_id = (ac_result.identity.principal_id
                    if ac_result.identity else None)

    if not connector_id:
        raise RuntimeError(
            f"Access Connector '{ac}' not found in RG '{rg}'. "
            f"Verify the resource group exists."
        )

    _log(f"Access Connector ID: {connector_id}")

    if principal_id:
        _log(f"Access Connector principal ID: {principal_id}")
        # 2c — Assign "Storage Blob Data Contributor" on the storage account
        storage_scope = (
            f"/subscriptions/{sub}/resourceGroups/{rg}"
            f"/providers/Microsoft.Storage/storageAccounts/{sa}"
        )
        role_name = cfg.get("role_assignment", "Storage Blob Data Contributor")
        _log(f"Assigning '{role_name}' role…")

        auth_client = AuthorizationManagementClient(credential, sub)

        # Find the role definition ID
        role_defs = list(auth_client.role_definitions.list(
            storage_scope,
            filter=f"roleName eq '{role_name}'"
        ))
        if not role_defs:
            _log(f"Role definition '{role_name}' not found!", "ERROR")
        else:
            role_def_id = role_defs[0].id
            assignment_name = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{principal_id}:{role_def_id}:{storage_scope}"))
            try:
                auth_client.role_assignments.create(
                    storage_scope,
                    assignment_name,
                    {
                        "role_definition_id": role_def_id,
                        "principal_id": principal_id,
                        "principal_type": "ServicePrincipal",
                    },
                )
                _log("Role assignment complete.")
            except Exception as e:
                if "already exists" in str(e).lower() or "RoleAssignmentExists" in str(e):
                    _log("Role assignment already exists — OK.", "WARN")
                else:
                    _log(f"Role assignment warning: {e}", "WARN")
                    _log(f"ACTION REQUIRED: Manually assign '{role_name}' role to the Access Connector's managed identity.", "WARN")
                    _log(f"  Principal ID : {principal_id}", "WARN")
                    _log(f"  Storage Acct : {sa}", "WARN")
                    _log(f"  Go to: Azure Portal → Storage Account '{sa}' → Access Control (IAM) → Add role assignment", "WARN")
                    _log("  External Locations will be created with skip_validation=true and can be validated later.", "WARN")
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
        elif "cloud_storage_access" in json.dumps(body).lower() or "abfsrestoperation" in json.dumps(body).lower():
            # Storage access not yet available (role assignment pending) — retry with skip_validation
            _log(f"Storage access validation failed — retrying '{loc_name}' with skip_validation=true…", "WARN")
            payload["skip_validation"] = True
            ok2, body2 = _databricks_api(
                "POST",
                "/api/2.1/unity-catalog/external-locations",
                cfg,
                payload,
            )
            if ok2:
                _log(f"External location '{loc_name}' created (validation skipped — assign Storage Blob Data Contributor role and validate later).", "WARN")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"External location '{loc_name}' already exists — OK.", "WARN")
            else:
                _log(f"Failed to create external location '{loc_name}' even with skip_validation: {body2}", "ERROR")
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

    # ── Create Reconciliation catalog if configured ──
    recon_cfg = cfg.get("reconciliation", {})
    if recon_cfg and recon_cfg.get("catalog"):
        r_cat = recon_cfg["catalog"]
        r_sch = recon_cfg.get("schema", "hr")
        r_loc = recon_cfg.get("location", "")
        _log(f"Creating reconciliation catalog '{r_cat}' → {r_loc}")
        payload = {"name": r_cat, "comment": "Reconciliation results catalog"}
        if r_loc:
            payload["storage_root"] = r_loc
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", cfg, payload)
        if ok:
            _log(f"Catalog '{r_cat}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"Catalog '{r_cat}' already exists — OK.", "WARN")
        else:
            _log(f"Failed to create reconciliation catalog: {body}", "ERROR")
        # Create schema
        if ok or "already exists" in json.dumps(body).lower():
            ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/schemas", cfg,
                {"name": r_sch, "catalog_name": r_cat, "comment": f"Reconciliation schema {r_sch}"})
            if ok2:
                _log(f"  Schema '{r_cat}.{r_sch}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{r_cat}.{r_sch}' already exists — OK.", "WARN")
            else:
                _log(f"  Failed to create schema '{r_cat}.{r_sch}': {body2}", "ERROR")

    # ── Create Logging catalog if configured ──
    log_cfg = cfg.get("logging", {})
    if log_cfg and log_cfg.get("catalog"):
        l_cat = log_cfg["catalog"]
        l_sch = log_cfg.get("schema", "hr")
        l_loc = log_cfg.get("location", "")
        _log(f"Creating logging catalog '{l_cat}' → {l_loc}")
        payload = {"name": l_cat, "comment": "Execution logging catalog"}
        if l_loc:
            payload["storage_root"] = l_loc
        ok, body = _databricks_api("POST", "/api/2.1/unity-catalog/catalogs", cfg, payload)
        if ok:
            _log(f"Catalog '{l_cat}' created.")
        elif "already exists" in json.dumps(body).lower():
            _log(f"Catalog '{l_cat}' already exists — OK.", "WARN")
        else:
            _log(f"Failed to create logging catalog: {body}", "ERROR")
        # Create schema
        if ok or "already exists" in json.dumps(body).lower():
            ok2, body2 = _databricks_api("POST", "/api/2.1/unity-catalog/schemas", cfg,
                {"name": l_sch, "catalog_name": l_cat, "comment": f"Logging schema {l_sch}"})
            if ok2:
                _log(f"  Schema '{l_cat}.{l_sch}' created.")
            elif "already exists" in json.dumps(body2).lower():
                _log(f"  Schema '{l_cat}.{l_sch}' already exists — OK.", "WARN")
            else:
                _log(f"  Failed to create schema '{l_cat}.{l_sch}': {body2}", "ERROR")


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

    # Step 0 — Verify Azure credentials
    set_subscription(cfg)

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

    # Step 0 — Verify Azure credentials
    _run_step(0, "Set Azure Subscription", set_subscription, cfg)

    # Step 1 — Storage
    _run_step(1, "Create Storage Account + Container + Folders", create_storage, cfg)

    # Step 2 — Access Connector
    connector_id = _run_step(2, "Create Access Connector + RBAC", create_access_connector, cfg)

    # Steps 3-6 require Databricks credentials
    if cfg.get("databricks_host") and cfg.get("databricks_token"):
        # Steps 3 & 4 need connector_id from Step 2
        if not connector_id:
            skip_msg = ("Access Connector ID not available (Step 2 failed). "
                        "Cannot create Storage Credential or External Locations. "
                        "Fix Step 2 errors and retry.")
            for skip_step, skip_name in [
                (3, "Register Storage Credential"),
                (4, "Create External Locations"),
            ]:
                steps.append({"step": skip_step, "name": skip_name,
                              "status": "skipped", "message": skip_msg, "logs": ""})
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

        # Steps 5 & 6 use Databricks API directly — no connector_id needed
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

    # Step 0 — Verify Azure credentials via SDK
    yield {"event": "step", "step": 0, "name": "Set Azure Subscription",
           "status": "running", "message": "Authenticating via Azure SDK…", "logs": ""}
    entry, _ = _do_step(0, "Set Azure Subscription", set_subscription, cfg)
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
        # Steps 3 & 4 need connector_id from Step 2
        if not connector_id:
            skip_msg = ("Access Connector ID not available (Step 2 failed). "
                        "Cannot create Storage Credential or External Locations.")
            for skip_step, skip_name in [
                (3, "Register Storage Credential"), (4, "Create External Locations"),
            ]:
                skip_entry = {"event": "step", "step": skip_step, "name": skip_name,
                              "status": "skipped", "message": skip_msg, "logs": ""}
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

        # Steps 5 & 6 use Databricks REST API directly — no connector_id needed
        yield {"event": "step", "step": 5, "name": "Create Unity Catalogs",
               "status": "running", "message": "Creating catalogs…", "logs": ""}
        entry, _ = _do_step(5, "Create Unity Catalogs", create_catalogs, cfg)
        yield entry

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
