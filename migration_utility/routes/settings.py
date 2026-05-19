"""Settings blueprint — deploy config, clean metadata, infrastructure deployment."""
from flask import Blueprint, request, jsonify, Response
import os, json, requests, threading

from .auth import login_required
from log_config import get_logger
from config_cache import get_config, save_config, reload_config, DEPLOY_CONFIG_PATH
from unity_catalog_executor import UnityCatalogExecutor

logger = get_logger(__name__)
settings_bp = Blueprint("settings", __name__, url_prefix="/api/v1")

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Azure User Auth — Device Code Flow                                          ║
# ║  Lets the admin authenticate once; credential is cached for RBAC/infra.       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

_azure_user_cred = None          # cached credential after successful auth
_azure_auth_state = {            # shared state between thread and route
    "status": "idle",            # idle | pending | success | error
    "device_code_info": None,    # {message, user_code, verification_uri}
    "error": "",
}
_auth_lock = threading.Lock()


@settings_bp.route("/azure-auth", methods=["POST"])
@login_required
def start_azure_auth():
    """Start Azure device code flow. Returns a code the user enters at microsoft.com/devicelogin."""
    global _azure_user_cred, _azure_auth_state

    # If already authenticated, return immediately
    if _azure_user_cred:
        try:
            _azure_user_cred.get_token("https://management.azure.com/.default")
            return jsonify({"success": True, "status": "success", "message": "Already authenticated with Azure."})
        except Exception:
            _azure_user_cred = None  # token expired, re-auth

    with _auth_lock:
        if _azure_auth_state["status"] == "pending":
            info = _azure_auth_state["device_code_info"]
            return jsonify({
                "success": True,
                "status": "pending",
                "user_code": info.get("user_code", "") if info else "",
                "verification_uri": info.get("verification_uri", "https://microsoft.com/devicelogin") if info else "",
                "message": info.get("message", "") if info else "Waiting for device code...",
            })

    cfg = get_config() or {}
    tenant_id = cfg.get("azure_tenant_id", "")

    def _auth_thread():
        global _azure_user_cred
        try:
            from azure.identity import DeviceCodeCredential

            def _on_device_code(verification_uri, user_code, expires_on):
                with _auth_lock:
                    _azure_auth_state["status"] = "pending"
                    _azure_auth_state["device_code_info"] = {
                        "message": f"To sign in, use a web browser to open {verification_uri} and enter the code {user_code} to authenticate.",
                        "user_code": user_code,
                        "verification_uri": verification_uri,
                    }

            kwargs = {"prompt_callback": _on_device_code}
            if tenant_id:
                kwargs["tenant_id"] = tenant_id

            cred = DeviceCodeCredential(**kwargs)
            # This blocks until user completes login
            cred.get_token("https://management.azure.com/.default")
            _azure_user_cred = cred
            with _auth_lock:
                _azure_auth_state["status"] = "success"
                _azure_auth_state["error"] = ""
            logger.info("Azure device code auth succeeded")
        except Exception as e:
            with _auth_lock:
                _azure_auth_state["status"] = "error"
                _azure_auth_state["error"] = str(e)[:300]
            logger.error("Azure device code auth failed: %s", e)

    with _auth_lock:
        _azure_auth_state["status"] = "starting"
        _azure_auth_state["device_code_info"] = None
        _azure_auth_state["error"] = ""

    t = threading.Thread(target=_auth_thread, daemon=True)
    t.start()

    # Wait briefly for the device code to be generated
    import time
    for _ in range(20):
        time.sleep(0.5)
        with _auth_lock:
            if _azure_auth_state["device_code_info"]:
                info = _azure_auth_state["device_code_info"]
                return jsonify({
                    "success": True,
                    "status": "pending",
                    "user_code": info["user_code"],
                    "verification_uri": info["verification_uri"],
                    "message": info["message"],
                })
            if _azure_auth_state["status"] in ("error", "success"):
                break

    with _auth_lock:
        return jsonify({"status": _azure_auth_state["status"], "error": _azure_auth_state.get("error", "")})


@settings_bp.route("/azure-auth/status", methods=["GET"])
@login_required
def azure_auth_status():
    """Check if device code auth has completed."""
    with _auth_lock:
        return jsonify({
            "status": _azure_auth_state["status"],
            "authenticated": _azure_user_cred is not None,
            "error": _azure_auth_state.get("error", ""),
        })


@settings_bp.route("/azure-auth/logout", methods=["POST"])
@login_required
def azure_auth_logout():
    """Clear cached Azure credential."""
    global _azure_user_cred
    _azure_user_cred = None
    with _auth_lock:
        _azure_auth_state["status"] = "idle"
        _azure_auth_state["device_code_info"] = None
    return jsonify({"success": True, "message": "Azure credential cleared."})


def _get_best_credential(cfg=None):
    """Return the best available Azure credential: user auth > SP > managed identity."""
    global _azure_user_cred

    # 1. User credential from device code flow (admin permissions)
    if _azure_user_cred:
        try:
            _azure_user_cred.get_token("https://management.azure.com/.default")
            return _azure_user_cred, "user"
        except Exception:
            _azure_user_cred = None

    # 2. Service Principal from config
    if cfg:
        sp_tenant = cfg.get("azure_tenant_id", "")
        sp_client = cfg.get("azure_client_id", "")
        sp_secret = cfg.get("azure_client_secret", "")
        if sp_tenant and sp_client and sp_secret:
            from azure.identity import ClientSecretCredential
            cred = ClientSecretCredential(tenant_id=sp_tenant, client_id=sp_client, client_secret=sp_secret)
            return cred, "service_principal"

    # 3. Managed identity / DefaultAzureCredential
    from azure.identity import DefaultAzureCredential
    return DefaultAzureCredential(), "managed_identity"


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Deploy Config — Settings (deployconfig.json)                                ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/deploy-config", methods=["GET"])
@login_required
def get_deploy_config():
    cfg = get_config()
    if not cfg:
        return jsonify({"success": True, "config": None, "message": "No config file found"})
    return jsonify({"success": True, "config": cfg})


@settings_bp.route("/deploy-config", methods=["POST"])
@login_required
def save_deploy_config():
    try:
        cfg = request.get_json()
        if not cfg:
            return jsonify({"success": False, "error": "No configuration data provided"}), 400
        save_config(cfg)
        return jsonify({"success": True, "message": "Configuration saved to deployconfig.json"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Clean Metadata — drop tables + purge ADLS table data                        ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/settings/clean-metadata", methods=["POST"])
@login_required
def clean_metadata():
    """Drop all UC tables and/or delete ADLS table data files across every catalog/schema."""
    try:
        body = request.get_json() or {}
        clean_adls   = body.get("clean_adls", True)
        clean_tables = body.get("clean_tables", True)

        if not clean_adls and not clean_tables:
            return jsonify({"success": False, "error": "Nothing selected to clean."}), 400

        # Load config
        cfg = get_config()
        if not cfg:
            return jsonify({"success": False, "error": "deployconfig.json not found."}), 400

        host  = cfg.get("databricks_host", "").rstrip("/")
        token = cfg.get("databricks_token", "")
        if not host or not token:
            return jsonify({"success": False, "error": "Databricks host or token missing in config."}), 400

        # Use PAT token if available; the UnityCatalogExecutor will auto-fallback
        # to Azure AD (managed identity) if the PAT is missing or expired (401/403).
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        sess    = requests.Session()
        sess.headers.update(headers)

        # Collect all catalog→schemas pairs from config
        targets = []
        for cat_name, cat_info in cfg.get("catalogs", {}).items():
            for sch in cat_info.get("schemas", []):
                targets.append((cat_name, sch, cat_info.get("location", "")))
        # Add reconciliation & logging catalogs
        for key in ("reconciliation", "logging"):
            info = cfg.get(key, {})
            if info.get("catalog") and info.get("schema"):
                targets.append((info["catalog"], info["schema"], info.get("location", "")))

        # Get a running SQL Warehouse
        uce = UnityCatalogExecutor(host, token)
        wh_resp = uce.list_warehouses()
        warehouses = wh_resp.get("warehouses", [])
        wh = next((w for w in warehouses if w["state"] == "RUNNING"), warehouses[0] if warehouses else None)
        if not wh:
            return jsonify({"success": False, "error": "No SQL Warehouse available."}), 400
        wh_id = wh["id"]

        log = []

        # ── 1. Drop tables ────────────────────────────────────────────────
        if clean_tables:
            for cat, sch, _ in targets:
                try:
                    show_sql = f"SHOW TABLES IN `{cat}`.`{sch}`"
                    res = uce._execute_statement(show_sql, wh_id, wait_timeout="30s")
                    stmt_id = res.get("statement_id")
                    if stmt_id:
                        poll = uce._poll_statement(stmt_id)
                        rows = poll.get("result", {}).get("data_array", [])
                    else:
                        rows = res.get("result", {}).get("data_array", [])
                    if not rows:
                        log.append(f"[TABLES] {cat}.{sch} — no tables found")
                        continue
                    for row in rows:
                        tbl = row[1] if len(row) > 1 else row[0]
                        drop_sql = f"DROP TABLE IF EXISTS `{cat}`.`{sch}`.`{tbl}`"
                        uce._execute_statement(drop_sql, wh_id, wait_timeout="30s")
                        log.append(f"[TABLES] Dropped {cat}.{sch}.{tbl}")
                except Exception as ex:
                    log.append(f"[TABLES] Error in {cat}.{sch}: {str(ex)[:200]}")

        # ── 2. Clean ADLS table data via Databricks SQL commands ──────────
        #    Uses the SQL Warehouse to run dbutils-style cleanup — avoids
        #    importing heavy Azure SDK modules that cause OOM on B1 plans.
        if clean_adls:
            # Collect ADLS paths from catalog locations
            adls_dirs = []
            for cat, sch, location in targets:
                if location:
                    adls_dirs.append((f"{cat}.{sch}", location))
            # Add landing/volume path
            vol_path = cfg.get("volume_path", "")
            if vol_path:
                adls_dirs.append(("landing", vol_path))

            if not adls_dirs:
                log.append("[ADLS] No ADLS locations found in config, skipped")
            else:
                for label, location in adls_dirs:
                    try:
                        # List directory contents via Databricks SQL
                        list_sql = f"LIST '{location}'"
                        res = uce._execute_statement(list_sql, wh_id, wait_timeout="30s")
                        stmt_id = res.get("statement_id")
                        if stmt_id:
                            poll = uce._poll_statement(stmt_id)
                            rows = poll.get("result", {}).get("data_array", [])
                        else:
                            rows = res.get("result", {}).get("data_array", [])

                        if not rows:
                            log.append(f"[ADLS] {label} — empty or inaccessible")
                            continue

                        PROTECTED = {"_unitystorage", "_delta_log", "_checkpoint", "_SUCCESS"}
                        cleaned = 0
                        for row in rows:
                            path = row[0] if row else ""
                            if not path:
                                continue
                            item_name = path.rstrip("/").split("/")[-1]
                            if item_name in PROTECTED or item_name.startswith("_unitystorage"):
                                log.append(f"[ADLS] Skipped protected: {item_name}")
                                continue
                            try:
                                drop_sql = f"DROP TABLE IF EXISTS `{label.split('.')[0]}`.`{label.split('.')[-1]}`.`{item_name}`"
                                uce._execute_statement(drop_sql, wh_id, wait_timeout="30s")
                                log.append(f"[ADLS] Cleaned {label}/{item_name}")
                                cleaned += 1
                            except Exception:
                                log.append(f"[ADLS] Could not clean {label}/{item_name}")
                        log.append(f"[ADLS] {label} — cleaned {cleaned} items")
                    except Exception as ex:
                        log.append(f"[ADLS] Error in {label}: {str(ex)[:200]}")

        return jsonify({"success": True, "log": log,
                        "summary": f"Cleaned {len([l for l in log if 'Dropped' in l or 'Deleted' in l or 'cleaned' in l])} items across {len(targets)} catalog/schema pairs."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Test Databricks Connection — validate host + PAT token                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/test-databricks", methods=["POST"])
@login_required
def test_databricks_connection():
    """Verify Databricks workspace host + PAT token by calling clusters list API."""
    try:
        data = request.get_json(silent=True) or {}
        host  = (data.get("databricks_host") or "").strip().rstrip("/")
        token = (data.get("databricks_token") or "").strip()
        if not host or not token:
            return jsonify({"success": False, "error": "Host URL and PAT Token are required"}), 400
        from databricks_connector import DatabricksConnector
        connector = DatabricksConnector(host, token)
        result = connector.test_connection()
        if result.get("success"):
            return jsonify({
                "success": True,
                "message": result.get("message", "Connected"),
                "workspace_host": result.get("workspace_host", host),
                "total_clusters": result.get("total_clusters", 0),
                "running_clusters": result.get("running_clusters", 0),
            })
        else:
            return jsonify({"success": False, "error": result.get("message", "Connection failed")})
    except Exception as e:
        logger.exception("Databricks connection test failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Test Storage Credential — validate UC credential can access ADLS            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/test-storage-credential", methods=["POST"])
@login_required
def test_storage_credential():
    """Validate a Unity Catalog storage credential by calling the Databricks API."""
    try:
        data = request.get_json(silent=True) or {}
        host  = (data.get("databricks_host") or "").strip().rstrip("/")
        token = (data.get("databricks_token") or "").strip()
        cred_name = (data.get("storage_credential_name") or "").strip()
        test_url  = (data.get("test_url") or "").strip()

        if not host or not token:
            return jsonify({"success": False, "error": "Databricks Host and Token are required (configure in Azure & Databricks section)"}), 400
        if not cred_name:
            return jsonify({"success": False, "error": "Storage Credential Name is required"}), 400

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        sess = requests.Session()
        sess.headers.update(headers)

        # Step 1: Check if the storage credential exists
        r = sess.get(f"{host}/api/2.1/unity-catalog/storage-credentials/{cred_name}")
        if r.status_code == 404:
            return jsonify({
                "success": False,
                "error": f"Storage credential '{cred_name}' not found in Unity Catalog.",
                "detail": "Create it first via Settings > Deploy Infrastructure, or manually in Databricks.",
            })
        if r.status_code not in (200, 201):
            err_msg = r.json().get("message", r.text[:300]) if r.headers.get("content-type", "").startswith("application/json") else r.text[:300]
            return jsonify({"success": False, "error": f"Failed to fetch credential: {err_msg}"})

        cred_info = r.json()
        cred_id = cred_info.get("id", "")
        cred_owner = cred_info.get("owner", "")
        azure_mi = cred_info.get("azure_managed_identity", {})
        connector_id = azure_mi.get("access_connector_id", "")

        result = {
            "success": True,
            "credential_name": cred_name,
            "credential_id": cred_id,
            "owner": cred_owner,
            "access_connector_id": connector_id,
            "read_only": cred_info.get("read_only", False),
        }

        # Step 2: Validate the credential if a test URL is provided
        if test_url:
            val_payload = {"storage_credential_name": cred_name, "url": test_url}
            vr = sess.post(f"{host}/api/2.1/unity-catalog/validate-storage-credentials", json=val_payload)
            if vr.status_code in (200, 201):
                val_result = vr.json()
                validations = val_result.get("results", [])
                all_ok = all(v.get("result") == "PASS" for v in validations)
                result["validation"] = {
                    "url": test_url,
                    "passed": all_ok,
                    "results": validations,
                }
                if not all_ok:
                    failed = [v for v in validations if v.get("result") != "PASS"]
                    result["success"] = False
                    result["error"] = f"Credential exists but validation failed for {test_url}"
                    result["failed_checks"] = [
                        f"{v.get('operation','?')}: {v.get('result','?')} - {v.get('message','')}"
                        for v in failed
                    ]
                else:
                    result["message"] = f"Storage credential '{cred_name}' is valid and can access {test_url}"
            else:
                val_err = ""
                try:
                    val_body = vr.json()
                    val_err = val_body.get("message", vr.text[:300])
                    # LOCATION_OVERLAP means an external location already uses this
                    # credential for the given path — the credential IS working.
                    # Check message text AND details.reason for overlap indicator
                    is_overlap = (
                        "overlaps with an existing external location" in val_err
                        or any(
                            d.get("reason") == "LOCATION_OVERLAP"
                            for d in val_body.get("details", [])
                            if isinstance(d, dict)
                        )
                    )
                    if is_overlap:
                        # Extract conflicting location name
                        loc_name = val_err.split("Conflicting location:")[-1].strip().rstrip(".")
                        result["validation"] = {"url": test_url, "passed": True, "overlap": True}
                        result["message"] = (
                            f"Storage credential '{cred_name}' is valid. "
                            f"External location '{loc_name}' already covers this path using this credential."
                        )
                        result["external_location"] = loc_name
                    else:
                        result["validation"] = {"url": test_url, "passed": False, "error": val_err}
                        result["success"] = False
                        result["error"] = f"Credential exists but validation request failed: {val_err}"
                except Exception:
                    val_err = vr.text[:300]
                    result["validation"] = {"url": test_url, "passed": False, "error": val_err}
                    result["success"] = False
                    result["error"] = f"Credential exists but validation request failed: {val_err}"
        else:
            result["message"] = f"Storage credential '{cred_name}' exists in Unity Catalog (owned by {cred_owner}). Provide a test URL for full validation."

        return jsonify(result)

    except Exception as e:
        logger.exception("Storage credential test failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Apply RBAC — assign role to App Service managed identity on storage acct    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/apply-rbac", methods=["POST"])
@login_required
def apply_rbac():
    """Assign the selected RBAC role to the App Service managed identity on the storage account."""
    try:
        cfg = get_config()
        if not cfg:
            return jsonify({"success": False, "error": "No config found. Save settings first."}), 400

        d = request.get_json() or {}
        role_name       = d.get("role_name") or cfg.get("role_assignment", "Storage Blob Data Owner")
        subscription_id = cfg.get("subscription_id", "")
        resource_group  = cfg.get("resource_group", "")
        storage_account = cfg.get("storage_account", "")

        if not subscription_id or not resource_group or not storage_account:
            return jsonify({"success": False, "error": "subscription_id, resource_group, and storage_account must be set in config."}), 400

        import uuid

        # ── Get best credential: user auth > SP > managed identity ─────────
        credential, cred_type = _get_best_credential(cfg)
        logger.info("RBAC: Using %s credential", cred_type)

        # Discover the App Service's own managed identity principal ID
        principal_id = None
        discover_error = ""

        # Method 1: On App Service, decode the managed identity token's oid claim
        try:
            import json as _json, base64 as _b64
            from azure.identity import DefaultAzureCredential as _DAC
            mi_cred = _DAC() if cred_type != "managed_identity" else credential
            token = mi_cred.get_token("https://management.azure.com/.default")
            payload = token.token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            claims = _json.loads(_b64.b64decode(payload))
            principal_id = claims.get("oid")
        except Exception:
            pass

        # Method 2: Query the specific App Service by name via azure-mgmt-web
        if not principal_id:
            try:
                from azure.mgmt.web import WebSiteManagementClient
                web_client = WebSiteManagementClient(credential, subscription_id)
                app_name = os.environ.get("WEBSITE_SITE_NAME", "")
                if app_name:
                    app = web_client.web_apps.get(resource_group, app_name)
                    if app.identity and app.identity.principal_id:
                        principal_id = app.identity.principal_id
                if not principal_id:
                    for app in web_client.web_apps.list_by_resource_group(resource_group):
                        if app.identity and app.identity.principal_id:
                            principal_id = app.identity.principal_id
                            break
            except Exception as ex:
                discover_error = str(ex)[:300]

        if not principal_id:
            return jsonify({"success": False, "error": f"Could not discover App Service managed identity. Ensure system-assigned identity is enabled. {discover_error}"}), 400

        storage_scope = (
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{storage_account}"
        )
        # For management roles (User Access Administrator, Contributor), scope to RG
        rg_scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        rg_level_roles = {"User Access Administrator", "Contributor", "Owner", "Reader"}
        assign_scope = rg_scope if role_name in rg_level_roles else storage_scope

        # ── Helper: assign role via direct Azure REST API ───────────────────
        def _assign_role_rest(p_id, scope, role, label="identity"):
            """Assign a role using raw REST API (bypasses SDK ABAC issues)."""
            mgmt_token = credential.get_token("https://management.azure.com/.default").token
            headers = {"Authorization": f"Bearer {mgmt_token}", "Content-Type": "application/json"}

            # 1. Find role definition ID
            role_url = (
                f"https://management.azure.com{scope}"
                f"/providers/Microsoft.Authorization/roleDefinitions"
                f"?api-version=2022-04-01&$filter=roleName eq '{role}'"
            )
            r = requests.get(role_url, headers=headers, timeout=15)
            if r.status_code != 200:
                return False, f"Could not look up role '{role}': {r.status_code}"
            defs = r.json().get("value", [])
            if not defs:
                return False, f"Role '{role}' not found"
            role_def_id = defs[0]["id"]

            # 2. Check if already assigned
            check_url = (
                f"https://management.azure.com{scope}"
                f"/providers/Microsoft.Authorization/roleAssignments"
                f"?api-version=2022-04-01&$filter=principalId eq '{p_id}'"
            )
            try:
                cr = requests.get(check_url, headers=headers, timeout=15)
                if cr.status_code == 200:
                    for ra in cr.json().get("value", []):
                        if ra.get("properties", {}).get("roleDefinitionId") == role_def_id:
                            return True, f"✓ '{role}' already assigned to {label} ({p_id})"
            except Exception:
                pass

            # 3. Create role assignment
            assignment_name = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{p_id}:{role_def_id}:{scope}"))
            create_url = (
                f"https://management.azure.com{scope}"
                f"/providers/Microsoft.Authorization/roleAssignments/{assignment_name}"
                f"?api-version=2022-04-01"
            )
            body = {
                "properties": {
                    "roleDefinitionId": role_def_id,
                    "principalId": p_id,
                    "principalType": "ServicePrincipal",
                }
            }
            cr = requests.put(create_url, headers=headers, json=body, timeout=20)
            if cr.status_code in (200, 201):
                return True, f"✓ '{role}' assigned to {label} ({p_id}) on {storage_account}"
            if cr.status_code == 409:
                return True, f"✓ '{role}' already assigned to {label} ({p_id})"
            return False, cr.json().get("error", {}).get("message", cr.text[:300])

        # ── Helper: assign role via az CLI subprocess ───────────────────────
        def _assign_role_cli(p_id, scope, role, label="identity"):
            """Fallback: try az CLI with SP or managed identity auth."""
            import subprocess
            sp_tenant = cfg.get("azure_tenant_id", "")
            sp_client = cfg.get("azure_client_id", "")
            sp_secret = cfg.get("azure_client_secret", "")
            # Login: prefer Service Principal, else managed identity
            try:
                if sp_tenant and sp_client and sp_secret:
                    login_cmd = [
                        "az", "login", "--service-principal",
                        "-u", sp_client, "-p", sp_secret, "--tenant", sp_tenant,
                    ]
                else:
                    login_cmd = ["az", "login", "--identity", "--allow-no-subscriptions"]
                login_result = subprocess.run(
                    login_cmd, capture_output=True, text=True, timeout=30
                )
                if login_result.returncode != 0:
                    logger.warning("az login failed: %s", login_result.stderr[:200])
            except FileNotFoundError:
                return False, "az CLI not available"
            except Exception as ex:
                logger.warning("az login error: %s", str(ex)[:200])

            cmd = [
                "az", "role", "assignment", "create",
                "--assignee-object-id", p_id,
                "--assignee-principal-type", "ServicePrincipal",
                "--role", role,
                "--scope", scope,
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    return True, f"✓ '{role}' assigned to {label} ({p_id}) via CLI"
                err = result.stderr or result.stdout
                if "already exists" in err.lower() or "RoleAssignmentExists" in err:
                    return True, f"✓ '{role}' already assigned to {label} ({p_id})"
                return False, err[:300]
            except FileNotFoundError:
                return False, "az CLI not available"
            except Exception as ex:
                return False, str(ex)[:300]

        # ── Main: try REST API first, then CLI, then show command ──────────
        msg = ""
        rest_error = ""
        ok, result_msg = _assign_role_rest(principal_id, assign_scope, role_name, "App Service")
        if ok:
            msg = result_msg
        else:
            rest_error = result_msg
            logger.warning("REST API RBAC failed: %s — trying az CLI", result_msg)
            ok, result_msg = _assign_role_cli(principal_id, assign_scope, role_name, "App Service")
            if ok:
                msg = result_msg
            else:
                logger.warning("az CLI RBAC also failed: %s", result_msg)
                cli_cmd = (
                    f'az role assignment create '
                    f'--assignee-object-id {principal_id} '
                    f'--assignee-principal-type ServicePrincipal '
                    f'--role "{role_name}" '
                    f'--scope "{assign_scope}"'
                )
                return jsonify({
                    "success": False,
                    "error": (
                        f"REST API: {rest_error}\n"
                        f"CLI: {result_msg}\n\n"
                        f"Run this CLI command manually:\n\n{cli_cmd}"
                    ),
                    "cli_command": cli_cmd,
                }), 200

        # ── Also assign the same role to the Access Connector if configured ──
        access_connector = cfg.get("access_connector", "")
        ac_msg = ""
        if access_connector:
            try:
                from azure.mgmt.databricks import DatabricksClient
                dbr_client = DatabricksClient(credential, subscription_id)
                connectors = list(dbr_client.access_connectors.list_by_resource_group(resource_group))
                ac_principal = None
                for c in connectors:
                    if c.name == access_connector and c.identity and c.identity.principal_id:
                        ac_principal = c.identity.principal_id
                        break
                if ac_principal:
                    ac_ok, ac_result = _assign_role_rest(ac_principal, assign_scope, role_name, f"Access Connector '{access_connector}'")
                    if ac_ok:
                        ac_msg = f" | {ac_result}"
                    else:
                        ac_ok, ac_result = _assign_role_cli(ac_principal, assign_scope, role_name, f"Access Connector '{access_connector}'")
                        if ac_ok:
                            ac_msg = f" | {ac_result}"
                        else:
                            ac_cli = (
                                f'az role assignment create '
                                f'--assignee-object-id {ac_principal} '
                                f'--assignee-principal-type ServicePrincipal '
                                f'--role "{role_name}" '
                                f'--scope "{assign_scope}"'
                            )
                            ac_msg = f" | For Access Connector, run: {ac_cli}"
                else:
                    ac_msg = f" | Access Connector '{access_connector}' not found or has no managed identity"
            except ImportError:
                ac_msg = " | Install azure-mgmt-databricks to auto-assign Access Connector role"
            except Exception as ex:
                ac_msg = f" | Access Connector lookup: {str(ex)[:150]}"

        # Update config to persist selected role
        cfg["role_assignment"] = role_name
        save_config(cfg)

        return jsonify({"success": True, "message": msg + ac_msg, "principal_id": principal_id})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)[:500]}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Deploy Infrastructure — runs AutoInfraCreation                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/deploy-infra", methods=["POST"])
@login_required
def deploy_infrastructure():
    """Read deployconfig.json → run infra setup. All config read from file."""
    try:
        from AutoInfraCreation import run_all_api, set_user_credential

        # Inject device-code / admin credential if available
        if _azure_user_cred:
            set_user_credential(_azure_user_cred)

        # Load saved config
        cfg = get_config()
        if not cfg:
            return jsonify({"success": False, "error": "No deployconfig.json found. Save settings first."}), 400

        # Validate required fields before attempting infra creation
        required = ["subscription_id", "resource_group", "region",
                     "storage_account", "access_connector",
                     "databricks_host", "databricks_token"]
        missing = [f for f in required if not cfg.get(f)]
        if missing:
            return jsonify({
                "success": False,
                "error": f"Missing required config fields: {', '.join(missing)}. "
                         "Go to Settings → save all fields first."
            }), 400

        # Ensure optional sections have sane defaults
        cfg.setdefault("external_locations", {})
        cfg.setdefault("catalogs", {})
        cfg.setdefault("folders", [])
        cfg.setdefault("container", "datalake")
        cfg.setdefault("role_assignment", "Storage Blob Data Owner")

        result = run_all_api(cfg)
        return jsonify(result)

    except Exception as e:
        logger.exception("Infrastructure deployment failed")
        return jsonify({"success": False, "error": str(e)}), 500


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Deploy Infrastructure — SSE Streaming (real-time progress)                  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

@settings_bp.route("/deploy-infra-stream")
@login_required
def deploy_infrastructure_stream():
    """SSE endpoint — streams step-by-step infrastructure deployment progress.

    All configuration (including Databricks creds) is read from deployconfig.json.
    """
    import importlib
    import AutoInfraCreation
    importlib.reload(AutoInfraCreation)          # pick up any code changes
    from AutoInfraCreation import run_all_streaming, set_user_credential

    # Inject device-code / admin credential if available
    if _azure_user_cred:
        set_user_credential(_azure_user_cred)

    cfg = get_config()
    if not cfg:
        def _err():
            yield 'data: ' + json.dumps({"event": "done", "success": False, "summary": "No deployconfig.json found. Save settings first."}) + '\n\n'
        return Response(_err(), mimetype='text/event-stream')

    # Validate required fields
    required = ["subscription_id", "resource_group", "region",
                 "storage_account", "access_connector",
                 "databricks_host", "databricks_token"]
    missing = [f for f in required if not cfg.get(f)]
    if missing:
        def _err():
            yield 'data: ' + json.dumps({"event": "done", "success": False, "summary": f"Missing required config: {', '.join(missing)}. Save settings first."}) + '\n\n'
        return Response(_err(), mimetype='text/event-stream')

    cfg.setdefault("external_locations", {})
    cfg.setdefault("catalogs", {})
    cfg.setdefault("folders", [])
    cfg.setdefault("container", "datalake")
    cfg.setdefault("role_assignment", "Storage Blob Data Owner")

    def generate():
        try:
            for evt in run_all_streaming(cfg):
                yield 'data: ' + json.dumps(evt) + '\n\n'
        except Exception as e:
            yield 'data: ' + json.dumps({"event": "done", "success": False, "summary": str(e)[:500]}) + '\n\n'

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})
