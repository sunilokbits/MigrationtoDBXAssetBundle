"""Centralized deploy-config cache — load once, refresh on save.

Eliminates 30+ per-request reads of deployconfig.json.
Every blueprint should import get_config() instead of reading the file directly.
"""
import json, os, threading, shutil
from log_config import get_logger

logger = get_logger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_CONFIG_PATH = os.path.join(_BASE_DIR, "deployconfig.json")

# On Azure App Service, /home is persistent across restarts.
# Locally, fall back to the app directory.
_PERSISTENT_DIR = "/home/migration_data" if os.path.isdir("/home") and os.access("/home", os.W_OK) else _BASE_DIR
os.makedirs(_PERSISTENT_DIR, exist_ok=True)
DEPLOY_CONFIG_PATH = os.path.join(_PERSISTENT_DIR, "deployconfig.json")

# On first run in Azure, seed from the bundled default if persistent copy doesn't exist
if not os.path.isfile(DEPLOY_CONFIG_PATH) and os.path.isfile(_DEFAULT_CONFIG_PATH):
    shutil.copy2(_DEFAULT_CONFIG_PATH, DEPLOY_CONFIG_PATH)
    logger.info("Seeded persistent config from bundled default: %s", DEPLOY_CONFIG_PATH)

_lock = threading.Lock()
_cache: dict | None = None


def get_config() -> dict:
    """Return the cached config dict (or load from disk on first call)."""
    global _cache
    if _cache is not None:
        return _cache
    return reload_config()


def reload_config() -> dict:
    """Force re-read from disk and update the cache.  Call after saving config."""
    global _cache
    with _lock:
        try:
            with open(DEPLOY_CONFIG_PATH, "r", encoding="utf-8") as f:
                _cache = json.load(f)
        except FileNotFoundError:
            logger.info("No deploy config file found at %s", DEPLOY_CONFIG_PATH)
            _cache = {}
        except Exception:
            logger.warning("Could not parse deploy config from %s", DEPLOY_CONFIG_PATH)
            _cache = {}
    return _cache


def save_config(cfg: dict) -> None:
    """Write config to disk AND update the in-memory cache."""
    global _cache
    with _lock:
        with open(DEPLOY_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        _cache = cfg


def get_source_password() -> str:
    """Get source password: resolve from Key Vault, fall back to config value."""
    from keyvault_helper import get_source_password as _kv_source_pw, is_masked
    # Try Key Vault first
    kv_val = _kv_source_pw()
    if kv_val:
        return kv_val
    # Fall back to config value (only if not masked)
    cfg = get_config()
    val = cfg.get("source", {}).get("password", "")
    return "" if is_masked(val) else val


def get_databricks_token() -> str:
    """Get databricks token: resolve from Key Vault, fall back to config value."""
    from keyvault_helper import get_databricks_token as _kv_dbr_token, is_masked
    # Try Key Vault first
    kv_val = _kv_dbr_token()
    if kv_val:
        return kv_val
    # Fall back to config value (only if not masked)
    cfg = get_config()
    val = cfg.get("databricks_token", "")
    return "" if is_masked(val) else val


def get_devops_token() -> str:
    """Get DevOps PAT: resolve from Key Vault, fall back to config value."""
    from keyvault_helper import get_devops_token as _kv_devops_token, is_masked
    # Try Key Vault first
    kv_val = _kv_devops_token()
    if kv_val:
        return kv_val
    # Fall back to config value (only if not masked)
    cfg = get_config()
    val = cfg.get("devops_pat", "")
    return "" if is_masked(val) else val
