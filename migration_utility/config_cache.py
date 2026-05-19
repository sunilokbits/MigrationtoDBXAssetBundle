"""Centralized deploy-config cache — load once, refresh on save.

Eliminates 30+ per-request reads of deployconfig.json.
Every blueprint should import get_config() instead of reading the file directly.
"""
import json, os, threading
from log_config import get_logger

logger = get_logger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEPLOY_CONFIG_PATH = os.path.join(_BASE_DIR, "deployconfig.json")

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
