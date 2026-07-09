"""Azure Key Vault helper — fetch secrets for source password and databricks token.

Secret names in Key Vault:
  - source-Azuresql-password   → source SQL password
  - databricks-token           → Databricks PAT token

Resolution order:
  1. In-memory cache
  2. Azure Key Vault via SDK (Managed Identity → DefaultAzureCredential)
  3. Azure Key Vault via Azure CLI (local dev — az invoked through PowerShell)
"""
import os
import subprocess
import threading
import json as _json
from log_config import get_logger

logger = get_logger(__name__)

_lock = threading.Lock()
_secret_cache: dict[str, str] = {}

# Well-known secret names
SOURCE_PASSWORD_SECRET = "source-Azuresql-password"
DATABRICKS_TOKEN_SECRET = "databricks-token"

MASKED_VALUE = "xxxxxxxxxxxxxxxxx"


def _get_keyvault_name() -> str:
    """Return Key Vault name from env var or config (avoid circular import)."""
    # Environment variable takes precedence (set via App Service config)
    env_name = os.environ.get("KEYVAULT_NAME", "").strip()
    if env_name:
        return env_name
    from config_cache import get_config
    cfg = get_config()
    return cfg.get("keyvault_name", "")


def _is_running_on_azure() -> bool:
    """Check if we're running on Azure App Service."""
    return bool(os.environ.get("WEBSITE_INSTANCE_ID") or os.environ.get("WEBSITE_SITE_NAME"))


def _build_client(vault_name: str):
    """Build a SecretClient for the given vault (only used on Azure)."""
    from azure.identity import ManagedIdentityCredential
    from azure.keyvault.secrets import SecretClient

    vault_url = f"https://{vault_name}.vault.azure.net"
    credential = ManagedIdentityCredential()
    return SecretClient(vault_url=vault_url, credential=credential)


def _fetch_via_cli(vault_name: str, secret_name: str) -> str:
    """Fetch a secret using Azure CLI (local dev fallback).

    Tries multiple approaches:
      1. Direct `az` command
      2. Python -m azure.cli (for venv-based az installs)
      3. PowerShell profile-loaded `az` function
    """
    # Possible az CLI Python executables (venv-based installs)
    az_python_paths = [
        r"c:\Live_MigrationProject\.az-venv\Scripts\python.exe",
        os.path.join(os.path.expanduser("~"), ".az-venv", "Scripts", "python.exe"),
    ]

    az_args = [
        "keyvault", "secret", "show",
        "--vault-name", vault_name,
        "--name", secret_name,
        "--query", "value",
        "-o", "tsv",
    ]

    # Attempt 1: Try python -m azure.cli directly (venv-based az)
    for py_path in az_python_paths:
        if os.path.isfile(py_path):
            try:
                result = subprocess.run(
                    [py_path, "-m", "azure.cli"] + az_args,
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode == 0 and result.stdout.strip():
                    return result.stdout.strip()
            except Exception as e:
                logger.debug("az via %s failed: %s", py_path, str(e)[:100])
            break

    # Attempt 2: Direct `az` on PATH
    try:
        result = subprocess.run(
            ["az"] + az_args,
            capture_output=True, text=True, timeout=30, shell=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass

    # Attempt 3: PowerShell with profile (loads az function)
    try:
        cmd = f'az keyvault secret show --vault-name "{vault_name}" --name "{secret_name}" --query "value" -o tsv'
        result = subprocess.run(
            ["powershell", "-Command", cmd],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass

    return ""


def get_secret(secret_name: str, force_refresh: bool = False) -> str:
    """Fetch a secret from Azure Key Vault with in-memory caching.

    Resolution order:
      1. In-memory cache
      2. Azure Key Vault via SDK (Managed Identity → DefaultAzureCredential)
      3. Azure CLI via PowerShell (local development)

    Returns empty string if secret is unavailable from all sources.
    """
    global _secret_cache

    if not force_refresh and secret_name in _secret_cache:
        return _secret_cache[secret_name]

    vault_name = _get_keyvault_name()
    if not vault_name:
        logger.debug("Key Vault not configured, skipping secret fetch for %s", secret_name)
        return ""

    with _lock:
        # Double-check after acquiring lock
        if not force_refresh and secret_name in _secret_cache:
            return _secret_cache[secret_name]

        # Attempt 1: Azure SDK with Managed Identity (only on Azure App Service)
        if _is_running_on_azure():
            try:
                client = _build_client(vault_name)
                secret = client.get_secret(secret_name)
                value = secret.value or ""
                _secret_cache[secret_name] = value
                logger.info("Successfully fetched secret '%s' from Key Vault '%s' via Managed Identity", secret_name, vault_name)
                return value
            except Exception as e:
                logger.debug("SDK fetch failed for '%s': %s", secret_name, str(e)[:150])

        # Attempt 2: Azure CLI (local dev + fallback)
        value = _fetch_via_cli(vault_name, secret_name)
        if value:
            _secret_cache[secret_name] = value
            logger.info("Successfully fetched secret '%s' from Key Vault '%s' via CLI", secret_name, vault_name)
            return value

        logger.warning("Failed to fetch secret '%s' from Key Vault '%s' (all methods exhausted)", secret_name, vault_name)
        return ""


def get_source_password() -> str:
    """Get the source Azure SQL password from Key Vault."""
    return get_secret(SOURCE_PASSWORD_SECRET)


def get_databricks_token() -> str:
    """Get the Databricks PAT token from Key Vault."""
    return get_secret(DATABRICKS_TOKEN_SECRET)


def clear_cache():
    """Clear the secret cache (e.g. after updating secrets)."""
    global _secret_cache
    with _lock:
        _secret_cache.clear()


def is_masked(value: str) -> bool:
    """Check if a value is the masked placeholder."""
    return value == MASKED_VALUE or (set(value) == {"x"} and len(value) >= 8)
