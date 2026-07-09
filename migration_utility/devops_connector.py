"""Azure DevOps Git connector — push files to a repo without local git clone.

Uses the Azure DevOps REST API (Git Pushes) to atomically commit files
(DDL .sql + ER diagram PNG + optional model JSON) to a branch.
"""
import base64
import requests
from log_config import get_logger

logger = get_logger(__name__)

_API_VERSION = "7.1-preview.1"


def _headers(pat: str) -> dict:
    """Build auth headers using PAT (Basic auth with empty username)."""
    b64 = base64.b64encode(f":{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {b64}",
        "Content-Type": "application/json",
    }


def _api_url(org: str, project: str, repo: str, path: str) -> str:
    """Build Azure DevOps REST API URL."""
    org = org.strip().rstrip("/")
    # Support full URL or just org name
    if org.startswith("http"):
        base = org.rstrip("/")
    else:
        base = f"https://dev.azure.com/{org}"
    return f"{base}/{project}/_apis/git/repositories/{repo}/{path}?api-version={_API_VERSION}"


def _get_branch_ref(org: str, project: str, repo: str, branch: str, pat: str) -> str | None:
    """Get the objectId of a branch (returns None if branch doesn't exist)."""
    url = _api_url(org, project, repo, "refs")
    url += f"&filter=heads/{branch}"
    resp = requests.get(url, headers=_headers(pat), timeout=30)
    if resp.status_code != 200:
        return None
    refs = resp.json().get("value", [])
    for ref in refs:
        if ref.get("name") == f"refs/heads/{branch}":
            return ref.get("objectId")
    return None


def _get_default_branch_ref(org: str, project: str, repo: str, pat: str) -> tuple[str, str]:
    """Get the default branch name and objectId."""
    url = _api_url(org, project, repo, "")
    # Remove trailing path separator
    url = url.replace("//", "/").replace("/?", "?")
    resp = requests.get(url, headers=_headers(pat), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Cannot access repo: HTTP {resp.status_code} — {resp.text[:200]}")
    data = resp.json()
    default_branch = data.get("defaultBranch", "refs/heads/main")
    # Get the ref objectId
    branch_name = default_branch.replace("refs/heads/", "")
    ref_id = _get_branch_ref(org, project, repo, branch_name, pat)
    if not ref_id:
        raise RuntimeError(f"Cannot find default branch '{branch_name}' ref")
    return branch_name, ref_id


def push_files_to_repo(
    org: str,
    project: str,
    repo: str,
    branch: str,
    files: list[dict],
    commit_message: str,
    pat: str,
) -> dict:
    """Push files to an Azure DevOps Git repo in a single atomic commit.

    Args:
        org: Azure DevOps organization name or full URL
        project: Project name
        repo: Repository name
        branch: Target branch name (created from default if missing)
        files: List of dicts with keys: path, content, encoding
               encoding: "utf-8" for text, "base64" for binary
        commit_message: Commit message
        pat: Personal Access Token

    Returns:
        dict with commit_id, url, success
    """
    if not org or not project or not repo or not pat:
        raise ValueError("org, project, repo, and pat are all required")
    if not files:
        raise ValueError("No files to push")

    # Check if target branch exists
    branch_ref = _get_branch_ref(org, project, repo, branch, pat)

    if not branch_ref:
        # Create branch from default branch
        logger.info("Branch '%s' not found, creating from default branch", branch)
        _, default_ref = _get_default_branch_ref(org, project, repo, pat)
        # Create the branch via refs endpoint
        create_url = _api_url(org, project, repo, "refs")
        create_body = [{
            "name": f"refs/heads/{branch}",
            "oldObjectId": "0000000000000000000000000000000000000000",
            "newObjectId": default_ref,
        }]
        resp = requests.post(create_url, json=create_body, headers=_headers(pat), timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Failed to create branch '{branch}': {resp.status_code} — {resp.text[:300]}")
        branch_ref = default_ref
        logger.info("Created branch '%s' from default", branch)

    # Check which files already exist (for edit vs add)
    changes = []
    for f in files:
        file_path = f["path"].lstrip("/")
        # Check if file exists on this branch
        item_url = _api_url(org, project, repo, f"items")
        item_url += f"&path=/{file_path}&versionDescriptor.version={branch}&versionDescriptor.versionType=branch"
        item_resp = requests.get(item_url, headers=_headers(pat), timeout=15)
        change_type = "edit" if item_resp.status_code == 200 else "add"

        if f.get("encoding") == "base64":
            content_type = "base64Encoded"
            content = f["content"]
        else:
            content_type = "rawText"
            content = f["content"]

        changes.append({
            "changeType": change_type,
            "item": {"path": f"/{file_path}"},
            "newContent": {
                "content": content,
                "contentType": content_type,
            },
        })

    # Build push payload
    push_body = {
        "refUpdates": [{
            "name": f"refs/heads/{branch}",
            "oldObjectId": branch_ref,
        }],
        "commits": [{
            "comment": commit_message,
            "changes": changes,
        }],
    }

    push_url = _api_url(org, project, repo, "pushes")
    resp = requests.post(push_url, json=push_body, headers=_headers(pat), timeout=60)

    if resp.status_code in (200, 201):
        data = resp.json()
        commits = data.get("commits", [{}])
        commit_id = commits[0].get("commitId", "") if commits else ""
        # Build web URL to the commit
        org_clean = org.strip().rstrip("/")
        if not org_clean.startswith("http"):
            org_clean = f"https://dev.azure.com/{org_clean}"
        web_url = f"{org_clean}/{project}/_git/{repo}/commit/{commit_id}"
        logger.info("Successfully pushed %d files to %s/%s branch '%s'", len(files), project, repo, branch)
        return {"success": True, "commit_id": commit_id, "url": web_url, "files_pushed": len(files)}
    else:
        error_msg = resp.text[:500]
        logger.error("Push failed: HTTP %d — %s", resp.status_code, error_msg)
        raise RuntimeError(f"Push failed (HTTP {resp.status_code}): {error_msg}")
