"""
Azure Blob Storage uploader utilities.

Uploads exported files to Azure Blob Storage using account URI and key
from configuration. Requires azure-storage-blob to be installed.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _parse_account_and_container(account_uri: str) -> tuple[str, str | None, str]:
    """
    Parse account URL and optional container/prefix from an account URI.

    account_uri examples:
      - https://account.blob.core.windows.net
      - https://account.blob.core.windows.net/container
      - https://account.blob.core.windows.net/container/prefix

    Returns (account_url, container, prefix)
    """
    u = urlparse(account_uri)
    if not u.scheme.startswith("http"):
        raise ValueError("Storage account URI must start with http(s)://")
    account_url = f"{u.scheme}://{u.netloc}"
    # Strip leading slash
    path = (u.path or "/").lstrip("/")
    parts = [p for p in path.split("/") if p]
    container = parts[0] if parts else None
    prefix = "/".join(parts[1:]) if len(parts) > 1 else ""
    return account_url, container, prefix


def upload_paths_to_blob(
    account_uri: str,
    account_key: str,
    paths: Iterable[Path],
    *,
    default_container: str = "retail",
    blob_prefix: str = "",
) -> dict:
    """
    Upload a list of files to Azure Blob Storage.

    Args:
        account_uri: Azure Storage account URI (may include container and prefix)
        account_key: Storage account key
        paths: Iterable of Path objects to upload
        default_container: Container to use when not present in URI
        blob_prefix: Optional prefix to prepend to blob names

    Returns: Summary dict with counts
    """
    try:
        from azure.storage.blob import BlobServiceClient
    except Exception as e:
        raise ImportError(
            "Azure upload requires 'azure-storage-blob'. Install with: pip install azure-storage-blob"
        ) from e

    account_url, container, uri_prefix = _parse_account_and_container(account_uri)
    container = container or default_container
    # Combine uri_prefix and blob_prefix cleanly
    prefix_parts = [p for p in [uri_prefix, blob_prefix] if p]
    full_prefix = "/".join(prefix_parts)

    bsc = BlobServiceClient(account_url=account_url, credential=account_key)
    container_client = bsc.get_container_client(container)
    try:
        container_client.create_container()
    except Exception as e:
        # Already exists or cannot create; continue
        logger.debug(f"Container {container} already exists or cannot create: {e}")

    uploaded = 0
    for p in paths:
        p = Path(p)
        if not p.exists() or not p.is_file():
            continue
        blob_name = f"{full_prefix}/{p.name}" if full_prefix else p.name
        try:
            with p.open("rb") as f:
                container_client.upload_blob(name=blob_name, data=f, overwrite=True)
            uploaded += 1
            logger.info(f"Uploaded to blob '{container}/{blob_name}'")
        except Exception as up_err:
            logger.error(f"Failed to upload {p} -> {container}/{blob_name}: {up_err}")
            raise

    return {"uploaded": uploaded, "container": container, "prefix": full_prefix}
