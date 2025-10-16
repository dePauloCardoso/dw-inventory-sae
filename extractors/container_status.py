from __future__ import annotations

from typing import Any, Dict, List

from wms_client import WMSClient
from db import upsert_container_status


def extract_and_upsert_container_status(client: WMSClient, conn) -> int:
    """Extract all container status data and upsert to database (lookup table - no date filter needed)"""
    items: List[Dict[str, Any]] = client.fetch_all("container_status")
    return upsert_container_status(conn, items)


