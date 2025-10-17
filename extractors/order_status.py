from __future__ import annotations

from typing import Any, Dict, List

from wms_client import WMSClient
from db import upsert_order_status


def extract_and_upsert_order_status(client: WMSClient, conn) -> int:
    """Extract all order status data and upsert to database (lookup table - no date filter needed)"""
    items: List[Dict[str, Any]] = client.fetch_all("order_status")
    return upsert_order_status(conn, items)