from typing import Any, Dict, List

from wms_client import WMSClient
from utils import flatten_one_level
from db import upsert_inventory
from config import get_today_range


def _flatten_inventory_record(inv: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(inv)
    # Convert numeric fields
    for qty_field in ["curr_qty", "orig_qty", "pack_qty", "case_qty"]:
        qty_val = flat.get(qty_field)
        try:
            flat[qty_field] = float(qty_val) if qty_val is not None else 0
        except (TypeError, ValueError):
            flat[qty_field] = 0

    # Convert date fields
    for date_field in ["priority_date", "manufacture_date", "expiry_date"]:
        if flat.get(date_field):
            try:
                from datetime import datetime

                if isinstance(flat[date_field], str):
                    # Handle ISO date format
                    if "T" in flat[date_field]:
                        flat[date_field] = datetime.fromisoformat(
                            flat[date_field].replace("Z", "+00:00")
                        ).date()
                    else:
                        flat[date_field] = datetime.strptime(
                            flat[date_field], "%Y-%m-%d"
                        ).date()
            except (ValueError, TypeError):
                flat[date_field] = None

    # Convert timestamp fields
    for ts_field in ["create_ts", "mod_ts"]:
        if flat.get(ts_field):
            try:
                from datetime import datetime

                if isinstance(flat[ts_field], str):
                    flat[ts_field] = datetime.fromisoformat(
                        flat[ts_field].replace("Z", "+00:00")
                    )
            except (ValueError, TypeError):
                flat[ts_field] = None

    return flat


def extract_and_upsert_inventory(client: WMSClient, conn) -> int:
    """Extract today's inventory data and upsert to database"""
    # Get today's date range
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    try:
        items: List[Dict[str, Any]] = client.fetch_all("inventory", params=params)
        flattened = [_flatten_inventory_record(x) for x in items]
        return upsert_inventory(conn, flattened)
    except Exception as e:
        print(f"Error fetching inventory data: {e}")
        # Try with a broader date range (last 3 days) if today's data is not available
        from datetime import datetime, timedelta

        print("Trying with last 3 days date range...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)

        params = {
            "create_ts__gte": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "create_ts__lt": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        items: List[Dict[str, Any]] = client.fetch_all("inventory", params=params)
        flattened = [_flatten_inventory_record(x) for x in items]
        return upsert_inventory(conn, flattened)
