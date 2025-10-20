from typing import Any, Dict, List

from wms_client import WMSClient
from utils import flatten_one_level
from db import upsert_container
from config import get_today_range


def _flatten_container_record(container: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(container)

    # Convert numeric fields
    for num_field in ["weight", "volume", "length", "width", "height"]:
        if flat.get(num_field):
            try:
                flat[num_field] = float(flat[num_field])
            except (TypeError, ValueError):
                flat[num_field] = None

    # Convert integer fields
    for int_field in [
        "status_id",
        "vas_status_id",
        "curr_location_id",
        "prev_location_id",
        "pallet_id",
        "lpn_type_id",
        "cart_posn_nbr",
        "audit_status_id",
        "qc_status_id",
        "asset_id",
        "nbr_files",
    ]:
        if flat.get(int_field):
            try:
                flat[int_field] = int(flat[int_field])
            except (TypeError, ValueError):
                flat[int_field] = None

    # Convert boolean fields
    for bool_field in ["parcel_batch_flg", "price_labels_printed", "actual_weight_flg"]:
        if flat.get(bool_field) is not None:
            flat[bool_field] = bool(flat[bool_field])

    # Convert timestamp fields
    for ts_field in ["create_ts", "mod_ts", "rcvd_ts", "first_putaway_ts"]:
        if flat.get(ts_field):
            try:
                from datetime import datetime

                if isinstance(flat[ts_field], str):
                    flat[ts_field] = datetime.fromisoformat(
                        flat[ts_field].replace("Z", "+00:00")
                    )
            except (ValueError, TypeError):
                flat[ts_field] = None

    # Convert date fields
    if flat.get("priority_date"):
        try:
            from datetime import datetime

            if isinstance(flat["priority_date"], str):
                if "T" in flat["priority_date"]:
                    flat["priority_date"] = datetime.fromisoformat(
                        flat["priority_date"].replace("Z", "+00:00")
                    ).date()
                else:
                    flat["priority_date"] = datetime.strptime(
                        flat["priority_date"], "%Y-%m-%d"
                    ).date()
        except (ValueError, TypeError):
            flat["priority_date"] = None

    return flat


def extract_and_upsert_container(client: WMSClient, conn) -> int:
    """Extract today's container data and upsert to database"""
    # Get today's date range
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    items: List[Dict[str, Any]] = client.fetch_all("container", params=params)
    flattened = [_flatten_container_record(x) for x in items]
    return upsert_container(conn, flattened)
