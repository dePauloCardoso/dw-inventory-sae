from typing import Any, Dict, List

from wms_client import WMSClient
from utils import flatten_one_level
from db import upsert_location


def _flatten_location_record(location: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(location)

    # Convert numeric fields
    for num_field in [
        "length",
        "width",
        "height",
        "max_units",
        "min_units",
        "max_volume",
        "min_volume",
        "min_weight",
        "max_weight",
        "cc_threshold_value",
        "x_coordinate",
        "y_coordinate",
        "z_coordinate",
        "in_transit_units",
    ]:
        if flat.get(num_field) is not None and flat.get(num_field) != "":
            try:
                flat[num_field] = float(flat[num_field])
            except (TypeError, ValueError):
                flat[num_field] = None

    # Convert integer fields
    for int_field in [
        "id",
        "facility_id_id",
        "dedicated_company_id_id",
        "type_id_id",
        "max_lpns",
        "lock_code_id",
        "replenishment_zone_id_id",
        "item_assignment_type_id_id",
        "item_id_id",
        "mhe_system_id",
        "task_zone_id",
        "cc_threshold_uom_id_id",
    ]:
        if flat.get(int_field):
            try:
                flat[int_field] = int(flat[int_field])
            except (TypeError, ValueError):
                flat[int_field] = None

    # Convert boolean fields
    for bool_field in [
        "allow_multi_sku",
        "to_be_counted_flg",
        "lock_for_putaway_flg",
        "allow_reserve_partial_pick_flg",
        "restrict_batch_nbr_flg",
        "restrict_invn_attr_flg",
        "assembly_flg",
    ]:
        if flat.get(bool_field) is not None:
            flat[bool_field] = bool(flat[bool_field])

    # Convert timestamp fields
    for ts_field in [
        "create_ts",
        "mod_ts",
        "to_be_counted_ts",
        "last_count_ts",
        "lock_applied_ts",
    ]:
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


def extract_and_upsert_location(client: WMSClient, conn) -> int:
    """Extract current month's location data and upsert to database"""
    # Get current month
    from datetime import datetime

    current_month = datetime.now().month

    params = {
        "create_ts__month": current_month,
    }

    try:
        items: List[Dict[str, Any]] = client.fetch_all("location", params=params)
        flattened = [_flatten_location_record(x) for x in items]
        return upsert_location(conn, flattened)
    except Exception as e:
        print(f"Error fetching location data: {e}")
        # Try with previous month if current month's data is not available
        from datetime import datetime

        print("Trying with previous month...")
        previous_month = datetime.now().month - 1
        if previous_month == 0:
            previous_month = 12

        params = {
            "create_ts__month": previous_month,
        }

        items: List[Dict[str, Any]] = client.fetch_all("location", params=params)
        flattened = [_flatten_location_record(x) for x in items]
        return upsert_location(conn, flattened)
