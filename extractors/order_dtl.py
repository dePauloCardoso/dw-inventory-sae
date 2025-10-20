from typing import Any, Dict, List

from wms_client import WMSClient
from utils import flatten_one_level
from db import upsert_order_dtl
from config import get_today_range


def _flatten_order_dtl_record(order_dtl: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(order_dtl)

    # Convert numeric fields
    for num_field in [
        "ord_qty",
        "orig_ord_qty",
        "alloc_qty",
        "cost",
        "sale_price",
        "voucher_amount",
        "unit_declared_value",
        "cust_number_1",
        "cust_number_2",
        "cust_number_3",
        "cust_number_4",
        "cust_number_5",
        "cust_decimal_1",
        "cust_decimal_2",
        "cust_decimal_3",
        "cust_decimal_4",
        "cust_decimal_5",
        "min_shipping_tolerance_percentage",
        "max_shipping_tolerance_percentage",
        "ordered_uom_qty",
    ]:
        if flat.get(num_field):
            try:
                flat[num_field] = float(flat[num_field])
            except (TypeError, ValueError):
                flat[num_field] = None

    # Convert integer fields
    for int_field in [
        "seq_nbr",
        "batch_number_id",
        "voucher_print_count",
        "status_id",
        "ob_lpn_type_id",
        "orig_order_ref_id",
    ]:
        if flat.get(int_field):
            try:
                flat[int_field] = int(flat[int_field])
            except (TypeError, ValueError):
                flat[int_field] = None

    # Convert timestamp fields
    for ts_field in [
        "create_ts",
        "mod_ts",
        "voucher_exp_date",
        "cust_date_1",
        "cust_date_2",
        "cust_date_3",
        "cust_date_4",
        "cust_date_5",
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

    # Convert arrays to text
    for array_field in ["order_instructions_set", "required_serial_nbr_set"]:
        if flat.get(array_field):
            if isinstance(flat[array_field], list):
                flat[array_field] = str(flat[array_field])
            elif flat[array_field] is None:
                flat[array_field] = None

    return flat


def extract_and_upsert_order_dtl(client: WMSClient, conn) -> int:
    """Extract today's order detail data and upsert to database"""
    # Get today's date range
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    items: List[Dict[str, Any]] = client.fetch_all("order_dtl", params=params)
    flattened = [_flatten_order_dtl_record(x) for x in items]
    return upsert_order_dtl(conn, flattened)
