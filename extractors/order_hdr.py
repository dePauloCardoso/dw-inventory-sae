from typing import Any, Dict, List

from wms_client import WMSClient
from utils import flatten_one_level
from db import upsert_order_hdr
from config import get_today_range


def _flatten_order_hdr_record(order_hdr: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(order_hdr)
    
    # Convert numeric fields
    for num_field in ["total_orig_ord_qty", "orig_sale_price", "cust_number_1", "cust_number_2", 
                      "cust_number_3", "cust_number_4", "cust_number_5", "cust_decimal_1", 
                      "cust_decimal_2", "cust_decimal_3", "cust_decimal_4", "cust_decimal_5"]:
        if flat.get(num_field):
            try:
                flat[num_field] = float(flat[num_field])
            except (TypeError, ValueError):
                flat[num_field] = None
    
    # Convert integer fields
    for int_field in ["status_id", "dest_facility_id", "shipto_facility_id", "stage_location_id", 
                      "ship_via_id", "priority", "payment_method_id", "ob_lpn_type_id", 
                      "orig_sku_count", "work_order_kit_id", "duties_payment_method_id", 
                      "customs_broker_contact_id"]:
        if flat.get(int_field):
            try:
                flat[int_field] = int(flat[int_field])
            except (TypeError, ValueError):
                flat[int_field] = None
    
    # Convert boolean fields
    for bool_field in ["externally_planned_load_flg", "stop_ship_flg"]:
        if flat.get(bool_field) is not None:
            flat[bool_field] = bool(flat[bool_field])
    
    # Convert timestamp fields
    for ts_field in ["create_ts", "mod_ts", "order_shipped_ts"]:
        if flat.get(ts_field):
            try:
                from datetime import datetime
                if isinstance(flat[ts_field], str):
                    flat[ts_field] = datetime.fromisoformat(flat[ts_field].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                flat[ts_field] = None
    
    # Convert date fields
    for date_field in ["ord_date", "exp_date", "req_ship_date", "start_ship_date", 
                       "stop_ship_date", "sched_ship_date", "cust_date_1", "cust_date_2", 
                       "cust_date_3", "cust_date_4", "cust_date_5"]:
        if flat.get(date_field):
            try:
                from datetime import datetime
                if isinstance(flat[date_field], str):
                    if 'T' in flat[date_field]:
                        flat[date_field] = datetime.fromisoformat(flat[date_field].replace('Z', '+00:00')).date()
                    else:
                        flat[date_field] = datetime.strptime(flat[date_field], '%Y-%m-%d').date()
            except (ValueError, TypeError):
                flat[date_field] = None
    
    # Handle order_dtl_set object
    if flat.get("order_dtl_set"):
        order_dtl_set = flat["order_dtl_set"]
        if isinstance(order_dtl_set, dict):
            flat["order_dtl_set_result_count"] = order_dtl_set.get("result_count")
            flat["order_dtl_set_url"] = order_dtl_set.get("url")
        flat.pop("order_dtl_set", None)
    
    # Convert arrays to text
    for array_field in ["order_instructions_set", "order_lock_set"]:
        if flat.get(array_field):
            if isinstance(flat[array_field], list):
                flat[array_field] = str(flat[array_field])
            elif flat[array_field] is None:
                flat[array_field] = None
    
    return flat


def extract_and_upsert_order_hdr(client: WMSClient, conn) -> int:
    """Extract today's order header data and upsert to database"""
    # Get today's date range
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    items: List[Dict[str, Any]] = client.fetch_all("order_hdr", params=params)
    flattened = [_flatten_order_hdr_record(x) for x in items]
    return upsert_order_hdr(conn, flattened)