# order_hdr.py
from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime, timedelta
import logging

from wms_client import WMSClient
from utils import flatten_one_level, batched
from db import upsert_order_hdr
from config import get_today_range

logger = logging.getLogger(__name__)


def _safe_float(v: Any) -> float | None:
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> int | None:
    if v in (None, "", "null"):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "t", "1", "yes", "y")
    return bool(v)


def _parse_date(s: Any) -> datetime.date | None:
    if not s:
        return None
    try:
        if isinstance(s, str):
            if "T" in s:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
            else:
                return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None
    return None


def _parse_datetime(s: Any) -> datetime | None:
    if not s:
        return None
    try:
        if isinstance(s, str):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
    return None


def _flatten_order_hdr_record(order_hdr: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(order_hdr)

    # Numeric conversions
    numeric_fields = [
        "total_orig_ord_qty",
        "orig_sale_price",
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
    ]
    for f in numeric_fields:
        if f in flat:
            flat[f] = _safe_float(flat.get(f))

    # Integer conversions
    integer_fields = [
        "id",
        "facility_id_id",
        "company_id_id",
        "status_id",
        "dest_facility_id",
        "shipto_facility_id",
        "stage_location_id",
        "ship_via_id",
        "priority",
        "payment_method_id",
        "ob_lpn_type_id",
        "orig_sku_count",
        "work_order_kit_id",
        "duties_payment_method_id",
        "customs_broker_contact_id",
        "order_type_id_id",
        "destination_company_id_id",
    ]
    for f in integer_fields:
        if f in flat:
            flat[f] = _safe_int(flat.get(f))

    # Boolean conversions
    for f in ["externally_planned_load_flg", "stop_ship_flg"]:
        if f in flat:
            flat[f] = _safe_bool(flat.get(f))

    # Timestamp conversions
    for f in ["create_ts", "mod_ts", "order_shipped_ts"]:
        if f in flat:
            flat[f] = _parse_datetime(flat.get(f))

    # Date conversions
    date_fields = [
        "ord_date",
        "exp_date",
        "req_ship_date",
        "start_ship_date",
        "stop_ship_date",
        "sched_ship_date",
        "cust_date_1",
        "cust_date_2",
        "cust_date_3",
        "cust_date_4",
        "cust_date_5",
    ]
    for f in date_fields:
        if f in flat:
            flat[f] = _parse_date(flat.get(f))

    # Nested references
    nested_fields = [
        "facility_id",
        "company_id",
        "order_type_id",
        "destination_company_id",
    ]
    for nf in nested_fields:
        if nf in flat and isinstance(flat[nf], dict):
            flat[f"{nf}_key"] = flat[nf].get("key")
            flat[f"{nf}_url"] = flat[nf].get("url")
            flat.pop(nf, None)

    # Handle order_dtl_set
    if flat.get("order_dtl_set"):
        ods = flat.pop("order_dtl_set")
        if isinstance(ods, dict):
            flat["order_dtl_set_result_count"] = ods.get("result_count")
            flat["order_dtl_set_url"] = ods.get("url")

    # Convert array fields to string for storage
    for array_field in ["order_instructions_set", "order_lock_set"]:
        if array_field in flat:
            v = flat[array_field]
            if isinstance(v, list):
                flat[array_field] = str(v)
            elif v is None:
                flat[array_field] = None

    return flat


def extract_and_upsert_order_hdr(client: WMSClient, conn) -> int:
    """
    Extract order header records for today's date (or last 3 days if empty),
    flatten them and upsert into the database.
    """
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    try:
        logger.info("Fetching order_hdr records (today)...")
        items: List[Dict[str, Any]] = client.fetch_all_sync("order_hdr", params=params)
        if not items:
            raise ValueError("No records found for today")
    except Exception as e:
        logger.warning(
            "Failed to fetch today's order_hdr (%s). Trying last 3 days...", e
        )
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        params = {
            "create_ts__gte": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "create_ts__lt": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        items: List[Dict[str, Any]] = client.fetch_all_sync("order_hdr", params=params)

    logger.info("Fetched %d order_hdr records", len(items))
    if not items:
        logger.info("No order_hdr data found to upsert")
        return 0

    flattened = [_flatten_order_hdr_record(rec) for rec in items]

    total = 0
    for chunk in batched(flattened, 500):
        total += upsert_order_hdr(conn, chunk)

    logger.info("Upserted %d order_hdr rows", total)
    return total
