# order_dtl.py
from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime, timedelta
import logging

from wms_client import WMSClient
from utils import flatten_one_level, batched
from db import upsert_order_dtl
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


def _parse_datetime(s: Any) -> datetime | None:
    if not s:
        return None
    try:
        if isinstance(s, str):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
    return None


def _flatten_order_dtl_record(order_dtl: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(order_dtl)

    # Numeric conversions
    numeric_fields = [
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
    ]
    for f in numeric_fields:
        if f in flat:
            flat[f] = _safe_float(flat.get(f))

    # Integer conversions
    integer_fields = [
        "id",
        "order_id_id",
        "seq_nbr",
        "item_id_id",
        "batch_number_id",
        "voucher_print_count",
        "status_id",
        "invn_attr_id_id",
        "uom_id_id",
        "ordered_uom_id_id",
        "ob_lpn_type_id",
        "orig_order_ref_id",
    ]
    for f in integer_fields:
        if f in flat:
            flat[f] = _safe_int(flat.get(f))

    # Timestamps
    ts_fields = [
        "create_ts",
        "mod_ts",
        "voucher_exp_date",
        "cust_date_1",
        "cust_date_2",
        "cust_date_3",
        "cust_date_4",
        "cust_date_5",
    ]
    for f in ts_fields:
        if f in flat:
            flat[f] = _parse_datetime(flat.get(f))

    # Nested fields (references)
    nested_fields = [
        "order_id",
        "item_id",
        "invn_attr_id",
        "uom_id",
        "ordered_uom_id",
    ]
    for nf in nested_fields:
        if nf in flat and isinstance(flat[nf], dict):
            flat[f"{nf}_key"] = flat[nf].get("key")
            flat[f"{nf}_url"] = flat[nf].get("url")
            flat.pop(nf, None)

    # Convert array fields to string
    for array_field in ["order_instructions_set", "required_serial_nbr_set"]:
        if array_field in flat:
            v = flat[array_field]
            if isinstance(v, list):
                flat[array_field] = str(v)
            elif v is None:
                flat[array_field] = None

    return flat


def extract_and_upsert_order_dtl(client: WMSClient, conn) -> int:
    """
    Extract order detail records for today's date (or last 3 days if empty),
    flatten them and upsert into the database.
    """
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    try:
        logger.info("Fetching order_dtl records (today)...")
        items: List[Dict[str, Any]] = client.fetch_all_sync("order_dtl", params=params)
        if not items:
            raise ValueError("No records found for today")
    except Exception as e:
        logger.warning(
            "Failed to fetch today's order_dtl (%s). Trying last 3 days...", e
        )
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        params = {
            "create_ts__gte": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "create_ts__lt": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        items: List[Dict[str, Any]] = client.fetch_all_sync("order_dtl", params=params)

    logger.info("Fetched %d order_dtl records", len(items))
    if not items:
        logger.info("No order_dtl data found to upsert")
        return 0

    flattened = [_flatten_order_dtl_record(rec) for rec in items]

    total = 0
    for chunk in batched(flattened, 500):
        total += upsert_order_dtl(conn, chunk)

    logger.info("Upserted %d order_dtl rows", total)
    return total
