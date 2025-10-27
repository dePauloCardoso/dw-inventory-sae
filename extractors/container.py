# container.py
from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime, timedelta
import logging

from wms_client import WMSClient
from utils import flatten_one_level, batched
from db import upsert_container
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


def _parse_datetime(s: Any) -> datetime | None:
    if not s:
        return None
    try:
        if isinstance(s, str):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
    return None


def _flatten_container_record(container: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(container)

    # Numeric conversions
    for f in ["weight", "volume", "length", "width", "height"]:
        if f in flat:
            flat[f] = _safe_float(flat.get(f))

    # Integer conversions
    integer_fields = [
        "id",
        "facility_id_id",
        "company_id_id",
        "status_id",
        "vas_status_id",
        "curr_location_id_id",
        "prev_location_id_id",
        "pallet_id",
        "rcvd_shipment_id_id",
        "putawaytype_id_id",
        "lpn_type_id",
        "cart_posn_nbr",
        "audit_status_id",
        "qc_status_id",
        "asset_id",
        "nbr_files",
    ]
    for f in integer_fields:
        if f in flat:
            flat[f] = _safe_int(flat.get(f))

    # Boolean conversions
    for f in ["parcel_batch_flg", "price_labels_printed", "actual_weight_flg"]:
        if f in flat:
            flat[f] = _safe_bool(flat.get(f))

    # Timestamp conversions
    ts_fields = ["create_ts", "mod_ts", "rcvd_ts", "first_putaway_ts", "priority_date"]
    for f in ts_fields:
        if f in flat:
            flat[f] = _parse_datetime(flat.get(f))

    # Nested fields (reference IDs, keys, urls)
    nested_fields = [
        "facility_id",
        "company_id",
        "curr_location_id",
        "prev_location_id",
        "rcvd_shipment_id",
        "putawaytype_id",
    ]
    for nf in nested_fields:
        if nf in flat and isinstance(flat[nf], dict):
            flat[f"{nf}_key"] = flat[nf].get("key")
            flat[f"{nf}_url"] = flat[nf].get("url")
            flat.pop(nf, None)

    return flat


def extract_and_upsert_container(client: WMSClient, conn) -> int:
    """
    Extract container records for today's date (or last 3 days if empty),
    flatten them and upsert into the database.
    """
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    try:
        logger.info("Fetching container records (today)...")
        items: List[Dict[str, Any]] = client.fetch_all_sync("container", params=params)
        if not items:
            raise ValueError("No records found for today")
    except Exception as e:
        logger.warning(
            "Failed to fetch today's container (%s). Trying last 3 days...", e
        )
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        params = {
            "create_ts__gte": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "create_ts__lt": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        items: List[Dict[str, Any]] = client.fetch_all_sync("container", params=params)

    logger.info("Fetched %d container records", len(items))
    if not items:
        logger.info("No container data found to upsert")
        return 0

    flattened = [_flatten_container_record(rec) for rec in items]

    total = 0
    for chunk in batched(flattened, 500):
        total += upsert_container(conn, chunk)

    logger.info("Upserted %d container rows", total)
    return total
