# location.py
from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime
import logging

from wms_client import WMSClient
from utils import flatten_one_level, batched
from db import upsert_location

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


def _flatten_location_record(location: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(location)

    # Float fields
    for f in [
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
        if f in flat:
            flat[f] = _safe_float(flat.get(f))

    # Integer fields
    for f in [
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
        if f in flat:
            flat[f] = _safe_int(flat.get(f))

    # Boolean fields
    for f in [
        "allow_multi_sku",
        "to_be_counted_flg",
        "lock_for_putaway_flg",
        "allow_reserve_partial_pick_flg",
        "restrict_batch_nbr_flg",
        "restrict_invn_attr_flg",
        "assembly_flg",
    ]:
        if f in flat:
            flat[f] = _safe_bool(flat.get(f))

    # Timestamp fields
    for f in [
        "create_ts",
        "mod_ts",
        "to_be_counted_ts",
        "last_count_ts",
        "lock_applied_ts",
    ]:
        if f in flat:
            flat[f] = _parse_datetime(flat.get(f))

    # Nested dicts (id/key/url)
    nested_fields = [
        "facility_id",
        "dedicated_company_id",
        "type_id",
        "destination_company_id",
        "replenishment_zone_id",
        "item_assignment_type_id",
        "item_id",
        "cc_threshold_uom_id",
    ]
    for nf in nested_fields:
        if nf in flat and isinstance(flat[nf], dict):
            flat[f"{nf}_key"] = flat[nf].get("key")
            flat[f"{nf}_url"] = flat[nf].get("url")
            flat.pop(nf, None)

    return flat


def extract_and_upsert_location(client: WMSClient, conn) -> int:
    """
    Extract location data for the current month, flatten, and upsert into DB.
    If no records are found, tries previous month.
    """
    current_month = datetime.now().month
    params = {"create_ts__month": current_month}

    try:
        logger.info("Fetching location data for current month (%d)...", current_month)
        items: List[Dict[str, Any]] = client.fetch_all_sync("location", params=params)
        if not items:
            raise ValueError("No data found for current month")
    except Exception as e:
        logger.warning("Failed to fetch current month: %s", e)
        prev_month = current_month - 1 or 12
        params = {"create_ts__month": prev_month}
        logger.info("Retrying with previous month (%d)...", prev_month)
        items: List[Dict[str, Any]] = client.fetch_all_sync("location", params=params)

    logger.info("Fetched %d location records", len(items))
    if not items:
        logger.info("No location data to upsert")
        return 0

    flattened = [_flatten_location_record(rec) for rec in items]
    total = 0
    for chunk in batched(flattened, 500):
        total += upsert_location(conn, chunk)

    logger.info("Upserted %d location rows", total)
    return total
