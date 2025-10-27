# inventory.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import asyncio
import logging
from datetime import datetime

import httpx

from config import get_today_range
from utils import flatten_one_level, batched
from wms_client import WMSClient
from db import upsert_inventory

logger = logging.getLogger(__name__)


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def _parse_iso_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Accept ISO-like strings
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            # fallback: try common format
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def _flatten_inventory_record(inv: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(inv)

    # numeric
    for q in ["curr_qty", "orig_qty", "pack_qty", "case_qty"]:
        if q in flat:
            flat[q] = _safe_float(flat.get(q))

    # ints
    for i in [
        "id",
        "facility_id_id",
        "item_id_id",
        "location_id_id",
        "container_id_id",
        "status_id",
        "batch_number_id",
        "invn_attr_id_id",
        "uom_id_id",
    ]:
        if i in flat:
            flat[i] = _safe_int(flat.get(i))

    # dates / timestamps
    for ds in ["priority_date", "manufacture_date", "expiry_date"]:
        if ds in flat:
            d = flat.get(ds)
            if isinstance(d, str):
                parsed = _parse_iso_date(d)
                flat[ds] = parsed.date() if parsed else None

    for ts in ["create_ts", "mod_ts"]:
        if ts in flat:
            flat[ts] = _parse_iso_date(flat.get(ts))

    # normalize nested fields for backward compatibility
    nested = [
        "facility_id",
        "item_id",
        "location_id",
        "container_id",
        "invn_attr_id",
        "uom_id",
    ]
    for n in nested:
        if n in inv and isinstance(inv[n], dict):
            if "id" in inv[n]:
                flat[f"{n}_id"] = inv[n].get("id")
            if "key" in inv[n]:
                flat[f"{n}_key"] = inv[n].get("key")
            if "url" in inv[n]:
                flat[f"{n}_url"] = inv[n].get("url")

    return flat


async def _fetch_details_for_batch(
    client: WMSClient, ids: List[Any], concurrency: int = 10
) -> List[Optional[Dict[str, Any]]]:
    sem = asyncio.Semaphore(concurrency)
    out: List[Optional[Dict[str, Any]]] = []

    async_client = None  # noqa: F841
    try:
        # ensure async client is ready
        # WMSClient provides fetch_one_detail that will create its own async client.
        async def _fetch_one(eid):
            await sem.acquire()
            try:
                return await client.fetch_one_detail("inventory", eid)
            except httpx.HTTPStatusError as e:
                logger.warning("fetch detail status error for %s: %s", eid, e)
                return None
            except Exception:
                logger.exception("fetch detail failed for %s", eid)
                return None
            finally:
                sem.release()

        tasks = [asyncio.create_task(_fetch_one(i)) for i in ids]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        out.extend(results)
    finally:
        # leave client open (aclosing done by caller)
        pass

    return out


def extract_and_upsert_inventory(client: WMSClient, conn) -> int:
    """
    Sync wrapper: fetch paginated inventory list synchronously, then fetch details async in batches,
    flatten and upsert using provided conn.
    """
    dr = get_today_range()
    params = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    logger.info("Fetching inventory summary pages (sync)...")
    items = client.fetch_all_sync("inventory", params=params)
    logger.info("Found %d inventory summary records", len(items))

    # If no items, nothing to do
    if not items:
        return 0

    # prepare ids in original order
    ids = [it.get("id") for it in items if "id" in it]

    # fetch details in batches using asyncio
    all_details: List[Optional[Dict[str, Any]]] = []
    batch_size = 50  # number of concurrent detail fetches per batch

    async def _gather_all():
        tasks = []
        for i in range(0, len(ids), batch_size):
            chunk = ids[i : i + batch_size]
            tasks.append(
                asyncio.create_task(
                    _fetch_details_for_batch(client, chunk, concurrency=10)
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=False)
        for r in results:
            all_details.extend(r)

    try:
        asyncio.run(_gather_all())
    except RuntimeError as e:
        # In rare contexts where an event loop runs (e.g. other frameworks), fallback to sequential detail fetch
        logger.warning(
            "Async loop unavailable (%s). Falling back to synchronous detail fetch.", e
        )
        for eid in ids:
            try:
                d = client._client.get(
                    f"{client.base_url}/entity/inventory/{eid}",
                    headers=client._headers(),
                )
                d.raise_for_status()
                jd = d.json()
                all_details.append(jd.get("result", jd))
            except Exception:
                logger.exception("Failed sync detail for %s", eid)
                all_details.append(None)

    # Merge summaries with details: if detail exists use it else keep summary
    merged = []
    for summary, detail in zip(items, all_details):
        merged.append(detail if isinstance(detail, dict) and detail else summary)

    # Flatten and transform
    flattened = [_flatten_inventory_record(m) for m in merged]

    # Upsert in chunks
    total = 0
    for chunk in batched(flattened, 500):
        total += upsert_inventory(conn, chunk)

    logger.info("Finished inventory upsert, total rows: %d", total)
    return total
