from __future__ import annotations
from typing import Any, Dict, List, Optional
import asyncio
import logging
from datetime import datetime, timedelta

import httpx

from config import get_today_range
from utils import flatten_one_level, batched
from wms_client import WMSClient
from db import upsert_oblpn

logger = logging.getLogger(__name__)


# === Funções auxiliares ===


def _safe_float(v: Any) -> float:
    if v in (None, "", "null"):
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def _safe_int(v: Any) -> Optional[int]:
    if v in (None, "", "null"):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _parse_iso_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


# === Normalização / Flatten ===


def _flatten_oblpn_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(rec)

    # Convert numeric-like strings
    for q in ["weight", "volume", "length", "width", "height"]:
        if q in flat:
            flat[q] = _safe_float(flat.get(q))

    # Integers
    for i in [
        "id",
        "facility_id_id",
        "company_id_id",
        "curr_location_id_id",
        "prev_location_id_id",
        "status_id",
        "vas_status_id",
        "audit_status_id",
        "qc_status_id",
        "lpn_type_id",
        "cart_posn_nbr",
        "nbr_files",
    ]:
        if i in flat:
            flat[i] = _safe_int(flat.get(i))

    # Dates / timestamps
    for ts in ["create_ts", "mod_ts", "rcvd_ts", "first_putaway_ts"]:
        if ts in flat:
            flat[ts] = _parse_iso_date(flat.get(ts))

    # Nested (relacionais)
    nested = [
        "facility_id",
        "company_id",
        "curr_location_id",
        "prev_location_id",
    ]
    for n in nested:
        if n in rec and isinstance(rec[n], dict):
            if "id" in rec[n]:
                flat[f"{n}_id"] = rec[n].get("id")
            if "key" in rec[n]:
                flat[f"{n}_key"] = rec[n].get("key")
            if "url" in rec[n]:
                flat[f"{n}_url"] = rec[n].get("url")

    return flat


# === Fetch assíncrono de detalhes ===


async def _fetch_details_for_batch(
    client: WMSClient, ids: List[Any], concurrency: int = 10
) -> List[Optional[Dict[str, Any]]]:
    sem = asyncio.Semaphore(concurrency)
    out: List[Optional[Dict[str, Any]]] = []

    async def _fetch_one(eid):
        await sem.acquire()
        try:
            return await client.fetch_one_detail("oblpn", eid)
        except httpx.HTTPStatusError as e:
            logger.warning("fetch detail status error for oblpn %s: %s", eid, e)
            return None
        except Exception:
            logger.exception("fetch detail failed for oblpn %s", eid)
            return None
        finally:
            sem.release()

    tasks = [asyncio.create_task(_fetch_one(i)) for i in ids]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    out.extend(results)
    return out


# === Função principal ===


def extract_and_upsert_oblpn(client: WMSClient, conn) -> int:
    """
    Extrai dados de OBLPN (Outbound LPN), coleta detalhes e faz upsert no banco.
    Se não houver dados para o dia atual, busca dos últimos 2 dias.
    """
    dr = get_today_range()
    params_today = {
        "create_ts__gte": dr.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "create_ts__lt": dr.end.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    logger.info("Fetching OBLPN summary pages (sync)...")
    items = []
    try:
        items = client.fetch_all_sync("oblpn", params=params_today)
        logger.info("Found %d oblpn summary records for today", len(items))
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.warning("No OBLPN found for today (404). Trying last 2 days...")
        else:
            raise

    # Retry with last 2 days if no data today
    if not items:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=2)
        params_retry = {
            "create_ts__gte": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "create_ts__lt": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        try:
            items = client.fetch_all_sync("oblpn", params=params_retry)
            logger.info("Found %d OBLPN records in last 2 days", len(items))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning("No OBLPN found in the last 2 days either.")
                return 0
            else:
                raise

    if not items:
        logger.warning("No OBLPN records found at all.")
        return 0

    ids = [it.get("id") for it in items if "id" in it]
    all_details: List[Optional[Dict[str, Any]]] = []
    batch_size = 50

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
        logger.warning(
            "Async loop unavailable (%s). Falling back to sync detail fetch for OBLPN.",
            e,
        )
        for eid in ids:
            try:
                d = client._client.get(
                    f"{client.base_url}/entity/oblpn/{eid}",
                    headers=client._headers(),
                )
                d.raise_for_status()
                jd = d.json()
                all_details.append(jd.get("result", jd))
            except Exception:
                logger.exception("Failed sync detail for OBLPN %s", eid)
                all_details.append(None)

    merged = []
    for summary, detail in zip(items, all_details):
        merged.append(detail if isinstance(detail, dict) and detail else summary)

    flattened = [_flatten_oblpn_record(m) for m in merged]

    total = 0
    for chunk in batched(flattened, 500):
        total += upsert_oblpn(conn, chunk)

    logger.info("Finished OBLPN upsert, total rows: %d", total)
    return total
