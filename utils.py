# utils.py
from typing import Any, Dict, Iterable, List, Generator
import itertools


def flatten_one_level(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collapse immediate nested dicts into fields with suffixes:
    { 'facility_id': {'id': 1, 'url': '...'}, 'x': 1 } ->
    { 'facility_id_id': 1, 'facility_id_url': '...', 'x': 1 }
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            # keep nested keys with predictable suffixes
            if "id" in v:
                out[f"{k}_id"] = v.get("id")
            if "key" in v:
                out[f"{k}_key"] = v.get("key")
            if "url" in v:
                out[f"{k}_url"] = v.get("url")
            # keep original flattened content (others)
            for subk, subv in v.items():
                if subk not in {"id", "key", "url"}:
                    out[f"{k}_{subk}"] = subv
        else:
            out[k] = v
    return out


def batched(iterable: Iterable, n: int) -> Generator[List, None, None]:
    """
    Yield lists of length up to n.
    """
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if not chunk:
            break
        yield chunk
