# wms_client.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, AsyncIterator
import logging

import httpx

from config import get_wms_config

logger = logging.getLogger(__name__)


class WMSClient:
    """
    Client with both sync and async helpers.
    Use as:
      with WMSClient() as c:
          c.fetch_all_sync(...)
      or
      async with WMSClient(async_mode=True) as ac:
          await ac.fetch_all_async(...)
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        timeout: Optional[float] = None,
        retries: Optional[int] = None,
    ):
        cfg = get_wms_config()
        self.base_url = (base_url or cfg.get("base_url") or "").rstrip("/")
        self.username = username or cfg.get("username")
        self.password = password or cfg.get("password")
        self.verify_ssl = (
            verify_ssl if verify_ssl is not None else cfg.get("verify_ssl", True)
        )
        self.timeout = (
            timeout if timeout is not None else cfg.get("default_timeout", 30.0)
        )
        self.retries = retries if retries is not None else cfg.get("default_retries", 3)

        if not all([self.base_url, self.username, self.password]):
            raise ValueError("WMSClient requires base_url, username and password")

        # sync client
        self._client = httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            auth=(self.username, self.password),
        )
        # async client (created lazily)
        self._async_client: Optional[httpx.AsyncClient] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self._client.close()
        except Exception:
            logger.exception("Error closing sync client")
        if self._async_client is not None:
            # can't await here; user should use async context for async client
            pass

    async def aclose(self):
        if self._async_client is not None:
            await self._async_client.aclose()

    def _headers(self) -> Dict[str, str]:
        return {"Accept": "application/json"}

    # ---------------------- SYNC helpers ----------------------
    def fetch_all_sync(
        self, entity: str, params: Optional[Dict[str, Any]] = None, page_size: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages synchronously and return a flat list of items.
        """
        items: List[Dict[str, Any]] = []
        url = f"{self.base_url}/entity/{entity}"
        page = 1
        while True:
            q = dict(params or {})
            q["page"] = page
            q["page_size"] = page_size
            resp = self._client.get(url, headers=self._headers(), params=q)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", []) or data.get("result", []) or []
            if not results:
                break
            items.extend(results)
            if len(results) < page_size:
                break
            page += 1
        return items

    # ---------------------- ASYNC helpers ----------------------
    async def _ensure_async_client(self) -> httpx.AsyncClient:
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(
                timeout=self.timeout,
                verify=self.verify_ssl,
                auth=(self.username, self.password),
            )
        return self._async_client

    async def fetch_all_async(
        self, entity: str, params: Optional[Dict[str, Any]] = None, page_size: int = 200
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Async generator that yields one page (list) at a time.
        Example:
            async for page in client.fetch_all_async("inventory"):
                process(page)
        """
        client = await self._ensure_async_client()
        url = f"{self.base_url}/entity/{entity}"
        page = 1
        while True:
            q = dict(params or {})
            q["page"] = page
            q["page_size"] = page_size
            resp = await client.get(url, headers=self._headers(), params=q)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", []) or data.get("result", []) or []
            if not results:
                break
            yield results
            if len(results) < page_size:
                break
            page += 1

    async def fetch_one_detail(
        self, entity: str, entity_id: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single entity detail endpoint: /entity/{entity}/{id}
        """
        client = await self._ensure_async_client()
        url = f"{self.base_url}/entity/{entity}/{entity_id}"
        resp = await client.get(url, headers=self._headers())
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", data)
