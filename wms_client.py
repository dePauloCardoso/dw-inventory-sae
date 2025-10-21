from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from config import get_wms_config


class WMSClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: float = 30.0,
        retries: int = 3,
    ) -> None:
        # Load from config.json if not provided
        wms_config = get_wms_config()
        self.base_url = base_url or wms_config.get("base_url")
        self.username = username or wms_config.get("username")
        self.password = password or wms_config.get("password")
        self.verify_ssl = (
            verify_ssl if verify_ssl is not None else wms_config.get("verify_ssl", True)
        )
        self.timeout = (
            timeout if timeout != 30.0 else wms_config.get("default_timeout", 30.0)
        )
        self.retries = retries if retries != 3 else wms_config.get("default_retries", 3)

        # Validate required fields
        if not all([self.base_url, self.username, self.password]):
            raise ValueError("base_url, username, and password are required")

        # Clean base_url
        self.base_url = self.base_url.rstrip("/")

        # Create HTTP client with basic auth
        self.client = httpx.Client(
            timeout=self.timeout,
            verify=self.verify_ssl,
            auth=(self.username, self.password),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
        }

    @retry(wait=wait_exponential_jitter(initial=1, max=10), stop=stop_after_attempt(5))
    def fetch_all(
        self, entity: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/entity/{entity}"
        items: List[Dict[str, Any]] = []
        page = 1
        while True:
            query = dict(params or {})
            query["page"] = page
            resp = self.client.get(url, headers=self._headers(), params=query)
            resp.raise_for_status()
            data = resp.json()
            results = data if isinstance(data, list) else data.get("results", [])
            if not results:
                break
            items.extend(results)
            # Break if no pagination indicators
            if isinstance(data, dict) and not data.get("next"):
                break
            page += 1
        return items
