# engine/risk_client.py
import asyncio
import json
import logging
from typing import Dict, Any, Optional

import httpx

LOG = logging.getLogger("risk_client")
RISK_URL = "http://risk:8003/assess"  # service name in docker-compose
DEFAULT_TIMEOUT = 5.0


class RiskClient:
    def __init__(self, url: str = RISK_URL, max_retries: int = 3, base_backoff: float = 0.5):
        self.url = url
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self._client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)

    async def assess(self, account_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send account payload to risk service and return the parsed JSON dict.
        Retries on network errors and server 5xx. Raises on unrecoverable errors.
        """
        body = {"account": account_payload}
        attempt = 0
        while True:
            attempt += 1
            try:
                LOG.debug("RiskClient: POST %s attempt=%d body=%s", self.url, attempt, body)
                resp = await self._client.post(self.url, json=body)
                if resp.status_code >= 500:
                    # server error -> retry
                    raise httpx.HTTPStatusError("Server error", request=resp.request, response=resp)
                resp.raise_for_status()
                data = resp.json()
                LOG.debug("RiskClient: response=%s", data)
                return data
            except (httpx.TransportError, httpx.HTTPStatusError, httpx.ReadTimeout) as e:
                LOG.warning("RiskClient: attempt %d failed: %s", attempt, str(e))
                if attempt >= self.max_retries:
                    LOG.error("RiskClient: max retries reached, raising")
                    raise
                backoff = self.base_backoff * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
            except Exception:
                LOG.exception("RiskClient: unexpected error")
                raise

    async def close(self):
        await self._client.aclose()
