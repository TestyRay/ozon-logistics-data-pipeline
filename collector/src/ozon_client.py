import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = structlog.get_logger()


class OzonPermissionError(Exception):
    """Raised when API key has no access to requested method."""


class OzonClient:
    def __init__(self, base_url: str, client_id: str, api_key: str, timeout: float = 30.0):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={
                "Client-Id": client_id,
                "Api-Key": api_key,
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def post(self, path: str, body: dict) -> dict:
        resp = await self._client.post(path, json=body)
        if resp.status_code in (401, 403):
            raise OzonPermissionError(f"{path}: {resp.status_code} {resp.text[:200]}")
        if resp.status_code == 429:
            log.warning("ozon.rate_limited", path=path)
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()