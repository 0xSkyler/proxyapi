"""Probe targets and origin (own public) IP discovery.

The recommended setup points ``PROBE_HTTP_URL`` at the ``/probe`` location
served directly by this project's nginx (see ``nginx/proxy.conf``): nginx
answers with a few bytes (seen IP, echoed nonce, proxy headers) without
touching Python, and no third-party website receives validation traffic.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import secrets
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

from proxy_quality.domain import ip_is_public

log = logging.getLogger(__name__)


@dataclass(slots=True)
class ProbeTarget:
    url: str
    scheme: str
    host: str
    port: int
    path_query: str
    resolved_ipv4: str | None = None

    @classmethod
    def parse(cls, url: str) -> ProbeTarget:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError(f"invalid probe URL: {url!r}")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        path = parts.path or "/"
        if parts.query:
            path += "?" + parts.query
        return cls(url=url, scheme=parts.scheme, host=parts.hostname, port=port, path_query=path)

    @property
    def host_header(self) -> str:
        default = 443 if self.scheme == "https" else 80
        host = f"[{self.host}]" if ":" in self.host else self.host
        return host if self.port == default else f"{host}:{self.port}"

    def path_with_nonce(self, nonce: str | None) -> str:
        if nonce is None:
            return self.path_query
        sep = "&" if "?" in self.path_query else "?"
        return f"{self.path_query}{sep}n={nonce}"

    def absolute_url(self, nonce: str | None) -> str:
        return f"{self.scheme}://{self.host_header}{self.path_with_nonce(nonce)}"

    async def resolve(self) -> None:
        """Resolve an IPv4 address once (needed for plain SOCKS4, which cannot carry hostnames)."""
        try:
            ip = ipaddress.ip_address(self.host)
            self.resolved_ipv4 = str(ip) if ip.version == 4 else None
            return
        except ValueError:
            pass
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(
                self.host, self.port, family=socket.AF_INET, type=socket.SOCK_STREAM
            )
            if infos:
                self.resolved_ipv4 = infos[0][4][0]
        except OSError as exc:
            log.warning("probe host resolution failed", extra={"host": self.host, "error": str(exc)})


class ProbeSet:
    def __init__(self, http_url: str, https_url: str, owned: bool) -> None:
        self.http = ProbeTarget.parse(http_url)
        self.https = ProbeTarget.parse(https_url)
        if self.http.scheme != "http":
            raise ValueError("PROBE_HTTP_URL must be an http:// URL")
        if self.https.scheme != "https":
            raise ValueError("PROBE_HTTPS_URL must be an https:// URL")
        self.owned = owned

    async def resolve(self) -> None:
        await asyncio.gather(self.http.resolve(), self.https.resolve())

    def new_nonce(self) -> str | None:
        return secrets.token_hex(8) if self.owned else None


async def detect_origin_ip(services: list[str], timeout: float = 8.0) -> str | None:
    """Ask plain-text/JSON IP echo services for our public IP (direct, no proxy)."""
    async with httpx.AsyncClient(timeout=timeout, trust_env=False, follow_redirects=True) as client:
        for url in services:
            try:
                resp = await client.get(url, headers={"Accept": "text/plain"})
                text = resp.text.strip()
                if text.startswith("{"):
                    text = str(resp.json().get("ip", ""))
                candidate = text.splitlines()[0].strip() if text else ""
                if ip_is_public(candidate):
                    return str(ipaddress.ip_address(candidate))
            except (httpx.HTTPError, ValueError, IndexError) as exc:
                log.debug("origin ip service failed", extra={"url": url, "error": str(exc)})
    return None
