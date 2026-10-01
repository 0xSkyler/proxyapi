"""Exit-IP verification and ProxyScrape-style anonymity classification.

Supports the local owned probe, ProxyScrape judge responses, JSON IP echoes,
and plain IP responses. ProxyScrape judge responses use the same anonymity
ordering as ProxyScrape's open-source checker: transparent when the validator's
public IP is disclosed, anonymous when proxy-identifying headers are present,
otherwise elite.
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass

import orjson

from proxy_quality.validator.results import ErrorKind

_REMOTE_ADDR_RE = re.compile(r"(?im)^\s*REMOTE_ADDR\s*=\s*([^\s]+)")
_PROXY_ANON_HEADER_RE = re.compile(r"(?i)HTTP_VIA|PROXY_REMOTE_ADDR")


@dataclass(slots=True)
class ProbeEcho:
    ip: str | None
    nonce: str | None = None
    via: str | None = None
    xff: str | None = None
    fwd: str | None = None
    raw: str | None = None
    proxyscrape_judge: bool = False


@dataclass(slots=True)
class ExitVerdict:
    ok: bool
    error: ErrorKind | None
    exit_ip: str | None
    verified: bool
    anonymity: str | None
    detail: str | None = None


def _clean_ip(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return str(ipaddress.ip_address(value.strip().strip("[]")))
    except ValueError:
        return None


def parse_probe_body(body: bytes) -> ProbeEcho:
    text = body[:16384].decode("utf-8", errors="replace").strip()
    if not text:
        return ProbeEcho(ip=None)

    if match := _REMOTE_ADDR_RE.search(text):
        return ProbeEcho(
            ip=_clean_ip(match.group(1)),
            raw=text,
            proxyscrape_judge=True,
        )

    if text.startswith("{"):
        try:
            data = orjson.loads(text)
        except orjson.JSONDecodeError:
            return ProbeEcho(ip=None, raw=text)
        if isinstance(data, dict):
            ip = data.get("ip") or data.get("origin")
            return ProbeEcho(ip=_clean_ip(str(ip)) if ip else None, nonce=data.get("n"), raw=text)
        return ProbeEcho(ip=None, raw=text)

    if "=" in text:
        kv: dict[str, str] = {}
        for line in text.splitlines():
            k, sep, v = line.partition("=")
            if sep:
                kv[k.strip().lower()] = v.strip()
        return ProbeEcho(
            ip=_clean_ip(kv.get("ip")),
            nonce=kv.get("n") or None,
            via=kv.get("via") or None,
            xff=kv.get("xff") or None,
            fwd=kv.get("fwd") or None,
            raw=text,
        )

    return ProbeEcho(ip=_clean_ip(text.splitlines()[0]), raw=text)


def _mentions(header: str | None, ip: str) -> bool:
    return bool(header) and ip in header  # type: ignore[operator]


def _proxyscrape_anonymity(raw: str, origin_ip: str | None) -> str:
    if origin_ip and origin_ip in raw:
        return "transparent"
    if _PROXY_ANON_HEADER_RE.search(raw):
        return "anonymous"
    return "elite"


def evaluate_exit(
    echo: ProbeEcho,
    *,
    expected_nonce: str | None,
    origin_ip: str | None,
    owned_probe: bool,
    plain_http: bool,
) -> ExitVerdict:
    _ = plain_http

    if echo.ip is None:
        return ExitVerdict(
            False,
            ErrorKind.TAMPERED if owned_probe else ErrorKind.BAD_RESPONSE,
            None,
            False,
            None,
            detail="no_ip_in_probe_reply",
        )
    if owned_probe and expected_nonce is not None and echo.nonce != expected_nonce:
        return ExitVerdict(False, ErrorKind.TAMPERED, echo.ip, False, None, detail="nonce_mismatch")

    if echo.proxyscrape_judge:
        return ExitVerdict(
            True,
            None,
            echo.ip,
            origin_ip is not None,
            _proxyscrape_anonymity(echo.raw or "", origin_ip),
        )

    if origin_ip and echo.ip == origin_ip:
        return ExitVerdict(False, ErrorKind.BYPASS, echo.ip, False, None, detail="exit_ip_is_origin")

    if owned_probe:
        if origin_ip and any(_mentions(h, origin_ip) for h in (echo.via, echo.xff, echo.fwd)):
            anonymity = "transparent"
        elif echo.via or echo.xff or echo.fwd:
            anonymity = "anonymous"
        else:
            anonymity = "elite"
    else:
        anonymity = None

    return ExitVerdict(True, None, echo.ip, origin_ip is not None, anonymity)
