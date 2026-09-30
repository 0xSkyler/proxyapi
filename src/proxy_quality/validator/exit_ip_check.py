"""Exit-IP verification and (for plain HTTP proxies) header-based anonymity.

Probe response formats understood:

* owned probe (``nginx/proxy.conf`` ``location = /probe``)::

      ip=<address the probe saw>
      n=<nonce we sent>
      via=<Via header>
      xff=<X-Forwarded-For header>
      fwd=<Forwarded header>

* JSON ``{"ip": "..."}`` (ipify style) or a bare IP (icanhazip style).

Verdicts:

* the address seen by the probe equals our own public (origin) IP
  -> ``bypass``: traffic did not leave through the proxy -> rejected
* owned probe and the nonce is missing / different
  -> ``tampered``: the proxy served content that is not our probe's reply
  (injected page, captive portal, stale cache)
* otherwise the exit IP is *verified*.

Anonymity (documented definition, plain-HTTP forwarding only, owned probe only):

* ``transparent``: our origin IP appears in Via / X-Forwarded-For / Forwarded
* ``anonymous``:   proxy headers present but our origin IP is not disclosed
* ``elite``:       none of those headers reached the probe and the origin IP is not disclosed

CONNECT and SOCKS tunnels cannot rewrite end-to-end content, so they are
reported as ``tunnel`` instead of claiming an anonymity level.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass

import orjson

from proxy_quality.validator.results import ErrorKind


@dataclass(slots=True)
class ProbeEcho:
    ip: str | None
    nonce: str | None = None
    via: str | None = None
    xff: str | None = None
    fwd: str | None = None


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
    text = body[:4096].decode("utf-8", errors="replace").strip()
    if not text:
        return ProbeEcho(ip=None)
    if text.startswith("{"):
        try:
            data = orjson.loads(text)
        except orjson.JSONDecodeError:
            return ProbeEcho(ip=None)
        if isinstance(data, dict):
            ip = data.get("ip") or data.get("origin")
            return ProbeEcho(ip=_clean_ip(str(ip)) if ip else None, nonce=data.get("n"))
        return ProbeEcho(ip=None)
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
        )
    return ProbeEcho(ip=_clean_ip(text.splitlines()[0]))


def _mentions(header: str | None, ip: str) -> bool:
    return bool(header) and ip in header  # type: ignore[operator]


def evaluate_exit(
    echo: ProbeEcho,
    *,
    expected_nonce: str | None,
    origin_ip: str | None,
    owned_probe: bool,
    plain_http: bool,
) -> ExitVerdict:
    if echo.ip is None:
        return ExitVerdict(False, ErrorKind.TAMPERED if owned_probe else ErrorKind.BAD_RESPONSE, None, False, None,
                           detail="no_ip_in_probe_reply")
    if owned_probe and expected_nonce is not None and echo.nonce != expected_nonce:
        return ExitVerdict(False, ErrorKind.TAMPERED, echo.ip, False, None, detail="nonce_mismatch")
    if origin_ip and echo.ip == origin_ip:
        return ExitVerdict(False, ErrorKind.BYPASS, echo.ip, False, None, detail="exit_ip_is_origin")

    anonymity: str | None
    if not plain_http:
        anonymity = "tunnel"
    elif not owned_probe:
        anonymity = None  # third-party probe does not echo request headers
    elif origin_ip and any(_mentions(h, origin_ip) for h in (echo.via, echo.xff, echo.fwd)):
        anonymity = "transparent"
    elif echo.via or echo.xff or echo.fwd:
        anonymity = "anonymous"
    else:
        anonymity = "elite"
    return ExitVerdict(True, None, echo.ip, origin_ip is not None, anonymity)
