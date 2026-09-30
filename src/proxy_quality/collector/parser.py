"""Source payload parsers.

Each parser turns a downloaded payload into ``RawEntry`` objects; normalization
happens afterwards. Parsers are isolated behind a small registry so a
source-specific format can be added without touching the pipeline::

    @register_parser("my_format")
    def parse_my_format(text: str, options: dict) -> Iterator[RawEntry]:
        ...

and then ``format: my_format`` in ``config/sources.yaml``.

Built-in formats:

``text``  newline separated ``ip:port`` / ``protocol://ip:port`` (extra columns ignored)
``regex`` extract every ``[scheme://]ip:port`` occurrence from arbitrary text
``csv``   delimited rows; options ``proxy_column`` or ``ip_column`` + ``port_column``
          (+ optional ``protocol_column``), ``delimiter``, ``has_header``
``json``  list of strings or objects; options ``items_path`` (dot path),
          ``proxy_field`` or ``ip_field`` + ``port_field`` (+ ``protocol_field``)
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import orjson


@dataclass(slots=True)
class RawEntry:
    raw: str | None = None  # a complete "[scheme://]host:port" string
    protocol: str | None = None
    host: str | None = None
    port: str | int | None = None


ParserFn = Callable[[str, dict[str, Any]], Iterator[RawEntry]]
_PARSERS: dict[str, ParserFn] = {}


def register_parser(name: str) -> Callable[[ParserFn], ParserFn]:
    def deco(fn: ParserFn) -> ParserFn:
        _PARSERS[name] = fn
        return fn

    return deco


def get_parser(name: str) -> ParserFn:
    try:
        return _PARSERS[name]
    except KeyError:
        raise ValueError(f"unknown source format {name!r}; available: {sorted(_PARSERS)}") from None


def available_parsers() -> list[str]:
    return sorted(_PARSERS)


# ----------------------------------------------------------------------------- text


@register_parser("text")
def parse_text(text: str, options: dict[str, Any]) -> Iterator[RawEntry]:
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            yield RawEntry(raw=line)


_REGEX = re.compile(
    r"(?:(?P<scheme>socks5h?|socks4a?|https?)://)?"
    r"(?P<host>(?:\d{1,3}\.){3}\d{1,3})[:\s](?P<port>\d{1,5})\b",
    re.IGNORECASE,
)


@register_parser("regex")
def parse_regex(text: str, options: dict[str, Any]) -> Iterator[RawEntry]:
    for m in _REGEX.finditer(text):
        yield RawEntry(protocol=m.group("scheme"), host=m.group("host"), port=m.group("port"))


# ----------------------------------------------------------------------------- csv


def _col(row: list[str], header: dict[str, int] | None, spec: str | int | None) -> str | None:
    if spec is None:
        return None
    if isinstance(spec, int) or (isinstance(spec, str) and spec.isdigit()):
        idx = int(spec)
    else:
        if header is None:
            return None
        idx = header.get(str(spec).strip().lower(), -1)
    if 0 <= idx < len(row):
        return row[idx].strip()
    return None


@register_parser("csv")
def parse_csv(text: str, options: dict[str, Any]) -> Iterator[RawEntry]:
    delimiter = options.get("delimiter")
    if not delimiter:
        try:
            delimiter = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|").delimiter
        except csv.Error:
            delimiter = ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    has_header = options.get("has_header", True)
    header: dict[str, int] | None = None
    if has_header:
        first = next(reader, None)
        if first is None:
            return
        header = {name.strip().lower(): i for i, name in enumerate(first)}
    proxy_col = options.get("proxy_column")
    ip_col = options.get("ip_column", "ip" if proxy_col is None else None)
    port_col = options.get("port_column", "port" if proxy_col is None else None)
    proto_col = options.get("protocol_column")
    for row in reader:
        if not row or all(not c.strip() for c in row):
            continue
        proto = _col(row, header, proto_col)
        if proxy_col is not None:
            raw = _col(row, header, proxy_col)
            if raw:
                if proto and "://" not in raw:
                    raw = f"{proto}://{raw}"
                yield RawEntry(raw=raw)
        else:
            yield RawEntry(protocol=proto, host=_col(row, header, ip_col), port=_col(row, header, port_col))


# ----------------------------------------------------------------------------- json


def _dig(obj: Any, path: str | None) -> Any:
    if not path:
        return obj
    for part in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list) and part.isdigit() and int(part) < len(obj):
            obj = obj[int(part)]
        else:
            return None
    return obj


@register_parser("json")
def parse_json(text: str, options: dict[str, Any]) -> Iterator[RawEntry]:
    try:
        data = orjson.loads(text)
    except orjson.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON payload: {exc}") from None
    items = _dig(data, options.get("items_path"))
    if isinstance(items, dict):
        items = list(items.values())
    if not isinstance(items, list):
        raise ValueError("JSON items_path does not point to a list")
    proxy_field = options.get("proxy_field")
    ip_field = options.get("ip_field", "ip")
    port_field = options.get("port_field", "port")
    proto_field = options.get("protocol_field", "protocol")
    for item in items:
        if isinstance(item, str):
            yield RawEntry(raw=item)
            continue
        if not isinstance(item, dict):
            continue
        protos = _dig(item, proto_field) if proto_field else None
        if not isinstance(protos, list):
            protos = [protos]
        for proto in protos:
            proto_s = str(proto) if proto not in (None, "") else None
            if proxy_field:
                raw = _dig(item, proxy_field)
                if isinstance(raw, str) and raw:
                    if proto_s and "://" not in raw:
                        raw = f"{proto_s}://{raw}"
                    yield RawEntry(raw=raw)
            else:
                host = _dig(item, ip_field)
                port = _dig(item, port_field)
                yield RawEntry(
                    protocol=proto_s,
                    host=str(host) if host is not None else None,
                    port=port if isinstance(port, (int, str)) else None,
                )
