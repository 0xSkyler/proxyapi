from __future__ import annotations

import pytest

from proxy_quality.collector.deduplicator import Deduplicator
from proxy_quality.collector.normalizer import InvalidProxy, Normalizer
from proxy_quality.collector.parser import available_parsers, get_parser
from proxy_quality.collector.source_manager import SourceManager
from proxy_quality.config import NetworkCfg, Settings
from proxy_quality.domain import ProxyRecord


@pytest.fixture
def norm() -> Normalizer:
    return Normalizer(NetworkCfg())


# ----------------------------------------------------------------------------- normalizer


@pytest.mark.parametrize(
    ("raw", "default", "expected"),
    [
        ("8.8.8.8:8080", "http", "http://8.8.8.8:8080"),
        ("socks5://8.8.4.4:1080", None, "socks5://8.8.4.4:1080"),
        ("SOCKS5H://8.8.4.4:1080", None, "socks5://8.8.4.4:1080"),
        ("socks4a://1.1.1.1:4145", None, "socks4://1.1.1.1:4145"),
        ("https://1.1.1.1:443", None, "http://1.1.1.1:443"),
        ("  1.0.0.1:3128   US  elite  ", "http", "http://1.0.0.1:3128"),
        ("1.0.0.1:3128,US", "socks4", "socks4://1.0.0.1:3128"),
        ("[2606:4700:4700::1111]:1080", "socks5", "socks5://[2606:4700:4700::1111]:1080"),
        ("[::ffff:8.8.8.8]:80", "http", "http://8.8.8.8:80"),
        ("8.8.8.8:80", None, "http://8.8.8.8:80"),  # auto -> auto_default_protocol
    ],
)
def test_normalize_valid(norm, raw, default, expected):
    assert norm.normalize(raw, default).url == expected


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("", "blank"),
        ("   ", "blank"),
        ("# comment", "blank"),
        ("8.8.8.8", "malformed"),
        ("8.8.8.8:", "malformed"),
        ("8.8.8.8:0", "invalid_port"),
        ("8.8.8.8:65536", "invalid_port"),
        ("8.8.8.8:25", "blocked_port"),
        ("999.1.1.1:80", "invalid_ip"),
        ("01.02.03.04:80", "invalid_ip"),
        ("10.0.0.1:8080", "non_public_ip"),
        ("127.0.0.1:1080", "non_public_ip"),
        ("169.254.169.254:80", "non_public_ip"),
        ("192.0.2.1:8080", "non_public_ip"),  # TEST-NET documentation range is not global
        ("ftp://8.8.8.8:21", "unsupported_protocol"),
        ("user:pass@8.8.8.8:8080", "credentials_unsupported"),
        ("proxy.example.com:8080", "hostname_disabled"),
        ("garbage", "malformed"),
    ],
)
def test_normalize_rejects(norm, raw, reason):
    with pytest.raises(InvalidProxy) as exc:
        norm.normalize(raw, "http")
    assert exc.value.reason == reason


def test_hostnames_when_enabled():
    n = Normalizer(NetworkCfg(allow_hostnames=True))
    assert n.normalize("Proxy.Example.COM:8080", "http").host == "proxy.example.com"
    with pytest.raises(InvalidProxy):
        n.normalize("printer.local:8080", "http")


def test_private_allowed_only_when_requested():
    assert Normalizer(NetworkCfg(), allow_private=True).normalize("127.0.0.1:1080", "socks5").host == "127.0.0.1"


def test_reject_counter(norm):
    assert norm.try_normalize("10.1.1.1:80") is None
    assert norm.try_normalize("bad") is None
    assert norm.rejects == {"non_public_ip": 1, "malformed": 1}


# ----------------------------------------------------------------------------- parsers


def test_registry_has_builtins():
    assert {"text", "regex", "csv", "json"} <= set(available_parsers())


def test_text_parser():
    entries = list(get_parser("text")("1.1.1.1:80\n\n# x\nsocks5://8.8.8.8:1080 extra\n", {}))
    assert [e.raw for e in entries] == ["1.1.1.1:80", "socks5://8.8.8.8:1080 extra"]


def test_regex_parser():
    text = "<td>1.1.1.1</td> junk socks5://8.8.8.8:1080, and 9.9.9.9:3128."
    entries = list(get_parser("regex")(text, {}))
    got = {(e.protocol, e.host, str(e.port)) for e in entries}
    assert (None, "9.9.9.9", "3128") in got
    assert ("socks5", "8.8.8.8", "1080") in got


def test_csv_parser_columns():
    text = "ip,port,type,country\n1.1.1.1,8080,socks5,US\n8.8.8.8,3128,http,DE\n"
    entries = list(get_parser("csv")(text, {"protocol_column": "type"}))
    assert [(e.protocol, e.host, e.port) for e in entries] == [
        ("socks5", "1.1.1.1", "8080"),
        ("http", "8.8.8.8", "3128"),
    ]


def test_csv_parser_proxy_column_no_header():
    entries = list(get_parser("csv")("1.1.1.1:80;x\n", {"has_header": False, "proxy_column": 0, "delimiter": ";"}))
    assert entries[0].raw == "1.1.1.1:80"


def test_json_parser_nested_objects_and_protocol_lists():
    text = '{"data": {"items": [{"ip": "1.1.1.1", "port": 1080, "protocols": ["socks4", "socks5"]}, "8.8.8.8:80"]}}'
    entries = list(
        get_parser("json")(text, {"items_path": "data.items", "protocol_field": "protocols"})
    )
    assert [(e.protocol, e.host, e.port, e.raw) for e in entries] == [
        ("socks4", "1.1.1.1", 1080, None),
        ("socks5", "1.1.1.1", 1080, None),
        (None, None, None, "8.8.8.8:80"),
    ]


def test_json_parser_invalid():
    with pytest.raises(ValueError):
        list(get_parser("json")("{not json", {}))


# ----------------------------------------------------------------------------- dedup


def test_deduplicator_tracks_sources_and_ips():
    d = Deduplicator()
    a = ProxyRecord("http", "1.1.1.1", 80)
    assert d.add(a, "s1") is True
    assert d.add(a, "s2") is False
    assert d.add(a, "s1") is False
    assert d.add(ProxyRecord("socks5", "1.1.1.1", 1080), "s1") is True
    assert len(d) == 2
    assert d.unique_ips == 1
    assert d.raw_count == 4
    assert dict(d.items())[a] == ["s1", "s2"]
    assert d.protocol_counts() == {"http": 1, "socks5": 1}


# ----------------------------------------------------------------------------- source manager


async def test_source_manager_file_source_and_licensing(tmp_path):
    lst = tmp_path / "list.txt"
    lst.write_text("socks5://1.1.1.1:1080\n8.8.8.8:3128\n10.0.0.1:80\njunk\n8.8.8.8:3128\n")
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "sources.yaml").write_text(
        f"""
sources:
  - name: local
    url: "{lst.as_uri()}"
    protocol: auto
    redistribution_verified: true
  - name: unverified
    url: "{lst.as_uri()}"
    protocol: http
  - name: disabled_src
    url: "{lst.as_uri()}"
    enabled: false
"""
    )
    settings = Settings(config_dir=cfg_dir, _env_file=None)
    mgr = SourceManager(settings, Normalizer(NetworkCfg()))
    try:
        res = await mgr.collect()
    finally:
        await mgr.aclose()
    status = {r.name: r.status for r in res.reports}
    assert status == {"local": "ok", "unverified": "skipped_unverified", "disabled_src": "disabled"}
    assert {r.url for r, _ in res.dedup.items()} == {"socks5://1.1.1.1:1080", "http://8.8.8.8:3128"}
    local = next(r for r in res.reports if r.name == "local")
    assert local.raw_count == 5 and local.valid_count == 2
    assert res.reject_reasons == {"non_public_ip": 1, "malformed": 1}
