from __future__ import annotations

import pytest
from pydantic import ValidationError

from proxy_quality.config import (
    ScoringConfig,
    SourcesFile,
    ValidatorConfig,
    load_scoring_config,
    load_sources_config,
    load_validator_config,
)
from proxy_quality.validator.exit_ip_check import evaluate_exit, parse_probe_body
from proxy_quality.validator.probe import ProbeSet, ProbeTarget
from proxy_quality.validator.results import ErrorKind


def test_parse_owned_probe_body():
    e = parse_probe_body(b"ip=198.51.100.4\nn=abc123\nvia=1.1 squid\nxff=\nfwd=\n")
    assert (e.ip, e.nonce, e.via, e.xff, e.fwd) == ("198.51.100.4", "abc123", "1.1 squid", None, None)


def test_parse_json_plain_and_proxyscrape_judge_bodies():
    assert parse_probe_body(b'{"ip":"198.51.100.4"}').ip == "198.51.100.4"
    assert parse_probe_body(b"198.51.100.4\n").ip == "198.51.100.4"
    judge = parse_probe_body(
        b"AZ Environment variables\nREMOTE_ADDR = 198.51.100.4\nHTTP_VIA = 1.1 squid\n"
    )
    assert judge.ip == "198.51.100.4" and judge.proxyscrape_judge is True
    assert parse_probe_body(b"<html>").ip is None
    assert parse_probe_body(b"").ip is None


@pytest.mark.parametrize(
    ("body", "nonce", "origin", "owned", "plain", "err", "anon"),
    [
        (b"ip=198.51.100.4\nn=N1\n", "N1", "203.0.113.7", True, True, None, "elite"),
        (b"ip=198.51.100.4\nn=N1\nvia=1.1 x\n", "N1", "203.0.113.7", True, True, None, "anonymous"),
        (b"ip=198.51.100.4\nn=N1\nxff=203.0.113.7\n", "N1", "203.0.113.7", True, True, None, "transparent"),
        (b"ip=198.51.100.4\nn=OTHER\n", "N1", "203.0.113.7", True, True, ErrorKind.TAMPERED, None),
        (b"ip=203.0.113.7\nn=N1\n", "N1", "203.0.113.7", True, False, ErrorKind.BYPASS, None),
        (b'{"ip":"198.51.100.4"}', None, "203.0.113.7", False, True, None, None),
        (b"<html>ad</html>", None, None, False, True, ErrorKind.BAD_RESPONSE, None),
        (b"ip=198.51.100.4\nn=N1\n", "N1", None, True, False, None, "elite"),
        (
            b"AZ Environment variables\nREMOTE_ADDR = 198.51.100.4\n",
            None, "203.0.113.7", False, True, None, "elite",
        ),
        (
            b"AZ Environment variables\nREMOTE_ADDR = 198.51.100.4\nHTTP_VIA = 1.1 squid\n",
            None, "203.0.113.7", False, True, None, "anonymous",
        ),
        (
            b"AZ Environment variables\nREMOTE_ADDR = 203.0.113.7\n",
            None, "203.0.113.7", False, True, None, "transparent",
        ),
    ],
)
def test_evaluate_exit(body, nonce, origin, owned, plain, err, anon):
    v = evaluate_exit(parse_probe_body(body), expected_nonce=nonce, origin_ip=origin, owned_probe=owned,
                      plain_http=plain)
    assert v.error == err
    assert v.ok == (err is None)
    if v.ok:
        assert v.anonymity == anon
        assert v.verified == (origin is not None)


def test_probe_target_urls():
    t = ProbeTarget.parse("http://probe.example.org:8080/probe?x=1")
    assert t.host_header == "probe.example.org:8080"
    assert t.path_with_nonce("abc") == "/probe?x=1&n=abc"
    assert t.absolute_url(None) == "http://probe.example.org:8080/probe?x=1"
    assert ProbeTarget.parse("https://h.example/").port == 443
    with pytest.raises(ValueError):
        ProbeSet("https://a.example/", "https://b.example/", owned=False)


def test_shipped_configs_are_valid(repo_settings):
    v = load_validator_config(repo_settings)
    s = load_scoring_config(repo_settings)
    src = load_sources_config(repo_settings)
    assert sum(s.weights.values()) == pytest.approx(100)
    assert v.concurrency.fast == 300
    assert len(src.sources) == 1
    source = src.sources[0]
    assert source.name == "proxyscrape_v4"
    assert source.enabled is True and source.redistribution_verified is True
    assert source.min_interval_seconds == 120
    assert "api.proxyscrape.com/v4/free-proxy-list/get" in source.url


def test_env_overrides(repo_settings):
    repo_settings.fast_validation_concurrency = 50
    repo_settings.adaptive_concurrency = False
    v = load_validator_config(repo_settings)
    assert v.concurrency.fast == 50 and v.concurrency.adaptive.enabled is False


def test_invalid_configs_rejected():
    with pytest.raises(ValidationError):
        ScoringConfig(weights={"reliability": 50})  # does not sum to 100
    with pytest.raises(ValidationError):
        ScoringConfig(classes={"premium": 50, "high": 80, "normal": 65, "backup": 50})
    with pytest.raises(ValidationError):
        ValidatorConfig.model_validate({"timeouts": {"tcp_conect": 1}})  # typo is an error, not ignored
    with pytest.raises(ValidationError):
        SourcesFile.model_validate({"sources": [{"name": "a", "url": "ftp://x"}]})
    with pytest.raises(ValidationError):
        SourcesFile.model_validate(
            {"sources": [{"name": "a", "url": "http://x"}, {"name": "a", "url": "http://y"}]}
        )
