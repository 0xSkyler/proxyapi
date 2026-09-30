"""Command line entry point.

    python -m proxy_quality worker              run collector + validators + scheduler (long-running)
    python -m proxy_quality api                 run the REST API (uvicorn)
    python -m proxy_quality migrate             apply database migrations
    python -m proxy_quality check URL [URL...]  validate proxies once and print the measurements
    python -m proxy_quality collect             fetch configured sources once and print counts
    python -m proxy_quality healthcheck URL     exit 0 if URL answers 2xx (for Docker healthchecks)
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import sys
import urllib.request

import orjson

from proxy_quality.config import Settings, load_validator_config
from proxy_quality.utils.logging_setup import configure_logging

log = logging.getLogger("proxy_quality")


def _run(coro):  # type: ignore[no-untyped-def]
    """Run on uvloop when available (Linux/Docker), plain asyncio otherwise (Windows dev)."""
    try:
        import uvloop  # type: ignore[import-not-found]
    except ImportError:
        return asyncio.run(coro)
    return uvloop.run(coro)


def cmd_worker(settings: Settings) -> int:
    from proxy_quality.worker.service import WorkerService

    _run(WorkerService(settings).run())
    return 0


def cmd_api(settings: Settings, args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run(
        "proxy_quality.api.app:create_app",
        factory=True,
        host=args.host or settings.api_host,
        port=args.port or settings.api_port,
        workers=args.workers or settings.api_workers,
        proxy_headers=True,
        forwarded_allow_ips="*",
        access_log=False,
        log_config=None,
        server_header=False,
        timeout_keep_alive=15,
        loop="auto",
        http="auto",
    )
    return 0


def cmd_migrate(settings: Settings) -> int:
    from proxy_quality.database.migrate import upgrade_head

    upgrade_head(settings)
    return 0


async def _check(settings: Settings, urls: list[str], https: bool) -> int:
    from proxy_quality.collector.normalizer import Normalizer
    from proxy_quality.domain import ProxyState
    from proxy_quality.validator.checker import ProxyChecker
    from proxy_quality.validator.probe import ProbeSet, detect_origin_ip

    vcfg = load_validator_config(settings)
    probes = ProbeSet(settings.probe_http_url, settings.probe_https_url, settings.probe_owned)
    await probes.resolve()
    origin = settings.origin_ip or await detect_origin_ip(settings.origin_ip_service_list)
    checker = ProxyChecker(vcfg, probes, origin_ip=origin)
    norm = Normalizer(vcfg.network, allow_private=settings.allow_private_addresses)
    rc = 0
    for i, url in enumerate(urls):
        rec = norm.try_normalize(url)
        if rec is None:
            print(orjson.dumps({"input": url, "error": "invalid proxy", "reasons": dict(norm.rejects)}).decode())
            rc = 1
            continue
        state = ProxyState(id=i, protocol=rec.protocol, host=rec.host, port=rec.port)
        result = await checker.check(state, check_https=https)
        out = {"proxy": rec.url, **dataclasses.asdict(result)}
        out.pop("origin_ip", None)
        print(orjson.dumps(out, default=str, option=orjson.OPT_INDENT_2).decode())
        rc = rc or (0 if result.ok else 2)
    return rc


async def _collect(settings: Settings) -> int:
    from proxy_quality.collector.normalizer import Normalizer
    from proxy_quality.collector.source_manager import SourceManager

    vcfg = load_validator_config(settings)
    mgr = SourceManager(settings, Normalizer(vcfg.network, allow_private=settings.allow_private_addresses))
    try:
        res = await mgr.collect()
    finally:
        await mgr.aclose()
    for r in res.reports:
        print(f"{r.name:32} {r.status:20} valid={r.valid_count:<7} unique={r.unique_count:<7} "
              f"http={r.http_status} err={r.error or '-'}")
    print(f"\nraw={res.raw_count} valid={res.valid_count} unique={len(res.dedup)} "
          f"unique_ips={res.dedup.unique_ips} by_protocol={res.dedup.protocol_counts()}")
    if res.reject_reasons:
        print(f"rejected: {res.reject_reasons}")
    return 0


def cmd_healthcheck(url: str, timeout: float) -> int:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - fixed internal URL
            return 0 if 200 <= resp.status < 300 else 1
    except Exception as exc:  # noqa: BLE001
        print(f"healthcheck failed: {exc}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="proxy_quality", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("worker")
    api = sub.add_parser("api")
    api.add_argument("--host")
    api.add_argument("--port", type=int)
    api.add_argument("--workers", type=int)
    sub.add_parser("migrate")
    chk = sub.add_parser("check")
    chk.add_argument("proxies", nargs="+", help="e.g. socks5://203.0.113.5:1080")
    chk.add_argument("--no-https", action="store_true")
    sub.add_parser("collect")
    hc = sub.add_parser("healthcheck")
    hc.add_argument("url")
    hc.add_argument("--timeout", type=float, default=4.0)
    args = parser.parse_args(argv)

    if args.command == "healthcheck":
        return cmd_healthcheck(args.url, args.timeout)

    settings = Settings()
    configure_logging(settings.log_level, settings.log_format if args.command in ("worker", "api") else "text")
    if args.command == "worker":
        return cmd_worker(settings)
    if args.command == "api":
        return cmd_api(settings, args)
    if args.command == "migrate":
        return cmd_migrate(settings)
    if args.command == "check":
        return asyncio.run(_check(settings, args.proxies, not args.no_https))
    if args.command == "collect":
        return asyncio.run(_collect(settings))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
