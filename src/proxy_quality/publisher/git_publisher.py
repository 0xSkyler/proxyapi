"""Optional publication of generated snapshots to a separate git branch.

Design choices that keep the repository healthy when publishing 24/7:

* Never commits more often than ``GIT_PUBLISH_INTERVAL_SECONDS`` (min 300 s,
  default 1800 s) and only when file content actually changed.
* ``squash`` mode (default): the data branch is an orphan branch holding a
  single commit that is force-pushed each time, so history (and clone size)
  does not grow by ~100 commits per day. ``append`` mode keeps history for
  those who want it (consider pruning periodically).
* The source-code branch is never touched: a dedicated work directory with
  its own clone of only the data branch is used.
* The access token is passed through the environment of the git process via
  an HTTP header, never written to disk or logs.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import shutil
import time

from proxy_quality.config import Settings

log = logging.getLogger(__name__)

PUBLISHED_FILES = (
    "all.txt", "http.txt", "https.txt", "socks4.txt", "socks5.txt",
    "premium.txt", "high.txt", "normal.txt", "backup.txt",
    "premium-http.txt", "premium-socks4.txt", "premium-socks5.txt",
    "proxies.json", "stats.json",
)


class GitPublishError(RuntimeError):
    pass


class GitPublisher:
    def __init__(self, settings: Settings) -> None:
        if not settings.git_publish_repo:
            raise ValueError("GIT_PUBLISH_REPO is required when GIT_PUBLISH_ENABLED=true")
        if not shutil.which("git"):
            raise ValueError("git executable not found in the container")
        self.s = settings
        self.workdir = settings.git_publish_workdir
        self.source_dir = settings.data_dir
        self.branch = settings.git_publish_branch

    def _env(self) -> dict[str, str]:
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.workdir.parent),
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_AUTHOR_NAME": self.s.git_publish_author_name,
            "GIT_AUTHOR_EMAIL": self.s.git_publish_author_email,
            "GIT_COMMITTER_NAME": self.s.git_publish_author_name,
            "GIT_COMMITTER_EMAIL": self.s.git_publish_author_email,
        }
        token = self.s.git_publish_token.get_secret_value() if self.s.git_publish_token else None
        if token and self.s.git_publish_repo and self.s.git_publish_repo.startswith("https://"):
            basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
            env.update(
                {
                    "GIT_CONFIG_COUNT": "1",
                    "GIT_CONFIG_KEY_0": "http.extraHeader",
                    "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}",
                }
            )
        return env

    async def _git(self, *args: str, check: bool = True, timeout: float = 120.0) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=self.workdir, env=self._env(),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except TimeoutError:
            proc.kill()
            raise GitPublishError(f"git {args[0]} timed out") from None
        text = out.decode(errors="replace").strip()
        if check and proc.returncode != 0:
            raise GitPublishError(f"git {args[0]} failed ({proc.returncode}): {text[-500:]}")
        return proc.returncode or 0, text

    async def _ensure_repo(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)
        if not (self.workdir / ".git").exists():
            await self._git("init", "-q")
            await self._git("remote", "add", "origin", self.s.git_publish_repo or "")
        else:
            await self._git("remote", "set-url", "origin", self.s.git_publish_repo or "")

    async def publish(self) -> bool:
        """Copy current snapshot files and push if anything changed. Returns True if pushed."""
        await self._ensure_repo()
        squash = self.s.git_publish_mode == "squash"
        if squash:
            # fresh orphan commit every time; the branch always has exactly one commit
            await self._git("checkout", "-q", "--orphan", f"tmp-{int(time.time())}")
            await self._git("rm", "-rq", "--cached", ".", check=False)
        else:
            code, _ = await self._git("fetch", "-q", "--depth", "1", "origin", self.branch, check=False)
            if code == 0:
                await self._git("checkout", "-q", "-B", self.branch, "FETCH_HEAD")
            else:
                await self._git("checkout", "-q", "--orphan", self.branch)

        for name in PUBLISHED_FILES:
            src = self.source_dir / name
            if src.exists():
                shutil.copyfile(src, self.workdir / name)
        (self.workdir / "README.md").write_text(
            "# Proxy snapshots\n\nAutomatically generated by proxy-quality-api. Only recently validated proxies "
            "are listed. See `stats.json` for generation time and statistics.\n",
            encoding="utf-8",
        )
        await self._git("add", "-A", "--", *PUBLISHED_FILES, "README.md")

        if not squash:
            code, _ = await self._git("diff", "--cached", "--quiet", check=False)
            if code == 0:
                log.info("git publish skipped: no changes")
                return False
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        await self._git("commit", "-q", "-m", f"Proxy snapshot {stamp}")
        if squash:
            await self._git("branch", "-M", self.branch)
            await self._git("push", "-q", "--force", "origin", f"{self.branch}:{self.branch}")
            # drop unreachable objects so the work dir does not grow forever
            await self._git("reflog", "expire", "--expire=now", "--all", check=False)
            await self._git("gc", "-q", "--prune=now", check=False, timeout=300)
        else:
            await self._git("push", "-q", "origin", f"HEAD:{self.branch}")
        log.info("snapshot published to git", extra={"branch": self.branch, "mode": self.s.git_publish_mode})
        return True
