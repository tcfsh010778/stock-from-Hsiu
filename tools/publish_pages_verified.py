"""Publish GitHub Pages and verify the deployed bytes belong to one exact commit."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Callable, Any
from urllib.parse import urlparse, urlencode, urlunparse, parse_qsl
from urllib.request import Request, urlopen

POLL_SECONDS = 20
BUILD_TIMEOUT = 15 * 60
CDN_TIMEOUT = 2 * 60
VERIFY_PATHS = (
    "index.html",
    "data/review_queue.json",
    "data/daily_market_flow.json",
    "v2/data/index.json",
    "v2/data/2353.json",
)


class PublishError(RuntimeError):
    pass


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_build_endpoint(repo: str, value: str | None) -> str:
    latest = f"repos/{repo}/pages/builds/latest"
    if not value:
        return latest
    parsed = urlparse(value)
    expected_prefix = f"/repos/{repo}/pages/builds/"
    if (
        parsed.scheme == "https"
        and parsed.netloc == "api.github.com"
        and parsed.path.startswith(expected_prefix)
    ):
        return parsed.path.lstrip("/")
    raise PublishError("Pages build response returned an untrusted status URL")


def cache_bust(url: str, commit: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["deploy"] = commit
    return urlunparse(parsed._replace(query=urlencode(query)))


def verify_publish(
    *,
    repo: str,
    commit: str,
    docs_dir: Path,
    trigger: Callable[[str], dict[str, Any]],
    build_status: Callable[[str], dict[str, Any]],
    pages_url: Callable[[str], str],
    fetch: Callable[[str], bytes],
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    build_timeout: int = BUILD_TIMEOUT,
    cdn_timeout: int = CDN_TIMEOUT,
) -> dict[str, Any]:
    if (
        not commit
        or any(c not in "0123456789abcdefABCDEF" for c in commit)
        or len(commit) < 7
    ):
        raise PublishError("expected commit is not a hexadecimal Git SHA")
    if not docs_dir.is_dir():
        raise PublishError("docs directory does not exist")
    local = {
        rel: (docs_dir / rel).read_bytes()
        for rel in VERIFY_PATHS
        if (docs_dir / rel).is_file()
    }
    if "index.html" not in local:
        raise PublishError("docs/index.html is required")
    response = trigger(repo)
    endpoint = safe_build_endpoint(repo, str(response.get("url") or "") or None)
    deadline = monotonic() + build_timeout
    while True:
        state = build_status(endpoint)
        status = str(state.get("status") or "").lower()
        built_commit = str(state.get("commit") or "")
        if status == "built":
            if built_commit.lower() != commit.lower():
                raise PublishError(
                    f"Pages built wrong commit: expected={commit} actual={built_commit or 'missing'}"
                )
            break
        if status in {"errored", "error", "failed", "cancelled"}:
            raise PublishError(f"Pages build failed: status={status}")
        if monotonic() >= deadline:
            raise PublishError(f"Pages build timed out: status={status or 'missing'}")
        sleep(POLL_SECONDS)
    base = pages_url(repo).rstrip("/") + "/"
    parsed_base = urlparse(base)
    if parsed_base.scheme != "https" or not parsed_base.netloc:
        raise PublishError("Pages html_url is missing or untrusted")
    pending = dict(local)
    cdn_deadline = monotonic() + cdn_timeout
    while pending:
        mismatched = []
        for rel, expected in list(pending.items()):
            try:
                actual = fetch(cache_bust(base + rel, commit))
            except Exception:
                actual = b""
            if digest(actual) == digest(expected):
                pending.pop(rel)
            else:
                mismatched.append(rel)
        if not pending:
            break
        if monotonic() >= cdn_deadline:
            raise PublishError(
                "Pages CDN bytes did not match local docs: "
                + ",".join(sorted(mismatched))
            )
        sleep(POLL_SECONDS)
    return {
        "status": "verified",
        "commit": commit,
        "build_status": "built",
        "verified_paths": sorted(local),
    }


def gh_json(endpoint: str, *, method: str = "GET") -> dict[str, Any]:
    command = ["gh", "api"]
    if method != "GET":
        command += ["--method", method]
    command.append(endpoint)
    result = subprocess.run(
        command, check=True, capture_output=True, text=True, encoding="utf-8"
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        raise PublishError("GitHub API returned a non-object")
    return value


def http_bytes(url: str) -> bytes:
    with urlopen(
        Request(
            url,
            headers={
                "User-Agent": "stock-from-Hsiu-pages-verifier/1",
                "Cache-Control": "no-cache",
            },
        ),
        timeout=30,
    ) as response:
        return response.read()


def git_head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--commit")
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"))
    args = parser.parse_args(argv)
    commit = args.commit or git_head()
    result = verify_publish(
        repo=args.repo,
        commit=commit,
        docs_dir=args.docs_dir,
        trigger=lambda repo: gh_json(f"repos/{repo}/pages/builds", method="POST"),
        build_status=lambda endpoint: gh_json(endpoint),
        pages_url=lambda repo: str(
            gh_json(f"repos/{repo}/pages").get("html_url") or ""
        ),
        fetch=http_bytes,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
