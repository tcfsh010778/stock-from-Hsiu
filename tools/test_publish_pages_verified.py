import json
from pathlib import Path
import pytest
from tools import publish_pages_verified as m


class Clock:
    def __init__(self):
        self.value = 0

    def now(self):
        return self.value

    def sleep(self, n):
        self.value += n


def docs(tmp_path):
    root = tmp_path / "docs"
    root.mkdir(exist_ok=True)
    (root / "index.html").write_bytes(b"home")
    (root / "data").mkdir(exist_ok=True)
    (root / "data/review_queue.json").write_bytes(b"queue")
    return root


def call(tmp_path, statuses, fetch=None, trigger=None, timeout=60):
    root = docs(tmp_path)
    clock = Clock()
    items = iter(statuses)
    return m.verify_publish(
        repo="owner/repo",
        commit="a" * 40,
        docs_dir=root,
        trigger=trigger
        or (
            lambda repo: {
                "url": "https://api.github.com/repos/owner/repo/pages/builds/1"
            }
        ),
        build_status=lambda endpoint: next(items),
        pages_url=lambda repo: "https://owner.github.io/repo/",
        fetch=fetch or (lambda url: b"home" if "index.html" in url else b"queue"),
        monotonic=clock.now,
        sleep=clock.sleep,
        build_timeout=timeout,
        cdn_timeout=timeout,
    )


def test_success_polls_exact_commit_and_bytes(tmp_path):
    result = call(
        tmp_path, [{"status": "building"}, {"status": "built", "commit": "a" * 40}]
    )
    assert result["status"] == "verified" and result["verified_paths"] == [
        "data/review_queue.json",
        "index.html",
    ]


def test_wrong_commit_and_build_failure_are_rejected(tmp_path):
    with pytest.raises(m.PublishError, match="wrong commit"):
        call(tmp_path, [{"status": "built", "commit": "b" * 40}])
    with pytest.raises(m.PublishError, match="build failed"):
        call(tmp_path, [{"status": "errored"}])


def test_build_timeout_is_rejected(tmp_path):
    with pytest.raises(m.PublishError, match="timed out"):
        call(tmp_path, [{"status": "building"}] * 5, timeout=20)


def test_cdn_mismatch_is_rejected(tmp_path):
    with pytest.raises(m.PublishError, match="CDN bytes"):
        call(
            tmp_path,
            [{"status": "built", "commit": "a" * 40}],
            fetch=lambda url: b"wrong",
            timeout=20,
        )


def test_untrusted_build_url_is_rejected(tmp_path):
    with pytest.raises(m.PublishError, match="untrusted"):
        call(
            tmp_path,
            [],
            trigger=lambda repo: {
                "url": "https://evil.test/repos/owner/repo/pages/builds/1"
            },
        )


def test_numeric_repository_alias_uses_configured_repository(tmp_path):
    url = "https://api.github.com/repositories/1225747295/pages/builds/latest"
    assert m.safe_build_endpoint("owner/repo", url) == "repos/owner/repo/pages/builds/latest"
    assert call(tmp_path, [{"status": "built", "commit": "a" * 40}],
                trigger=lambda repo: {"url": url})["status"] == "verified"
    with pytest.raises(m.PublishError, match="wrong commit"):
        call(tmp_path, [{"status": "built", "commit": "b" * 40}],
             trigger=lambda repo: {"url": url})


@pytest.mark.parametrize("url", [
    "https://evil.test/repositories/1225747295/pages/builds/latest",
    "https://api.github.com/repositories/1225747295/pages/builds/latest?redirect=evil",
    "https://api.github.com/repositories/1225747295/pages/builds/latest/other",
])
def test_numeric_alias_rejects_untrusted_variants(url):
    with pytest.raises(m.PublishError, match="untrusted"):
        m.safe_build_endpoint("owner/repo", url)


def test_workspace_rejects_stale_data_even_when_html_matches(tmp_path):
    root = docs(tmp_path)
    (root/'data/stocks').mkdir()
    index={'schema_version':'sfz-mda-workspace-1','stocks':[
        {'stock_id':'2330','market':'上市','price_verified':True,'detail':'data/stocks/2330.json'},
        {'stock_id':'6488','market':'上櫃','price_verified':True,'detail':'data/stocks/6488.json'}]}
    (root/'data/index.json').write_text(json.dumps(index),encoding='utf-8')
    for name in ('app.js','style.css','data/stocks/2330.json','data/stocks/6488.json'):
        (root/name).write_text('current',encoding='utf-8')
    from urllib.parse import urlparse
    def fetch(url):
        rel=urlparse(url).path.removeprefix('/repo/')
        return b'stale' if rel=='data/index.json' else (root/rel).read_bytes()
    with pytest.raises(m.PublishError,match='data/index.json'):
        call(tmp_path,[{'status':'built','commit':'a'*40}],fetch=fetch,timeout=20)
    result=call(tmp_path,[{'status':'built','commit':'a'*40}],fetch=lambda url:(root/urlparse(url).path.removeprefix('/repo/')).read_bytes())
    assert set(result['verified_paths'])=={'index.html','app.js','style.css','data/index.json','data/stocks/2330.json','data/stocks/6488.json'}
