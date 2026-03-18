import json
import os
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urldefrag

import requests


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def load_json(path: Path, default: Any):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@dataclass
class Config:
    feeds_out_root: Path
    targets_filename: str

    raw_dirname: str
    meta_dirname: str
    state_filename: str

    proxy: Optional[str]
    verify_tls: Any  # bool or str(path)
    timeout_connect: int
    timeout_read: int
    user_agent: str

    delay_sec: float
    max_pages_per_feed: int


def load_config(path: Path) -> Config:
    data = json.loads(path.read_text(encoding="utf-8"))
    return Config(
        feeds_out_root=Path(data.get("feeds_out_root", "./feeds_out")),
        targets_filename=data.get("targets_filename", "fetch_targets.json"),

        raw_dirname=data.get("raw_dirname", "html_raw"),
        meta_dirname=data.get("meta_dirname", "html_meta"),
        state_filename=data.get("state_filename", "html_fetch_state.json"),

        proxy=data.get("proxy"),
        verify_tls=data.get("verify_tls", True),
        timeout_connect=int(data.get("timeout_connect", 5)),
        timeout_read=int(data.get("timeout_read", 60)),
        user_agent=str(data.get("user_agent", "nec-csirt-html-fetch/1.0")),

        delay_sec=float(data.get("delay_sec", 0.0)),
        max_pages_per_feed=int(data.get("max_pages_per_feed", 2000)),
    )


def build_proxies(proxy: Optional[str]) -> Optional[dict[str, str]]:
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def fetch_one_page(
    session: requests.Session,
    url: str,
    proxies: Optional[dict[str, str]],
    verify_tls: Any,
    timeout: tuple[int, int],
    user_agent: str,
    prev_state: dict[str, Any],
) -> dict[str, Any]:
    base_url, fragment = urldefrag(url)

    headers = {"User-Agent": user_agent}
    if prev_state.get("etag"):
        headers["If-None-Match"] = prev_state["etag"]
    if prev_state.get("last_modified"):
        headers["If-Modified-Since"] = prev_state["last_modified"]

    resp = session.get(
        base_url,
        proxies=proxies,
        verify=verify_tls,
        timeout=timeout,
        headers=headers,
        allow_redirects=True,
    )

    info: dict[str, Any] = {
        "source_url": url,
        "base_url": base_url,
        "fragment": fragment,
        "final_url": resp.url,
        "http_status": resp.status_code,
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "content_type": resp.headers.get("Content-Type"),
    }

    if resp.status_code == 304:
        info["not_modified"] = True
        return info

    resp.raise_for_status()
    content = resp.content
    digest = sha256_bytes(content)

    info["not_modified"] = False
    info["sha256"] = digest
    info["content_bytes"] = len(content)
    info["_content"] = content  # 保存のため一時保持
    return info


def process_feed_dir(feed_dir: Path, cfg: Config, session: requests.Session) -> dict[str, Any]:
    targets_path = feed_dir / cfg.targets_filename
    if not targets_path.exists():
        return {"feed_dir": str(feed_dir), "skipped": True, "reason": "no fetch_targets.json"}

    targets_doc = load_json(targets_path, default={})
    targets = targets_doc.get("targets") or []

    # PoC：差分0なら何もしない
    if not targets:
        return {"feed_dir": str(feed_dir), "skipped": True, "reason": "no targets (delta is empty)"}

    run_ts = now_iso()

    raw_dir = feed_dir / cfg.raw_dirname
    meta_dir = feed_dir / cfg.meta_dirname
    state_path = feed_dir / cfg.state_filename
    raw_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    state = load_json(state_path, default={
        "schema_version": 1,
        "created_at": run_ts,
        "last_run": None,
        "pages": {},    # base_url -> state (etag/last_modified/sha256/error...)
        "entries": {},  # entry_key -> last fetched info
    })

    proxies = build_proxies(cfg.proxy)
    timeout = (cfg.timeout_connect, cfg.timeout_read)

    # 同一 base_url は1回だけ取得（#fragment付きの重複対策）
    base_map: dict[str, list[dict[str, Any]]] = {}
    for t in targets[: cfg.max_pages_per_feed]:
        url = t.get("url")
        if not url:
            continue
        base_url, frag = urldefrag(url)
        base_map.setdefault(base_url, []).append({
            "entry_key": t.get("entry_key"),
            "url": url,
            "fragment": frag,
            "reason": t.get("reason"),
            "title": t.get("title"),
            "timestamp": t.get("timestamp"),
        })

    fetched = 0
    not_modified = 0
    errors = 0

    for base_url, refs in base_map.items():
        prev = state["pages"].get(base_url, {})
        try:
            rep_url = refs[0]["url"]
            info = fetch_one_page(
                session=session,
                url=rep_url,
                proxies=proxies,
                verify_tls=cfg.verify_tls,
                timeout=timeout,
                user_agent=cfg.user_agent,
                prev_state=prev,
            )

            page_state = {
                "last_checked": run_ts,
                "etag": info.get("etag"),
                "last_modified": info.get("last_modified"),
                "final_url": info.get("final_url"),
                "http_status": info.get("http_status"),
                "content_type": info.get("content_type"),
            }

            if info.get("not_modified"):
                not_modified += 1
                page_state["sha256"] = prev.get("sha256")
                page_state["raw_path"] = prev.get("raw_path")
            else:
                fetched += 1
                content = info.pop("_content")
                digest = info["sha256"]

                raw_path = raw_dir / f"{digest}.html"
                if not raw_path.exists():
                    raw_path.write_bytes(content)

                meta_path = meta_dir / f"{digest}.json"
                meta = {
                    "fetched_at": run_ts,
                    **info,
                    "raw_path": str(raw_path),
                    "refs": refs,
                }
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

                page_state["last_fetched"] = run_ts
                page_state["sha256"] = digest
                page_state["raw_path"] = str(raw_path)
                page_state["meta_path"] = str(meta_path)

            state["pages"][base_url] = page_state

            # 後工程用：entry_key→rawの紐づけも残す
            for r in refs:
                ek = r.get("entry_key")
                if not ek:
                    continue
                state["entries"][ek] = {
                    "last_targeted": run_ts,
                    "base_url": base_url,
                    "source_url": r.get("url"),
                    "fragment": r.get("fragment"),
                    "sha256": state["pages"][base_url].get("sha256"),
                    "raw_path": state["pages"][base_url].get("raw_path"),
                    "reason": r.get("reason"),
                    "title": r.get("title"),
                    "timestamp": r.get("timestamp"),
                }

        except Exception as ex:
            errors += 1
            state["pages"][base_url] = {
                "last_checked": run_ts,
                "error": {"type": ex.__class__.__name__, "message": str(ex)[:1000]},
            }

        if cfg.delay_sec:
            time.sleep(cfg.delay_sec)

    state["last_run"] = run_ts
    state["last_summary"] = {
        "targets_in_file": len(targets),
        "unique_base_urls": len(base_map),
        "fetched": fetched,
        "not_modified": not_modified,
        "errors": errors,
    }
    save_json(state_path, state)

    return {"feed_dir": str(feed_dir), "skipped": False, **state["last_summary"]}


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="get_maincontent_config.json")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    root = cfg.feeds_out_root
    if not root.exists():
        raise FileNotFoundError(f"feeds_out_root not found: {root}")

    session = requests.Session()
    run_ts = now_iso()

    results = []
    for feed_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        res = process_feed_dir(feed_dir, cfg, session)
        results.append(res)

    processed = sum(1 for r in results if not r.get("skipped"))
    skipped = len(results) - processed
    print(f"[{run_ts}] feed_dirs={len(results)} processed={processed} skipped={skipped}")
    for r in results:
        if r.get("skipped"):
            continue
        name = Path(r["feed_dir"]).name
        print(f"  - {name}: urls={r.get('unique_base_urls')} fetched={r.get('fetched')} 304={r.get('not_modified')} err={r.get('errors')}")


if __name__ == "__main__":
    main()
