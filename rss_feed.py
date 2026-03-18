import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

import requests
import feedparser

try:
    import tomllib  # Python 3.11+
except Exception:
    tomllib = None


# ========= ユーティリティ =========

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


def _struct_time_to_iso(st) -> Optional[str]:
    if not st:
        return None
    dt_utc = datetime(*st[:6], tzinfo=timezone.utc)
    return dt_utc.astimezone().isoformat(timespec="seconds")


def _to_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, (list, tuple)):
        if not x:
            return None
        x = x[0]
    if isinstance(x, bytes):
        x = x.decode("utf-8", errors="replace")
    s = str(x).strip()
    return s or None


def make_unique_key(entry: Mapping[str, Any]) -> Optional[str]:
    # 一般RSSで一番事故が少ないのは「id があれば id」「なければ link」
    return _to_str(entry.get("id")) or _to_str(entry.get("link"))


def pick_fields(entry: Mapping[str, Any]) -> dict[str, Any]:
    updated = entry.get("updated") or entry.get("published") or entry.get("dc_date")
    updated_parsed = entry.get("updated_parsed") or entry.get("published_parsed") or entry.get("dc_date_parsed")

    return {
        "id": _to_str(entry.get("id")),
        "dc_identifier": _to_str(entry.get("dc_identifier")),
        "title": _to_str(entry.get("title")),
        "link": _to_str(entry.get("link")),
        "timestamp": _to_str(updated),
        "timestamp_parsed": _struct_time_to_iso(updated_parsed),
    }


def is_changed(old_item: Mapping[str, Any], new_item: Mapping[str, Any]) -> bool:
    old_ts = old_item.get("timestamp_parsed")
    new_ts = new_item.get("timestamp_parsed")

    # timestamp_parsed が取れるなら優先して更新判定
    if new_ts and (not old_ts or new_ts > old_ts):
        return True

    # 保険：主要フィールド差分
    for k in ("title", "link", "timestamp"):
        if (old_item.get(k) or "") != (new_item.get(k) or ""):
            return True

    return False


def sanitize_id(s: str) -> str:
    # ディレクトリ名として安全なIDにする
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    return s.strip("_") or "feed"


# ========= 設定読み込み =========

@dataclass
class FeedConfig:
    feed_id: str
    name: str
    url: str


@dataclass
class GlobalConfig:
    out_root: Path
    status_file: Path
    proxy: Optional[str]
    timeout_connect: int
    timeout_read: int
    verify_tls: Any  # bool or str(path)
    user_agent: str


def load_config(config_path: Path) -> tuple[GlobalConfig, list[FeedConfig]]:
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")

    if config_path.suffix.lower() == ".toml":
        if tomllib is None:
            raise RuntimeError("tomllib is not available. Use Python 3.11+ or JSON config.")
        data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    elif config_path.suffix.lower() == ".json":
        data = json.loads(config_path.read_text(encoding="utf-8"))
    else:
        raise ValueError("config must be .toml or .json")

    g = data.get("global", {})
    out_root = Path(g.get("out_root", "./feeds_out"))
    status_file = Path(g.get("status_file", "./status.json"))
    proxy = g.get("proxy")
    timeout_connect = int(g.get("timeout_connect", 5))
    timeout_read = int(g.get("timeout_read", 30))
    verify_tls = g.get("verify_tls", True)
    user_agent = g.get("user_agent", "nec-csirt-pubinfo-rss/1.0")

    feeds_data = data.get("feeds") or []
    feeds: list[FeedConfig] = []
    for f in feeds_data:
        fid = f.get("id") or sanitize_id(f.get("name", "feed"))
        feeds.append(FeedConfig(
            feed_id=sanitize_id(str(fid)),
            name=str(f.get("name", fid)),
            url=str(f["url"]),
        ))

    gc = GlobalConfig(
        out_root=out_root,
        status_file=status_file,
        proxy=proxy,
        timeout_connect=timeout_connect,
        timeout_read=timeout_read,
        verify_tls=verify_tls,
        user_agent=user_agent,
    )
    return gc, feeds


# ========= 収集処理 =========

def build_proxies(proxy: Optional[str]) -> Optional[dict[str, str]]:
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def collect_one_feed(
    session: requests.Session,
    gc: GlobalConfig,
    fc: FeedConfig,
    status: dict[str, Any],
) -> dict[str, Any]:
    """
    1フィード分を収集し、items.jsonを更新する。
    戻り値はこのフィードの実行結果（statusへ格納する用）
    """
    run_ts = now_iso()

    feed_dir = gc.out_root / fc.feed_id
    items_file = feed_dir / "items.json"
    items = load_json(items_file, default={})

    # 前回状態（ETag/Last-Modified）を status.json から引く
    feeds_state = status.setdefault("feeds", {})
    prev = feeds_state.get(fc.feed_id, {})

    headers = {"User-Agent": gc.user_agent}
    if prev.get("etag"):
        headers["If-None-Match"] = prev["etag"]
    if prev.get("last_modified"):
        headers["If-Modified-Since"] = prev["last_modified"]

    proxies = build_proxies(gc.proxy)

    result: dict[str, Any] = {
        "feed_id": fc.feed_id,
        "name": fc.name,
        "feed_url": fc.url,
        "items_file": str(items_file),
        "last_run": run_ts,
        "ok": False,
    }

    try:
        resp = session.get(
            fc.url,
            proxies=proxies,
            timeout=(gc.timeout_connect, gc.timeout_read),
            verify=gc.verify_tls,
            headers=headers,
        )
        result["http_status"] = resp.status_code

        if resp.status_code == 304:
            # 差分無し：PoCでは、targets を空で上書き
            fetch_targets_file = feed_dir / "fetch_targets.json"
            save_json(fetch_targets_file, {
                "shcama_version": 1,
                "generated_at": run_ts,
                "feed_id": fc.feed_id,
                "feed_name": fc.name,
                "feed_url": fc.url,
                "targets_count": 0,
                "targets": [],
                "note": "feed no modified(304)",
            })
            
            result.update({
                "ok": True,
                "not_modified": True,
                "entries_in_feed": prev.get("entries_in_feed"),
                "new_count_last_run": 0,
                "update_count_last_run": 0,
                "total_saved_items": len(items),
                "etag": prev.get("etag"),
                "last_modified": prev.get("last_modified"),
            })
            return result

        resp.raise_for_status()

        feed = feedparser.parse(resp.content)

        # bozoは「壊れてる可能性」だが、entriesが取れるケースもあるため“警告扱い”で継続
        if getattr(feed, "bozo", 0):
            result["bozo"] = True
            result["bozo_exception"] = str(getattr(feed, "bozo_exception", ""))[:500]
        
        new_keys = []
        update_keys = []
        new_count = 0
        update_count = 0

        for e in feed.entries:
            key = make_unique_key(e)
            if not key:
                continue

            new_item = pick_fields(e)
            
            if key not in items:
                new_item["first_seen"] = run_ts
                new_item["last_seen"] = run_ts
                new_item["last_changed"] = run_ts
                new_item["revision"] = 1
                items[key] = new_item
                new_count += 1
                new_keys.append(key)
            else:
                items[key]["last_seen"] = run_ts
                if is_changed(items[key], new_item):
                    items[key].update(new_item)
                    items[key]["last_changed"] = run_ts
                    items[key]["revision"] = int(items[key].get("revision", 1)) + 1
                    update_count += 1
                    update_keys.append(key)

        save_json(items_file, items)

        targets = []

        for k in new_keys:
            it = items.get(k, {})
            targets.append({
                "entry_key": k,
                "url": it.get("link") or k,
                "title": it.get("title"),
                "timestamp": it.get("timestamp"),
                "reason": "new",
            })

        for k in update_keys:
            it = items.get(k, {})
            targets.append({
                "entry_key": k,
                "url": it.get("link") or k,
                "title": it.get("title"),
                "timestamp": it.get("timestamp"),
                "reason": "updated",
            })
        
        fetch_targets_file = feed_dir / "fetch_targets.json"

        save_json(fetch_targets_file, {
            "shcama_version": 1,
            "generated_at": run_ts,
            "feed_id": fc.feed_id,
            "feed_name": fc.name,
            "feed_url": fc.url,
            "targets_count": len(targets),
            "targets": targets,
        })

        result["fetch_tagets_file"] = str(fetch_targets_file)
        result["targets_count"] = len(targets)

        result.update({
            "ok": True,
            "not_modified": False,
            "entries_in_feed": len(feed.entries),
            "new_count_last_run": new_count,
            "update_count_last_run": update_count,
            "total_saved_items": len(items),
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
        })
        return result

    except Exception as ex:
        # 失敗しても全体処理は継続できるよう、ここで握って結果として返す
        result["ok"] = False
        result["error"] = {
            "type": ex.__class__.__name__,
            "message": str(ex)[:1000],
        }
        return result


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="feeds.toml", help="config file (.toml or .json)")
    args = ap.parse_args()

    config_path = Path(args.config)
    gc, feeds = load_config(config_path)

    status = load_json(gc.status_file, default={
        "schema_version": 1,
        "created_at": now_iso(),
        "last_run": None,
        "feeds": {},
        "last_summary": {},
    })

    run_ts = now_iso()
    status["last_run"] = run_ts

    session = requests.Session()

    results = []
    for fc in feeds:
        res = collect_one_feed(session, gc, fc, status)
        results.append(res)
        status["feeds"][fc.feed_id] = res  # feed_idごとに上書き

    # 集計
    ok_count = sum(1 for r in results if r.get("ok"))
    ng_count = len(results) - ok_count
    new_total = sum(int(r.get("new_count_last_run", 0) or 0) for r in results if r.get("ok"))
    upd_total = sum(int(r.get("update_count_last_run", 0) or 0) for r in results if r.get("ok"))

    status["last_summary"] = {
        "run_ts": run_ts,
        "feeds_total": len(results),
        "feeds_ok": ok_count,
        "feeds_ng": ng_count,
        "new_total": new_total,
        "update_total": upd_total,
    }

    save_json(gc.status_file, status)

    # 表示（最低限）
    print(f"[{run_ts}] feeds_total={len(results)} ok={ok_count} ng={ng_count} new_total={new_total} update_total={upd_total}")
    for r in results:
        if r.get("ok"):
            nm = " (304)" if r.get("not_modified") else ""
            print(f"  - OK{nm} {r['feed_id']}: entries={r.get('entries_in_feed')} new={r.get('new_count_last_run')} upd={r.get('update_count_last_run')}")
        else:
            err = r.get("error", {})
            print(f"  - NG {r['feed_id']}: {err.get('type')}: {err.get('message')}")

if __name__ == "__main__":
    main()
