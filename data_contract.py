"""Public JSON contracts, compact homepage index, and validation helpers."""
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone


SCHEMA_VERSION = 1
PUBLIC_FILES = (
    "processed_etf_data.json",
    "stock_history_data.json",
    "homepage_index.json",
    "active_etf_ranking.json",
    "stock_price_cache.json",
)


def _atomic_write_json(path, data):
    fd, tmp = tempfile.mkstemp(prefix="tmp_", suffix=".json", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _normalise_date(value):
    return str(value or "").replace("/", "").replace("-", "")


def build_homepage_index(stock_data):
    """Build the small dataset needed before a user opens a stock detail."""
    changes_by_date, search = {}, []
    for code, stock in stock_data.items():
        holdings = stock.get("etf_holdings") or {}
        search.append({
            "code": code,
            "name": stock.get("name", ""),
            "holdings": {
                etf: {"current_count": info.get("current_count", 0)}
                for etf, info in holdings.items()
            },
        })
        for etf, holding in holdings.items():
            for record in holding.get("history") or []:
                date = _normalise_date(record.get("date"))
                if len(date) != 8:
                    continue
                item = {
                    "code": code, "name": stock.get("name", ""), "date": date,
                    "count": record.get("count"), "weight": record.get("weight"),
                    "count_change": record.get("count_change", record.get("countchange", 0)),
                    "weight_change": record.get("weight_change", 0), "status": record.get("status", ""),
                }
                # 每筆異動只保存一次；分類由前端在需要時依 status/count_change 計算。
                # 這能避免同一筆資料在 new/add/all 等欄位被重複序列化。
                day = changes_by_date.setdefault(date, {})
                day.setdefault(etf, []).append(item)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_data_date": max(changes_by_date, default=""),
        "all_dates": sorted(changes_by_date, reverse=True),
        "stock_search": sorted(search, key=lambda item: item["code"]),
        "changes_by_date": changes_by_date,
    }


def write_homepage_index(data_dir):
    with open(os.path.join(data_dir, "stock_history_data.json"), encoding="utf-8") as handle:
        stock_data = json.load(handle)
    index = build_homepage_index(stock_data)
    _atomic_write_json(os.path.join(data_dir, "homepage_index.json"), index)
    return index


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_data_manifest(data_dir, latest_data_date):
    files = {}
    for name in PUBLIC_FILES:
        path = os.path.join(data_dir, name)
        if not os.path.exists(path):
            continue
        files[name] = {"bytes": os.path.getsize(path), "sha256": _sha256(path)}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_data_date": latest_data_date,
        "files": files,
    }
    _atomic_write_json(os.path.join(data_dir, "data_manifest.json"), manifest)
    return manifest


def validate_public_data(data_dir):
    """Raise ValueError before publishing malformed or inconsistent public files."""
    required = ("processed_etf_data.json", "stock_history_data.json", "homepage_index.json", "data_manifest.json")
    missing = [name for name in required if not os.path.exists(os.path.join(data_dir, name))]
    if missing:
        raise ValueError("missing public files: " + ", ".join(missing))
    with open(os.path.join(data_dir, "processed_etf_data.json"), encoding="utf-8") as handle:
        processed = json.load(handle)
    with open(os.path.join(data_dir, "stock_history_data.json"), encoding="utf-8") as handle:
        history = json.load(handle)
    with open(os.path.join(data_dir, "homepage_index.json"), encoding="utf-8") as handle:
        index = json.load(handle)
    with open(os.path.join(data_dir, "data_manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(processed, dict) or not processed:
        raise ValueError("processed_etf_data.json must be a non-empty object")
    if not isinstance(history, dict) or not history:
        raise ValueError("stock_history_data.json must be a non-empty object")
    if index.get("schema_version") != SCHEMA_VERSION or manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported public data schema version")
    if index.get("latest_data_date") != manifest.get("latest_data_date"):
        raise ValueError("homepage index and manifest latest dates differ")
    for item in index.get("stock_search", []):
        if item.get("code") not in history:
            raise ValueError("homepage index references an unknown stock")
    for name, details in manifest.get("files", {}).items():
        path = os.path.join(data_dir, name)
        if not os.path.exists(path) or details.get("sha256") != _sha256(path):
            raise ValueError("manifest checksum mismatch: " + name)
    return True
