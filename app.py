"""
K-water 댐 운영현황 대시보드 서버
실행:  python app.py
접속:  http://localhost:5000
"""

from __future__ import annotations

import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any

import requests
from flask import Flask, jsonify, render_template
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

# ── API 설정 ───────────────────────────────────────────────────────
SERVICE_KEY = "aIue9pDdghRv/Cka1dZMXB5XndNX9Xq5Zx/GJxLzbj6x7pXeNVht6t4bzd1+gEm7kqb7xsa7SzUapHfNMehoTw=="

MULTI_URL = "https://apis.data.go.kr/B500001/dam/multipurPoseDam/multipurPoseDamlist"
WATER_URL = "https://apis.data.go.kr/B500001/dam/waterDam/waterDamlist"

CACHE_TTL       = 60
REQUEST_TIMEOUT = 20

_cache: dict[str, Any] = {"ts": 0.0, "data": None}


# ── HTTP 세션 ─────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://",  adapter)
    s.mount("https://", adapter)
    return s

session = make_session()


# ── XML 파싱 ──────────────────────────────────────────────────────
def parse_xml(text: str) -> list[dict]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    result_code = root.findtext(".//resultCode", "").strip()
    if result_code and result_code != "00":
        result_msg = root.findtext(".//resultMsg", "")
        raise RuntimeError(f"API 오류 코드 {result_code}: {result_msg}")

    items = []
    for item_el in root.findall(".//item"):
        row: dict[str, str] = {}
        for child in item_el:
            row[child.tag] = (child.text or "").strip()
        if row:
            items.append(row)
    return items


# ── JSON 파싱 ─────────────────────────────────────────────────────
def parse_json(payload: dict) -> list[dict]:
    body  = payload.get("response", {}).get("body", {})
    items = body.get("items", {})

    if isinstance(items, dict):
        item = items.get("item", [])
        if isinstance(item, dict):
            return [item]
        if isinstance(item, list):
            return item

    if isinstance(items, list):
        return items

    item = body.get("item", [])
    if isinstance(item, dict):
        return [item]
    if isinstance(item, list):
        return item

    if isinstance(payload, list):
        return payload

    return []


# ── 필드 추출 헬퍼 ────────────────────────────────────────────────
def pick(raw: dict, *keys: str, default: Any = None) -> Any:
    for k in keys:
        v = raw.get(k)
        if v not in (None, ""):
            return v
    return default


def to_float(v: Any) -> float:
    if v in (None, ""):
        return 0.0
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


# ── 필드명 표준화 ─────────────────────────────────────────────────
def map_item(raw: dict, dam_type: str) -> dict:
    return {
        "damName": pick(
            raw,
            "damName", "damnm", "dmobscnm", "name", "obsnm", "danm",
            default="-",
        ),
        "damWL": to_float(pick(
            raw, "damWL", "damwl", "wl", "waterLevel", "swl", "nowlowlevel",
        )),
        "abNormLV": to_float(pick(
            raw, "abNormLV", "abnormlv", "normalHighWaterLevel", "nhwl", "fwl", "nwl",
        )),
        "inflowQty": to_float(pick(
            raw, "inflowQty", "inflowqty", "inf", "inQty", "inflow", "inflowqy",
        )),
        "totalDrainQty": to_float(pick(
            raw, "totalDrainQty", "totaldrainqty", "otf", "outQty",
            "outflow", "totdcwtrqy", "total",
        )),
        "impoundQty": to_float(pick(
            raw, "impoundQty", "impoundqty", "storg", "storage",
            "stor", "nowrsvwtqy", "totrf",
        )),
        "limitQty": to_float(pick(
            raw, "limitQty", "limitqty", "planFloodQty", "effectiveStorage",
        )),
        "impoundRate": to_float(pick(
            raw, "impoundRate", "impoundrate", "rate", "storageRate",
            "rt", "rsvwtrt", "nowrsvwtqy2",
        )),
        "type": dam_type,
        "raw":  raw,
    }


# ── API 호출 ──────────────────────────────────────────────────────
def fetch_api(url: str, dam_type: str) -> list[dict]:
    today = datetime.now()
    vdate = today.strftime("%Y-%m-%d")
    tdate = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    ldate = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    vtime = today.strftime("%H")

    params = {
        "ServiceKey": SERVICE_KEY,
        "pageNo":     1,
        "numOfRows":  200,
        "_type":      "json",
        "vdate":      vdate,
        "tdate":      tdate,
        "ldate":      ldate,
        "vtime":      vtime,
    }

    resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")

    if "json" in content_type.lower():
        payload = resp.json()
        result_code = (
            payload.get("response", {})
                   .get("header", {})
                   .get("resultCode", "00")
        )
        if str(result_code) != "00":
            result_msg = (
                payload.get("response", {})
                       .get("header", {})
                       .get("resultMsg", "알 수 없는 오류")
            )
            raise RuntimeError(f"API 오류 [{result_code}]: {result_msg}")
        items = parse_json(payload)

    elif "xml" in content_type.lower():
        items = parse_xml(resp.text)

    else:
        try:
            items = parse_json(resp.json())
        except Exception:
            raise RuntimeError(f"알 수 없는 응답 형식: {content_type}")

    return [map_item(item, dam_type) for item in items]


# ── 라우트 ────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "kwater-dashboard"})


@app.route("/api/all")
def api_all():
    now = time.time()

    if _cache["data"] is not None and (now - _cache["ts"] < CACHE_TTL):
        return jsonify(_cache["data"])

    try:
        multi = fetch_api(MULTI_URL, "multi")
        water = fetch_api(WATER_URL, "water")

        result = {
            "ok":    True,
            "multi": multi,
            "water": water,
            "meta": {
                "cached_at":     int(now),
                "cache_ttl_sec": CACHE_TTL,
                "multi_count":   len(multi),
                "water_count":   len(water),
            },
        }

        _cache["ts"]   = now
        _cache["data"] = result
        return jsonify(result)

    except Exception as exc:
        return jsonify({
            "ok":    False,
            "error": str(exc),
            "multi": [],
            "water": [],
        }), 500


# ── 실행 ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
