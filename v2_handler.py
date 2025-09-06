import os, json
from datetime import datetime, timezone
from flask import request, jsonify
from supabase import create_client
import requests

# --- Env & Clients ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
WEBHOOK_PASSPHRASE_V2 = os.environ.get("WEBHOOK_PASSPHRASE_V2", None)
HEADER_TOKEN_V2 = os.environ.get("WEBHOOK_HEADER_TOKEN_V2", "")
WORKER_URL = os.environ.get("WORKER_URL", "")
WORKER_KICK_TOKEN = os.environ.get("WORKER_KICK_TOKEN", "")

if not WEBHOOK_PASSPHRASE_V2 or len(WEBHOOK_PASSPHRASE_V2) < 16:
    raise RuntimeError("WEBHOOK_PASSPHRASE_V2 must be set and >=16 chars")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set for v2")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def _dedup_exists(key: str) -> bool:
    d = sb.table("signals_raw").select("id").eq("dedup_key", key).execute().data
    return bool(d)


def _insert_signal_raw(p: dict, key: str):
    sb.table("signals_raw").insert({
        "strategy": p["strategy"],
        "ticker": p["ticker"],
        "timeframe": str(p.get("timeframe", "")),
        "action": p["action"],
        "price": p.get("price"),
        "atr": p.get("atr"),
        "risk_pct": p.get("risk_pct"),
        "trail_atr_mult": p.get("trail_atr_mult"),
        "bar_time": datetime.fromtimestamp(int(p["bar_time"]) / 1000, tz=timezone.utc),
        "dedup_key": key,
        "source": "tv-v2",
        "raw": p,
    }).execute()


def _account_enabled() -> bool:
    d = sb.table("account_state").select("trading_enabled").eq("id", 1).execute().data
    return bool(d and d[0].get("trading_enabled"))


def _strategy_active(name: str):
    d = sb.table("strategies").select("name,status").eq("name", name).execute().data
    if not d:
        return False, None
    return d[0].get("status") == "active", d[0]


def _enqueue_order(data: dict) -> str:
    resp = sb.table("order_queue").insert({
        "status": "ready",
        "reason": None,
        "strategy": data["strategy"],
        "ticker": data["ticker"],
        "timeframe": str(data.get("timeframe", "")),
        "action": data["action"],
        "price": data.get("price"),
        "atr": data.get("atr"),
        "risk_pct": data.get("risk_pct"),
        "trail_atr_mult": data.get("trail_atr_mult"),
        "bar_time": datetime.fromtimestamp(int(data["bar_time"]) / 1000, tz=timezone.utc),
        "subaccount": data.get("subaccount", "default"),
        "raw": data,
    }).execute()
    return resp.data[0]["id"]


def _kick_worker(queue_id: str):
    if not WORKER_URL or not WORKER_KICK_TOKEN:
        return
    try:
        requests.post(
            f"{WORKER_URL.rstrip('/')}/worker/kick",
            json={"id": queue_id},
            headers={"X-Worker-Token": WORKER_KICK_TOKEN},
            timeout=1.5,
        )
    except Exception:
        # Best-effort fire-and-forget
        pass


def tv_webhook_v2():
    # Validate JSON
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"error": "[v2] invalid json"}), 400

    # Passphrase (strict)
    if not data or data.get("passphrase") != WEBHOOK_PASSPHRASE_V2:
        return jsonify({"error": "[v2] bad passphrase"}), 401

    # Optional header token
    if HEADER_TOKEN_V2:
        header_val = request.headers.get("X-Auth") or request.headers.get("X-Webhook-Token")
        if header_val != HEADER_TOKEN_V2:
            return jsonify({"error": "[v2] bad header token"}), 401

    # Required fields
    for k in ("strategy", "ticker", "timeframe", "action", "bar_time"):
        if k not in data:
            return jsonify({"error": f"[v2] missing {k}"}), 400

    # Default subaccount
    if "subaccount" not in data:
        data["subaccount"] = "default"

    # Dedup
    dedup_key = f"{data['strategy']}|{data['ticker']}|{data['timeframe']}|{data['bar_time']}|{data['action']}"
    if _dedup_exists(dedup_key):
        return jsonify({"status": "[v2] dup_ignored", "dedup_key": dedup_key}), 200

    # Always insert into signals_raw
    _insert_signal_raw(data, dedup_key)

    # Check global/state
    if not _account_enabled():
        return jsonify({"status": "[v2] trading_disabled"}), 200
    active, _st = _strategy_active(data["strategy"])
    if not active:
        return jsonify({"status": "[v2] strategy_paused"}), 200

    # Enqueue for worker (QUEUE mode only; do not place orders here)
    qid = _enqueue_order(data)
    _kick_worker(qid)
    return jsonify({"status": "[v2] queued", "id": qid}), 200
