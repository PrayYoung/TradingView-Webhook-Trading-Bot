import os, json, math, time
from datetime import datetime, timezone
from flask import request, jsonify
from supabase import create_client
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_PASSPHRASE_V2 = os.environ.get("WEBHOOK_PASSPHRASE_V2", "duguai-v2")
HEADER_TOKEN_V2 = os.environ.get("WEBHOOK_HEADER_TOKEN_V2", "")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_KEY = os.environ["ALPACA_KEY_ID"]
ALPACA_SECRET = os.environ["ALPACA_SECRET_KEY"]

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json"
    }

def _alpaca_equity():
    r = requests.get(f"{ALPACA_BASE_URL}/v2/account", headers=_alpaca_headers(), timeout=10)
    r.raise_for_status()
    return float(r.json()["equity"])

def _place_bracket(symbol, side, qty, tp, sl, tif="day"):
    payload = {
        "symbol": symbol, "side": side, "type": "market",
        "qty": str(int(qty)) if float(qty).is_integer() else str(qty),
        "time_in_force": tif, "order_class": "bracket",
        "take_profit": {"limit_price": tp}, "stop_loss": {"stop_price": sl}
    }
    r = requests.post(f"{ALPACA_BASE_URL}/v2/orders", headers=_alpaca_headers(),
                      data=json.dumps(payload), timeout=10)
    ok = r.status_code in (200, 201)
    return ok, r.json()

def _account_enabled():
    d = sb.table("account_state").select("*").eq("id", 1).execute().data
    return bool(d and d[0]["trading_enabled"])

def _load_strategy(name):
    d = sb.table("strategies").select("*").eq("name", name).execute().data
    if not d:
        # 没记录 → 默认暂停，确保安全
        return {"name": name, "status": "paused", "default_risk_pct": 0.005,
                "trail_atr_mult": 2.5, "r_multiple_tp": 2.0,
                "max_positions": 5, "allow_short": False, "time_in_force": "day"}
    return d[0]

def _dedup_exists(key):
    d = sb.table("signals_raw").select("id").eq("dedup_key", key).execute().data
    return bool(d)

def _insert_signal_raw(p, key):
    sb.table("signals_raw").insert({
        "strategy": p["strategy"], "ticker": p["ticker"], "timeframe": str(p.get("timeframe","")),
        "action": p["action"], "price": p.get("price"), "atr": p.get("atr"),
        "risk_pct": p.get("risk_pct"), "trail_atr_mult": p.get("trail_atr_mult"),
        "bar_time": datetime.fromtimestamp(p["bar_time"]/1000, tz=timezone.utc),
        "dedup_key": key, "source": "tv-v2", "raw": p
    }).execute()

def tv_webhook_v2():
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "[v2] invalid json"}), 400

    if not data or data.get("passphrase") != WEBHOOK_PASSPHRASE_V2:
        return jsonify({"error": "[v2] bad passphrase"}), 401
    if HEADER_TOKEN_V2 and request.headers.get("X-Auth","") != HEADER_TOKEN_V2:
        return jsonify({"error": "[v2] bad header token"}), 401

    for k in ("strategy","ticker","timeframe","action","bar_time"):
        if k not in data:
            return jsonify({"error": f"[v2] missing {k}"}), 400

    dedup_key = f'{data["strategy"]}|{data["ticker"]}|{data["timeframe"]}|{data["bar_time"]}|{data["action"]}'
    if _dedup_exists(dedup_key):
        return jsonify({"status":"[v2] dup_ignored","dedup_key":dedup_key}), 200
    _insert_signal_raw(data, dedup_key)

    # 全局与策略状态
    if not _account_enabled():
        return jsonify({"status":"[v2] trading_disabled"}), 200
    st = _load_strategy(data["strategy"])
    if st["status"] != "active":
        return jsonify({"status":"[v2] strategy_paused"}), 200

    price = float(data.get("price") or 0)
    atr   = float(data.get("atr") or 0)
    if price <= 0 or atr <= 0:
        return jsonify({"error":"[v2] bad price/atr"}), 400

    action = data["action"].lower()
    if action not in ("buy","sell"):
        return jsonify({"status":"[v2] ignored_action"}), 200
    if action=="sell" and not st["allow_short"]:
        return jsonify({"status":"[v2] short_not_allowed"}), 200

    trail_mult = float(data.get("trail_atr_mult") or st["trail_atr_mult"])
    risk_pct   = float(data.get("risk_pct") or st["default_risk_pct"])
    r_tp       = float(st["r_multiple_tp"])

    if action=="buy":
        sl = round(price - trail_mult*atr, 4)
        risk_per = max(price - sl, 0.01)
        side = "buy"
    else:
        sl = round(price + trail_mult*atr, 4)
        risk_per = max(sl - price, 0.01)
        side = "sell"

    equity = _alpaca_equity()
    qty = math.floor(max(equity*risk_pct,1.0) / risk_per)
    if qty < 1:
        return jsonify({"status":"[v2] qty_too_small","qty":qty}), 200

    tp = round(price + (price-sl)*r_tp if side=="buy" else price - (sl-price)*r_tp, 4)

    ok, resp = _place_bracket(data["ticker"], side, qty, tp, sl, st["time_in_force"])
    sb.table("orders").insert({
        "ticker": data["ticker"], "side": side, "type":"bracket",
        "qty": qty, "take_profit_px": tp, "stop_px": sl,
        "status": "submitted" if ok else "failed",
        "reason": "" if ok else resp.get("message","alpaca_error"),
        "raw_resp": resp
    }).execute()

    if ok:
        sb.table("positions").insert({
            "ticker": data["ticker"], "side": "long" if side=="buy" else "short",
            "qty": qty, "avg_entry": price, "stop_px": sl, "take_profit_px": tp,
            "strategy": data["strategy"], "status":"open"
        }).execute()

    return jsonify({"ok":ok,"qty":qty,"tp":tp,"sl":sl,"v":"v2"}), (200 if ok else 500)
