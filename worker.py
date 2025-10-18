import os, time, datetime
from datetime import timezone
from datetime import time as dtime
from supabase import create_client
from orderapi import order
import logbot
from flask import Blueprint, request, jsonify
import requests
from config import (
    load_account_state,
    update_account_state,
    resolve_alpaca_for_alias,
    get_or_set_day_open_equity,
    get_equity_cached,
)

_last_report_key = None  # 全局变量：记录已发送日期

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Need SUPABASE_URL and SUPABASE_API_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- V1 polling loop (unchanged) ---
def process():
    try:
        result = supabase.table("webhook_queue").select("*").eq("status", "pending").execute()
        pending_orders = result.data

        if not pending_orders:
            time.sleep(10)
            return

        for row in pending_orders:
            row_id = row["id"]
            payload = row["data"]
            if isinstance(payload, dict):
                action_v1 = (payload.get("action") or "").upper()
                if action_v1 == "SELL" and "percentage" in payload:
                    payload = dict(payload)
                    payload.pop("percentage", None)
            logbot.logs(f"[Worker] 🚀 Executing order: {payload}")

            try:
                # Step 1: Mark as processing
                supabase.table("webhook_queue").update({"status": "processing"}).eq("id", row_id).execute()

                # Step 2: Place the order
                result = order(payload)

                # Step 3: Mark appropriately based on result
                if isinstance(result, dict) and result.get("success"):
                    supabase.table("webhook_queue").update({"status": "processed"}).eq("id", row_id).execute()
                    logbot.logs(f"[Worker] ✅ Order placed and marked processed")
                else:
                    msg = (result or {}).get("message") if isinstance(result, dict) else None
                    logbot.logs(f"[Worker] ❌ Order failed result; marking error. {msg}", True)
                    supabase.table("webhook_queue").update({"status": "error"}).eq("id", row_id).execute()

            except Exception as e:
                logbot.logs(f"[Worker] ❌ Order error: {e}", True)
                # Optional: revert to 'pending' or mark as 'error'
                supabase.table("webhook_queue").update({"status": "error"}).eq("id", row_id).execute()

    except Exception as e:
        logbot.logs(f"[Worker] ❌ Supabase poll error: {e}", True)
        time.sleep(10)

# --- Helpers for V2 queue ---
def _now_utc():
    return datetime.datetime.now(tz=timezone.utc)

def _normalize_base_url(raw: str) -> str:
    b = (raw or "").strip()
    if b.endswith("/v2"):
        b = b[:-3]
    return b.rstrip("/")

def _is_market_open(ts: datetime.datetime) -> bool:
    # US RTH: Mon–Fri 13:30–20:00 UTC
    dow = ts.weekday()  # 0=Mon .. 6=Sun
    if dow > 4:
        return False
    t = ts.time()
    start = datetime.time(13, 30)
    end = datetime.time(20, 0)
    return (t >= start) and (t <= end)

def _parse_reset_time(cfg: dict) -> datetime.time:
    v = (cfg or {}).get("reset_time_utc") or "00:05:00"
    try:
        hh, mm, ss = str(v).split(":")
        return datetime.time(int(hh), int(mm), int(ss))
    except Exception:
        return datetime.time(0, 5, 0)

def _is_after_reset_window(now_utc: datetime.datetime, cfg: dict) -> bool:
    return now_utc.time() >= _parse_reset_time(cfg)

def _count_open_positions(alias: str) -> int:
    try:
        key, sec, base, _paper = resolve_alpaca_for_alias(alias)
        r = requests.get(
            f"{base}/v2/positions",
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec},
            timeout=5,
        )
        if r.status_code == 404:
            return 0
        r.raise_for_status()
        arr = r.json() or []
        return len(arr)
    except Exception as e:
        logbot.logs(f"[Risk] positions fetch failed for {alias}: {e}", True)
        return 0

def _ensure_day_open_equity(sb, alias: str, cfg: dict, now_utc: datetime.datetime):
    cur = get_or_set_day_open_equity(sb, alias, now_utc)
    if cur is None and _is_after_reset_window(now_utc, cfg):
        try:
            eq = get_equity_cached(alias)
            dkey = now_utc.strftime("%Y-%m-%d")
            sb.table("daily_metrics").update({"equity": eq}).eq("d", dkey).eq("alias", alias).execute()
            logbot.logs(f"[Risk] day_open_equity set for {alias} d={dkey} eq={eq}")
        except Exception as e:
            logbot.logs(f"[Risk] set day_open_equity failed: {e}", True)

def risk_guard(sb, alias: str) -> None:
    if (os.getenv("RISK_GUARD_DISABLED", "0").strip().lower() in ("1", "true", "yes")):
        return
    cfg = load_account_state(sb) or {}
    if not cfg:
        return
    if not cfg.get("trading_enabled", True):
        raise Exception("trading_disabled")

    now = _now_utc()
    _ensure_day_open_equity(sb, alias, cfg, now)

    # Equity + HWM
    eq = get_equity_cached(alias)
    hwm = cfg.get("daily_high_watermark") or eq
    if eq > float(hwm or 0):
        update_account_state(sb, daily_high_watermark=eq)
        hwm = eq

    # Daily drawdown breaker
    dd_limit = cfg.get("daily_dd_limit_pct")
    if dd_limit is not None and (float(hwm or 0) > 0):
        dd = (float(hwm) - float(eq)) / float(hwm)
        if dd >= float(dd_limit):
            update_account_state(sb, trading_enabled=False, daily_dd_triggered=True, pause_reason='daily_dd')
            raise Exception("daily_drawdown_limit_reached")

    # Absolute daily loss cap
    dkey = now.strftime("%Y-%m-%d")
    try:
        row = sb.table("daily_metrics").select("equity").eq("d", dkey).eq("alias", alias).execute().data
        day_open = float(row[0]["equity"]) if row and row[0].get("equity") is not None else None
    except Exception:
        day_open = None
    loss_cap = cfg.get("daily_loss_cap_usd")
    if (loss_cap is not None) and (day_open is not None):
        if (float(eq) - float(day_open)) <= -float(loss_cap):
            update_account_state(sb, trading_enabled=False, daily_dd_triggered=True, pause_reason='daily_loss_cap')
            raise Exception("daily_loss_cap_reached")

    # Max concurrent positions (account-level)
    max_total = cfg.get("max_positions_total")
    if max_total is not None:
        cur_positions = _count_open_positions(alias)
        if int(cur_positions) >= int(max_total):
            raise Exception("max_positions_total_reached")

def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_bool(x, default=False):
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        t = x.strip().lower()
        if t in ("1", "true", "yes", "y", "on"):
            return True
        if t in ("0", "false", "no", "n", "off"):
            return False
    return default


def _safe_str(x, default=""):
    if x is None:
        return default
    try:
        return str(x)
    except Exception:
        return default

def claim_task(queue_id: str) -> bool:
    try:
        res = (
            supabase.table("order_queue")
            .update({"status": "processing"})
            .eq("id", queue_id)
            .eq("status", "ready")
            .execute()
        )
        rows = res.data or []
        return len(rows) > 0
    except Exception as e:
        logbot.logs(f"[Worker] ❌ claim_task error for {queue_id}: {e}", True)
        return False

# --- V2: process single order_queue item by id ---
def process_one_by_id(queue_id: str):
    try:
        # Try to atomically claim the task
        if not claim_task(queue_id):
            return {"success": False, "message": "already_taken"}

        # Load the queued item
        res = supabase.table("order_queue").select("*").eq("id", queue_id).execute()
        items = res.data or []
        if not items:
            return {"success": False, "message": "not_found"}

        item = items[0]
        if item.get("status") != "processing":
            return {"success": False, "message": f"invalid_status:{item.get('status')}"}

        # If next_attempt_at is set in the future, release back to ready
        naa = item.get("next_attempt_at")
        if naa:
            try:
                naa_dt = datetime.datetime.fromisoformat(naa.replace("Z", "+00:00"))
                if naa_dt > _now_utc():
                    supabase.table("order_queue").update({"status": "ready"}).eq("id", queue_id).execute()
                    return {"success": False, "message": "deferred"}
            except Exception:
                pass

        # -------- Build order payload (compute tp/sl if possible) --------
        payload = item.get("raw") or {}
        flat_exit = _safe_bool(payload.get("flat_exit"), default=True)
        after_hours_mode = _safe_str(payload.get("after_hours_mode"), default="").strip().lower()
        # Build idempotent client_order_id from queue_id
        qid_compact = queue_id.replace("-", "")
        client_order_id = ("q_" + qid_compact)[:30]

        # Trading mode + base url guard（基于默认 base；若使用多账户，orderapi 内部会按 alias 取具体账户）
        trading_mode = (os.getenv("TRADING_MODE", "paper") or "paper").strip().lower()
        alias_for_mode = item.get("subaccount", "default")
        try:
            _k, _s, alias_base, _paper = resolve_alpaca_for_alias(alias_for_mode)
            base_env = _normalize_base_url(alias_base)
        except Exception:
            base_env = _normalize_base_url(os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"))
        if trading_mode == "paper" and "paper-api.alpaca.markets" not in base_env:
            raise Exception("mode_mismatch: paper expected")
        if trading_mode == "live" and "paper-api.alpaca.markets" in base_env:
            raise Exception("mode_mismatch: live expected")

        # Market hours guard (skip for crypto symbols)
        def _is_crypto_symbol(raw: str) -> bool:
            s = (raw or "").strip().upper()
            return ("/" in s) or s.endswith("USD") or s.endswith("USDT")

        ticker_raw = item.get("ticker")
        market_open_now = _is_market_open(_now_utc())
        if not _is_crypto_symbol(ticker_raw) and not market_open_now:
            if after_hours_mode in ("allow", "opg", "market", "mkt", "opg_market"):
                logbot.logs(
                    f"[Worker] 🌙 after_hours_mode={after_hours_mode or 'allow'} bypassing market hours for {ticker_raw}"
                )
            else:
                raise Exception("market_closed")

        # Risk guard (blocks new entries only; no auto-flatten here)
        try:
            risk_guard(supabase, item.get("subaccount", "default"))
        except Exception as rg:
            supabase.table("order_queue").update({
                "status": "failed",
                "reason": str(rg)
            }).eq("id", queue_id).execute()
            return {"success": False, "message": str(rg)}

        # 从队列字段推导 bracket 价位（若 price/atr/trail_atr_mult 齐备）
        action = (item.get("action") or "").upper()
        entry = _safe_float(item.get("price"))
        atr = _safe_float(item.get("atr"))
        trail_k = _safe_float(item.get("trail_atr_mult"))
        r_mult = _safe_float(item.get("r_multiple_tp"), 2.0)  # 没有就用 2R

        tp = sl = None
        if entry is not None and atr is not None and trail_k is not None:
            if action == "BUY":
                sl = entry - atr * trail_k
                risk = max(entry - sl, 0.01)
                tp = entry + r_mult * risk
            elif action == "SELL":
                sl = entry + atr * trail_k
                risk = max(sl - entry, 0.01)
                tp = entry - r_mult * risk

        # 把 risk_pct 作为资金百分比映射给 orderapi 的 percentage（最小改动）
        pct = _safe_float(item.get("risk_pct"))
        if action == "SELL" and flat_exit:
            pct = None
        # 组装下单 payload
        payload_out = {
            "ticker": item.get("ticker"),
            "action": action,
            "strategy": item.get("strategy"),
            "subaccount": item.get("subaccount", "default"),
            "client_order_id": client_order_id,
            "order_type": "market",
            # Use GTC for crypto (Alpaca crypto doesn't support DAY); DAY for equities
            "time_in_force": ("gtc" if _is_crypto_symbol(item.get("ticker")) else "day"),
        }
        if after_hours_mode in ("opg", "opg_market") and not _is_crypto_symbol(item.get("ticker")):
            payload_out["time_in_force"] = "opg"
            payload_out["order_type"] = "market"
        elif after_hours_mode in ("allow", "market", "mkt") and not market_open_now:
            payload_out["time_in_force"] = "day"
        if pct is not None:
            payload_out["percentage"] = pct
        if action == "SELL" and flat_exit:
            logbot.logs(f"[Worker] 🧹 flat_exit=True forcing full unload for {item.get('ticker')}")
        tp_sl_attached = False
        if item.get("max_slots") is not None:
            payload_out["max_slots"] = item.get("max_slots")
        if item.get("buffer_ratio") is not None:
            payload_out["buffer_ratio"] = item.get("buffer_ratio")
        if action == "BUY" and tp is not None and sl is not None:
            payload_out["tp"] = tp
            payload_out["sl"] = sl
            tp_sl_attached = True

        logbot.logs(
            f"[Worker] 🚀 v2 processing id={queue_id} clid={client_order_id} "
            f"{payload_out.get('strategy')} {payload_out.get('ticker')} {payload_out.get('subaccount')} "
            f"tp/sl={'on' if tp_sl_attached else 'off'}"
        )

        # ---------- Place order ----------
        result = order(payload_out)

        if result.get("success"):
            supabase.table("order_queue").update({"status": "done", "reason": ""}).eq("id", queue_id).execute()
            return {"success": True, "message": "done"}
        else:
            raise Exception(result.get("message", "order_failed"))

    except Exception as e:
        logbot.logs(f"[Worker] ❌ v2 process_one_by_id error: {e}", True)
        # If market closed, mark failed immediately (no retry)
        try:
            msg = str(e)
            if "market_closed" in msg:
                supabase.table("order_queue").update({
                    "status": "failed",
                    "reason": "market_closed",
                    "last_error": msg
                }).eq("id", queue_id).execute()
            else:
                # Retry with backoff (<=3) else DLQ
                row = supabase.table("order_queue").select("*").eq("id", queue_id).execute().data[0]
                rc = int(row.get("retry_count") or 0) + 1
                if rc <= 3:
                    supabase.table("order_queue").update({
                        "status": "ready",
                        "retry_count": rc,
                        "last_error": msg,
                        "next_attempt_at": (_now_utc() + datetime.timedelta(seconds=30)).isoformat()
                    }).eq("id", queue_id).execute()
                else:
                    body = dict(row)
                    body.update({"status": "failed", "last_error": msg})
                    supabase.table("order_queue_dlq").insert(body).execute()
                    supabase.table("order_queue").update({
                        "status": "failed",
                        "reason": msg
                    }).eq("id", queue_id).execute()
        except Exception as e2:
            logbot.logs(f"[Worker] ❌ backoff/DLQ error: {e2}", True)
        return {"success": False, "message": str(e)}


# Blueprint to expose /worker/kick
worker_bp = Blueprint("worker", __name__)

@worker_bp.route("/worker/kick", methods=["POST"])
def worker_kick():
    token_required = os.getenv("WORKER_SECRET", "")
    hdr = request.headers.get("X-Worker-Token", "")
    if not token_required or hdr != token_required:
        return jsonify({"success": False, "message": "unauthorized"}), 401

    try:
        body = request.get_json(force=True)
        qid = (body or {}).get("id")
        if not qid:
            return jsonify({"success": False, "message": "missing id"}), 400
        res = process_one_by_id(qid)
        code = 200 if res.get("success") else 500
        return jsonify(res), code
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


def _should_run_daily_report(now_utc):
    # 设定触发时间（收盘后 20:10 UTC）
    target = dtime(20, 10)
    return (now_utc.time() >= target)

def _try_run_daily_report_once_per_day():
    global _last_report_key
    now = datetime.datetime.now(timezone.utc)
    key = now.strftime("%Y-%m-%d")
    if _last_report_key == key:
        return
    if _should_run_daily_report(now):
        try:
            # 直接复用你 scripts/daily_report.py 里的 main()，或提取成可导入函数
            from daily_report import main as run_report
            run_report()
            logbot.logs("[Report] ✅ daily report sent")
            _last_report_key = key
        except Exception as e:
            logbot.logs(f"[Report] ❌ daily report error: {e}", True)

if __name__ == "__main__":
    logbot.logs("[Worker] 🟢 Started polling for orders...")
    while True:
        process()
        # Also pick up ready v2 tasks
        try:
            res = (
                supabase.table("order_queue")
                .select("id,status,next_attempt_at")
                .eq("status", "ready")
                .limit(20)
                .execute()
            )
            for row in (res.data or []):
                naa = row.get("next_attempt_at")
                if naa:
                    try:
                        naa_dt = datetime.datetime.fromisoformat(naa.replace("Z", "+00:00"))
                        if naa_dt > _now_utc():
                            continue
                    except Exception:
                        pass
                process_one_by_id(row["id"])
        except Exception as e:
            logbot.logs(f"[Worker] ❌ v2 poll error: {e}", True)
        # 每天只运行一次的报表（可选开关）
        if (os.getenv("ENABLE_DAILY_REPORT", "0").strip().lower() in ("1", "true", "yes")):
            _try_run_daily_report_once_per_day()
        time.sleep(2)
