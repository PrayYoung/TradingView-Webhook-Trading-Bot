import os, time, datetime
from datetime import timezone
from supabase import create_client
from orderapi import order
import logbot
from flask import Blueprint, request, jsonify

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
            logbot.logs(f"[Worker] 🚀 Executing order: {payload}")

            try:
                # Step 1: Mark as processing
                supabase.table("webhook_queue").update({"status": "processing"}).eq("id", row_id).execute()

                # Step 2: Place the order
                result = order(payload)

                # Step 3: Mark as processed
                supabase.table("webhook_queue").update({"status": "processed"}).eq("id", row_id).execute()
                logbot.logs(f"[Worker] ✅ Order placed and marked processed")

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

        payload = item.get("raw") or {}
        # Build idempotent client_order_id from queue_id
        qid_compact = queue_id.replace("-", "")
        client_order_id = ("q_" + qid_compact)[:30]

        # Trading mode + base url guard
        trading_mode = (os.getenv("TRADING_MODE", "paper") or "paper").strip().lower()
        base_env = _normalize_base_url(os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"))
        if trading_mode == "paper" and "paper-api.alpaca.markets" not in base_env:
            raise Exception("mode_mismatch: paper expected")
        if trading_mode == "live" and "paper-api.alpaca.markets" in base_env:
            raise Exception("mode_mismatch: live expected")

        # Market hours guard
        if not _is_market_open(_now_utc()):
            raise Exception("market_closed")

        # Call canonical order entry; pass through client_order_id
        payload_out = {
            "ticker": item.get("ticker"),
            "action": (item.get("action") or "").upper(),
            "strategy": item.get("strategy"),
            "subaccount": item.get("subaccount", "default"),
            "client_order_id": client_order_id,
        }
        logbot.logs(
            f"[Worker] 🚀 v2 processing id={queue_id} clid={client_order_id} "
            f"{payload_out.get('strategy')} {payload_out.get('ticker')} {payload_out.get('subaccount')}"
        )
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
        time.sleep(2)
