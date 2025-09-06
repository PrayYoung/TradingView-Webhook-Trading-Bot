import os, time
from supabase import create_client
from orderapi import order
import logbot
from flask import Blueprint, request, jsonify

SUPABASE_URL = os.getenv("SUPABASE_URL")
# Prefer service role for worker; fallback to anon
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_API_KEY")
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

# --- V2: process single order_queue item by id ---
def process_one_by_id(queue_id: str):
    try:
        # Fetch the queued item
        res = supabase.table("order_queue").select("*").eq("id", queue_id).execute()
        items = res.data or []
        if not items:
            return {"success": False, "message": "not_found"}

        item = items[0]
        if item.get("status") not in ("ready", "processing"):
            return {"success": False, "message": f"invalid_status:{item.get('status')}"}

        # Mark processing
        supabase.table("order_queue").update({"status": "processing"}).eq("id", queue_id).execute()

        payload = item.get("raw") or {}
        logbot.logs(f"[Worker] 🚀 v2 processing id={queue_id} payload={payload}")
        result = order(payload)

        if result.get("success"):
            supabase.table("order_queue").update({"status": "done", "reason": None}).eq("id", queue_id).execute()
            return {"success": True, "message": "done"}
        else:
            reason = result.get("message", "order_failed")
            supabase.table("order_queue").update({"status": "failed", "reason": reason}).eq("id", queue_id).execute()
            return {"success": False, "message": reason}

    except Exception as e:
        logbot.logs(f"[Worker] ❌ v2 process_one_by_id error: {e}", True)
        try:
            supabase.table("order_queue").update({"status": "failed", "reason": str(e)}).eq("id", queue_id).execute()
        except Exception:
            pass
        return {"success": False, "message": str(e)}


# Blueprint to expose /worker/kick
worker_bp = Blueprint("worker", __name__)

@worker_bp.route("/worker/kick", methods=["POST"])
def worker_kick():
    token_required = os.getenv("WORKER_KICK_TOKEN", "")
    if token_required:
        hdr = request.headers.get("X-Worker-Token", "")
        if hdr != token_required:
            return jsonify({"success": False, "message": "unauthorized"}), 403

    try:
        body = request.get_json(force=True)
        qid = (body or {}).get("id")
        if not qid:
            return jsonify({"success": False, "message": "missing id"}), 400
        res = process_one_by_id(qid)
        code = 200 if res.get("success") else 400
        return jsonify(res), code
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

if __name__ == "__main__":
    logbot.logs("[Worker] 🟢 Started polling for orders...")
    while True:
        process()
