import os, time
from supabase import create_client
from orderapi import order
import logbot

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

if __name__ == "__main__":
    logbot.logs("[Worker] 🟢 Started polling for orders...")
    while True:
        process()
