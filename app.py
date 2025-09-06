import logbot
import json, os, config
from flask import Flask, request
from orderapi import order
from supabase import create_client, Client
from v2_handler import tv_webhook_v2


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
WORKER_SECRET = os.getenv("WORKER_SECRET", "defaultsecret")

app = Flask(__name__)

def enqueue_to_supabase(data):
    try:
        response = supabase.table("webhook_queue").insert({
            "data": data,
            "status": "pending"
        }).execute()
        logbot.logs("✅ Alert queued to Supabase")
        return {"success": True, "message": "Order queued", "id": response.data[0]["id"]}
    except Exception as e:
        logbot.logs(f">>> /!\\ Supabase insert error: {e}", True)
        return {"success": False, "message": "Supabase insert error"}

@app.route("/")
def hello_trader():
    return "<p>Hello young trader!</p>"

@app.route("/tradingview-to-webhook-order", methods=['POST'])
def tradingview_webhook():
    logbot.logs("========= STRATEGY =========")
    
    # Step 1: 解析数据
    try:
        data = json.loads(request.data)
    except Exception as e:
        logbot.logs(f">>> /!\\ JSON decode error: {e}", True)
        return {"success": False, "message": "Invalid JSON format"}

    # Step 2: passphrase 验证
    webhook_passphrase = os.environ.get('WEBHOOK_PASSPHRASE', config.WEBHOOK_PASSPHRASE)

    if 'passphrase' not in data or data['passphrase'] != webhook_passphrase:
        logbot.logs(">>> /!\\ Invalid or missing passphrase", True)
        return {"success": False, "message": "Invalid passphrase"}

    # Step 3: 自动补充 subaccount（如果没写）
    if 'subaccount' not in data:
        data['subaccount'] = 'default'

    # Step 4: 基础字段校验
    required_fields = ['ticker', 'action']
    missing = [f for f in required_fields if f not in data]
    if missing:
        logbot.logs(f">>> /!\\ Missing fields: {missing}", True)
        return {"success": False, "message": f"Missing fields: {', '.join(missing)}"}

    # Step 5: 校验 action
    if data['action'].upper() not in ['BUY', 'SELL']:
        logbot.logs(f">>> /!\\ Invalid action: {data['action']}", True)
        return {"success": False, "message": "Action must be BUY or SELL"}

    # # Step 6: 下单
    # orders = order(data)
    # print("✅ Order response:", orders)
    # logbot.logs(f"✅ Order response: {orders}")
    # return orders

    # # Step 6, 写进database
    return enqueue_to_supabase(data)


@app.route("/tradingview-to-discord-study", methods=['POST'])
def discord_study_tv():
    logbot.logs("========== STUDY ==========")

    try:
        data = json.loads(request.data)
    except Exception as e:
        logbot.logs(f">>> /!\\ JSON decode error: {e}", True)
        return {"success": False, "message": "Invalid JSON format"}

    webhook_passphrase = os.environ.get('WEBHOOK_PASSPHRASE', config.WEBHOOK_PASSPHRASE)

    if 'passphrase' not in data or data['passphrase'] != webhook_passphrase:
        logbot.logs(">>> /!\\ Invalid or missing passphrase", True)
        return {"success": False, "message": "Invalid passphrase"}

    del data["passphrase"]
    chart_url = data.pop("chart_url", None)
    if not chart_url:
        logbot.logs(">>> /!\\ Key 'chart_url' not found", True)

    logbot.study_alert(json.dumps(data), chart_url)
    return {"success": True}


@app.route("/run-worker", methods=["GET"])
def run_worker():
    key = request.args.get("key")
    if key != WORKER_SECRET:
        return {"success": False, "message": "Unauthorized"}, 403

    try:
        result = supabase.table("webhook_queue").select("*").eq("status", "pending").execute()
        for row in result.data:
            payload = row["data"]
            logbot.logs(f"[Worker] Executing order: {payload}")
            try:
                result = order(payload)
                supabase.table("webhook_queue").update({"status": "processed"}).eq("id", row["id"]).execute()
                logbot.logs(f"[Worker] ✅ Order placed and marked processed")
            except Exception as e:
                logbot.logs(f"[Worker] ❌ Order error: {e}", True)
                supabase.table("webhook_queue").update({"status": "error"}).eq("id", row["id"]).execute()
        return {"success": True, "message": "Worker run complete"}
    except Exception as e:
        logbot.logs(f"[Worker] ❌ Supabase poll error: {e}", True)
        return {"success": False, "message": "Worker failed"}


@app.route("/v2/tradingview-to-webhook-order", methods=["POST"])
def v2_entry():
    return tv_webhook_v2()
