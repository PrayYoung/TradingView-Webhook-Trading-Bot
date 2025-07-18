import os
import alpaca_trade_api as tradeapi
import logbot

# 从环境变量获取 API 凭证
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')


def order(payload):
    symbol = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    qty = payload.get("qty", 1)  # 默认买入1股
    price = payload.get("price", None)
    strategy = payload.get("strategy", "")
    subaccount = payload.get("subaccount", "default")

    logbot.logs(f"📩 Incoming Order:\nSubaccount: {subaccount}\nStrategy: {strategy}\n{action} {qty} x {symbol} @ {price}")

    try:
        if action == "BUY":
            api.submit_order(
                symbol=symbol,
                qty=qty,
                side='buy',
                type='market',
                time_in_force='gtc'
            )
            return {"success": True, "message": f"✅ Bought {qty} x {symbol}"}

        elif action == "SELL":
            api.submit_order(
                symbol=symbol,
                qty=qty,
                side='sell',
                type='market',
                time_in_force='gtc'
            )
            return {"success": True, "message": f"✅ Sold {qty} x {symbol}"}

        else:
            return {"success": False, "message": f"❌ Unknown action: {action}"}

    except Exception as e:
        logbot.logs(f"🚫 Order failed: {e}", True)
        return {"success": False, "message": f"Error: {e}"}
