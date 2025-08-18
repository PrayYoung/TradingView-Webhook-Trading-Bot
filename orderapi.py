import os
import alpaca_trade_api as tradeapi
import logbot

# 环境变量中获取API密钥
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')

def get_latest_price(symbol):
    """获取实时价格，支持股票和加密货币"""
    try:
        barset = api.get_latest_trade(symbol)
        return float(barset.price)
    except Exception as e:
        logbot.logs(f"⚠️ Failed to get latest price for {symbol}: {e}", True)
        return None

def get_account_equity():
    """获取账户总资产"""
    try:
        account = api.get_account()
        return float(account.equity)
    except Exception as e:
        logbot.logs(f"⚠️ Failed to get account equity: {e}", True)
        return None

def get_position_qty(symbol):
    """获取某个标的当前持仓数量"""
    try:
        position = api.get_position(symbol)
        return float(position.qty)
    except:
        return 0.0

def order(payload):
    symbol = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    qty = payload.get("qty", None)
    percent = payload.get("percent", None)
    price = payload.get("price", None)
    strategy = payload.get("strategy", "")
    subaccount = payload.get("subaccount", "default")

    # 获取价格（用于 percent 模式）
    if price is None:
        price = get_latest_price(symbol)

    if not price:
        return {"success": False, "message": "❌ Cannot fetch price"}

    # qty fallback（支持 percent 自动计算）
    if qty is None:
        if percent:
            equity = get_account_equity()
            if not equity:
                return {"success": False, "message": "❌ Cannot fetch account equity"}
            cost = equity * float(percent)
            qty = cost / price
        elif action == "SELL":
            qty = get_position_qty(symbol)
        else:
            return {"success": False, "message": "❌ Must provide either qty or percent"}

    # 整数化 qty
    qty = int(qty)

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
