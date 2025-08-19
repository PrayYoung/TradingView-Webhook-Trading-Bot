import os
import alpaca_trade_api as tradeapi
import logbot

# 从环境变量获取 API 凭证
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')


def get_latest_price(symbol):
    try:
        # Step 1: Try as stock
        trade = api.get_latest_trade(symbol)
        return float(trade.price)
    except Exception as e_stock:
        logbot.logs(f"⚠️ Stock price fetch failed for {symbol}, trying crypto...")

        try:
            # Step 2: Try as crypto
            quote = api.get_latest_crypto_quote(symbol)
            mid_price = (float(quote.bid_price) + float(quote.ask_price)) / 2
            return mid_price
        except Exception as e_crypto:
            logbot.logs(f"❌ Failed to get any price for {symbol}: {e_crypto}", True)
            return None

def order(payload):
    symbol = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    strategy = payload.get("strategy", "")
    subaccount = payload.get("subaccount", "default")

    # 可选字段
    percentage = payload.get("percentage", None)
    qty = payload.get("qty", None)

    logbot.logs(f"📩 Incoming Order:\nSubaccount: {subaccount}\nStrategy: {strategy}\n{action} {qty if qty else ''} x {symbol}")

    try:
        if action == "BUY":
            if percentage:
                price = get_latest_price(symbol)
                if not price:
                    raise Exception("No price data")
                buying_power = float(api.get_account().cash)
                qty = round((buying_power * float(percentage)) / price, 6)
            elif qty is None:
                qty = 1

            api.submit_order(
                symbol=symbol,
                qty=qty,
                side='buy',
                type='market',
                time_in_force='gtc'
            )
            return {"success": True, "message": f"✅ Bought {qty} x {symbol}"}

        elif action == "SELL":
            # always full sell
            position = api.get_position(symbol)
            qty = float(position.qty)

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
