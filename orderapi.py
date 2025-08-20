import os, logbot
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest, StockLatestBarRequest

# order type: "market" / "limit" / "stop"
# time in force: "gtc" / "day" / "ioc" / "fok" / "opg" / "cls"
#                 Good Till Cancel/ Day/ Immediate Or Cancel / Fill or Kill/ On the Open/ On the Close


# 环境变量
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
USE_PAPER = os.getenv("USE_PAPER")

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=USE_PAPER)
stock_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
crypto_client = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

def get_latest_price(symbol):
    try:
        # Try crypto first
        quote = crypto_client.get_crypto_latest_quote(
            CryptoLatestQuoteRequest(symbol_or_symbols=[symbol])
        )[symbol]
        return (quote.bid_price + quote.ask_price) / 2
    except Exception as e:
        logbot.logs(f"[Price] Crypto quote failed for {symbol}: {e}", True)
        try:
            bar = stock_client.get_latest_bar(StockLatestBarRequest(symbol_or_symbols=[symbol]))
            return float(bar[symbol].close)
        except Exception as e2:
            logbot.logs(f"[Price] Stock quote failed for {symbol}: {e2}", True)
    return None

def order(payload):
    symbol = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    strategy = payload.get("strategy", "")
    subaccount = payload.get("subaccount", "default")

    percentage = payload.get("percentage", None)
    qty = payload.get("qty", None)
    order_type = payload.get("order_type", "market").lower()
    tif = payload.get("time_in_force", "gtc").lower()

    limit_price = payload.get("limit_price", None)
    stop_price = payload.get("stop_price", None)

    logbot.logs(f"📩 Incoming Order:\nSubaccount: {subaccount}\nStrategy: {strategy}\n{action} {qty if qty else ''} x {symbol} ({order_type.upper()})")

    try:
        if action == "BUY":
            if percentage:
                price = get_latest_price(symbol)
                if not price:
                    raise Exception("No price data")
                buying_power = float(trading_client.get_account().cash)
                qty = round((buying_power * float(percentage)) / price, 6)
            elif qty is None:
                qty = 1
            side = OrderSide.BUY

        elif action == "SELL":
            position = trading_client.get_open_position(symbol)
            qty = float(position.qty)
            side = OrderSide.SELL

        else:
            return {"success": False, "message": f"❌ Unknown action: {action}"}

        # 构建请求对象
        if order_type == "market":
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce(tif)
            )
        elif order_type == "limit":
            if not limit_price:
                return {"success": False, "message": "❌ Missing limit_price for limit order"}
            order_request = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce(tif),
                limit_price=limit_price
            )
        elif order_type == "stop":
            if not stop_price:
                return {"success": False, "message": "❌ Missing stop_price for stop order"}
            order_request = StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce(tif),
                stop_price=stop_price
            )
        else:
            return {"success": False, "message": f"❌ Unsupported order type: {order_type}"}

        trading_client.submit_order(order_request)
        return {"success": True, "message": f"✅ Submitted {order_type} order for {qty} x {symbol}"}

    except Exception as e:
        logbot.logs(f"🚫 Order failed: {e}", True)
        return {"success": False, "message": f"Error: {e}"}
