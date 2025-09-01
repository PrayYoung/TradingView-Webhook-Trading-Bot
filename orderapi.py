import os, logbot
from decimal import Decimal
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest, StockLatestBarRequest

# =========================
# Helpers
# =========================
def _parse_bool_env(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _is_crypto(raw: str) -> bool:
    s = (raw or "").upper()
    # 简单判断：含 "/" 或 以 USD/USDT 结尾（适配常见加密对写法）
    return ("/" in s) or s.endswith("USD") or s.endswith("USDT")

def _norm_trade_symbol(raw: str) -> str:
    """
    供 交易 / 持仓 使用的符号：
    - crypto: 诸如 ETH/USD、ETHUSDT、BINANCE:ETHUSDT 等 ——> ETHUSD
    - stock: 原样返回（AAPL）
    """
    s = (raw or "").strip().upper()
    if _is_crypto(s):
        s = s.replace("BINANCE:", "").replace("COINBASE:", "").replace("FTX:", "")
        s = s.replace("USDT", "USD").replace("/", "").replace(":", "")
    return s

def _to_crypto_pair_for_data(symbol_raw: str) -> str:
    """
    数据端尽量使用带斜杠的 crypto 对（ETH/USD）。
    若传入 ETHUSD / ETHUSDT，则转换为 ETH/USD。
    仅在 is_crypto 时转换。
    """
    s = (symbol_raw or "").strip().upper()
    if not _is_crypto(s):
        return s
    if "/" in s:
        return s
    # ETHUSD / ETHUSDT -> ETH/USD
    base = s.replace("USDT", "USD")
    if base.endswith("USD") and len(base) > 3:
        return f"{base[:-3]}/USD"
    return s

def _clamp_crypto_qty(q: Decimal) -> str:
    """
    Alpaca crypto 支持小数数量；保留 6 位且避免 0。
    """
    if q <= Decimal("0"):
        q = Decimal("0.000001")
    return format(q.quantize(Decimal("0.000001")), "f")

def _to_tif_enum(tif: str) -> TimeInForce:
    try:
        return TimeInForce[tif.strip().upper()]
    except Exception:
        return TimeInForce.GTC

# =========================
# Env & Clients
# =========================
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
USE_PAPER = _parse_bool_env("USE_PAPER", True)

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=USE_PAPER)
stock_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
crypto_client = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# =========================
# Market Data
# =========================
def get_latest_price(symbol_raw: str):
    """
    先尝试加密（用带斜杠的对），失败再回退股票。
    """
    # ---- Crypto ----
    try:
        sym_for_data = _to_crypto_pair_for_data(symbol_raw)
        if _is_crypto(sym_for_data):
            quote_map = crypto_client.get_crypto_latest_quote(
                CryptoLatestQuoteRequest(symbol_or_symbols=[sym_for_data])
            )
            quote = quote_map[sym_for_data]
            mid = (quote.bid_price + quote.ask_price) / 2
            return float(mid)
    except Exception as e:
        logbot.logs(f"[Price] Crypto quote failed for {symbol_raw}: {e}", True)

    # ---- Stock ----
    try:
        bar_map = stock_client.get_latest_bar(StockLatestBarRequest(symbol_or_symbols=[symbol_raw]))
        return float(bar_map[symbol_raw].close)
    except Exception as e2:
        logbot.logs(f"[Price] Stock quote failed for {symbol_raw}: {e2}", True)

    return None

# =========================
# Order Entry
# =========================
def order(payload):
    symbol_raw = payload.get("ticker")
    action = (payload.get("action", "BUY") or "").upper()
    strategy = payload.get("strategy", "")
    subaccount = payload.get("subaccount", "default")

    percentage = payload.get("percentage", None)   # 0.3 表示 30%
    qty_in = payload.get("qty", None)              # 显式数量优先
    order_type = (payload.get("order_type", "market") or "").lower()
    tif = (payload.get("time_in_force", "gtc") or "gtc").lower()

    limit_price = payload.get("limit_price", None)
    stop_price = payload.get("stop_price", None)

    logbot.logs(
        f"📩 Incoming Order:\n"
        f"Subaccount: {subaccount}\nStrategy: {strategy}\n"
        f"{action} {qty_in if qty_in else ''} x {symbol_raw} ({order_type.upper()})"
    )

    # 关键：交易/持仓统一用“无斜杠、USDT->USD”的符号
    symbol_trade = _norm_trade_symbol(symbol_raw)

    try:
        # ---------- 方向：BUY / SELL ----------
        if action == "BUY":
            # qty 明确则优先，其次 percentage，最后默认 1
            if qty_in is not None:
                qty_dec = Decimal(str(qty_in))
            elif percentage:
                price = get_latest_price(symbol_raw)
                if not price:
                    raise Exception("No price data")
                buying_power = Decimal(str(trading_client.get_account().cash))
                qty_dec = (buying_power * Decimal(str(percentage))) / Decimal(str(price))
            else:
                qty_dec = Decimal("1")
            side = OrderSide.BUY

        elif action == "SELL":
            # 先查仓（注意必须用规范化后的 symbol_trade，避免 ETH/USD 触发 404）
            try:
                pos = trading_client.get_open_position(symbol_trade)
                pos_qty = Decimal(str(pos.qty))
            except Exception as ge:
                raise Exception(f"Not holding {symbol_trade}: {ge}")

            if pos_qty <= Decimal("0"):
                raise Exception(f"No open position for {symbol_trade}")

            if qty_in is not None:
                qty_dec = Decimal(str(qty_in))
            elif percentage:
                qty_dec = (pos_qty * Decimal(str(percentage)))
            else:
                qty_dec = pos_qty
            side = OrderSide.SELL

        else:
            return {"success": False, "message": f"❌ Unknown action: {action}"}

        # ---------- 数量规整 ----------
        if _is_crypto(symbol_raw):
            qty_str = _clamp_crypto_qty(qty_dec)
        else:
            # 股票至少 1 股
            if qty_dec < 1:
                qty_dec = Decimal("1")
            qty_str = str(int(qty_dec))

        # ---------- TIF ----------
        tif_enum = _to_tif_enum(tif)

        # ---------- 构建订单 ----------
        if order_type == "market":
            order_request = MarketOrderRequest(
                symbol=symbol_trade,
                qty=qty_str,
                side=side,
                time_in_force=tif_enum
            )
        elif order_type == "limit":
            if limit_price is None:
                return {"success": False, "message": "❌ Missing limit_price for limit order"}
            order_request = LimitOrderRequest(
                symbol=symbol_trade,
                qty=qty_str,
                side=side,
                time_in_force=tif_enum,
                limit_price=str(limit_price)
            )
        elif order_type == "stop":
            if stop_price is None:
                return {"success": False, "message": "❌ Missing stop_price for stop order"}
            order_request = StopOrderRequest(
                symbol=symbol_trade,
                qty=qty_str,
                side=side,
                time_in_force=tif_enum,
                stop_price=str(stop_price)
            )
        else:
            return {"success": False, "message": f"❌ Unsupported order type: {order_type}"}

        # ---------- 下单 ----------
        resp = trading_client.submit_order(order_request)
        logbot.logs(f"✅ Submitted {order_type} order: {resp.id} {symbol_trade} x {qty_str}")
        return {"success": True, "message": f"✅ Submitted {order_type} order for {qty_str} x {symbol_trade}"}

    except Exception as e:
        # 尽量把错误打全，包含规范化后的符号，便于定位
        logbot.logs(f"🚫 Order failed ({symbol_trade}): {type(e).__name__}: {e}", True)
        return {"success": False, "message": f"Error: {e}"}
