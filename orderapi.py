import os, logbot
from decimal import Decimal
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    TakeProfitRequest,   # 新增：Bracket 的 TP 腿
    StopLossRequest      # 新增：Bracket 的 SL 腿（非 StopOrderRequest）
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass  # 新增 OrderClass
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
# Env resolution (multi-account)
# =========================

def _get_env_first(*names):
    for n in names:
        v = os.getenv(n)
        if v is not None and v != "":
            return v
    return None

def _resolve_alpaca_creds(alias: str):
    """
    Resolve Alpaca credentials for an alias (e.g., 'paper', 'live', 'default').
    Supports both new and legacy env names and alias suffixes with __<alias>.
    """
    a = (alias or "default").strip().lower()

    # Key ID
    key = (
        _get_env_first(f"ALPACA_KEY_ID__{a}", f"ALPACA_API_KEY__{a}")
        or _get_env_first("ALPACA_KEY_ID", "ALPACA_API_KEY", "APCA_API_KEY_ID")
    )
    # Secret
    secret = (
        _get_env_first(f"ALPACA_SECRET_KEY__{a}", f"ALPACA_API_SECRET__{a}")
        or _get_env_first("ALPACA_SECRET_KEY", "ALPACA_API_SECRET", "APCA_API_SECRET_KEY")
    )

    # Paper flag by base URL or explicit USE_PAPER
    base_url = _get_env_first(f"ALPACA_BASE_URL__{a}", "ALPACA_BASE_URL") or ""
    if base_url:
        paper = ("paper" in base_url)
    else:
        paper_env = _get_env_first(f"USE_PAPER__{a}", "USE_PAPER")
        paper = True if paper_env is None else _parse_bool_env(f"USE_PAPER__{a}", _parse_bool_env("USE_PAPER", True))

    if not key or not secret:
        raise RuntimeError(f"Missing Alpaca creds for alias '{a}'")
    return key, secret, paper

# Cache clients by alias
_clients = {}

def _get_clients(alias: str):
    a = (alias or "default").strip().lower()
    if a in _clients:
        return _clients[a]
    key, secret, paper = _resolve_alpaca_creds(a)
    trading = TradingClient(key, secret, paper=paper)
    stock = StockHistoricalDataClient(key, secret)
    crypto = CryptoHistoricalDataClient(key, secret)
    _clients[a] = (trading, stock, crypto)
    return _clients[a]

# =========================
# Market Data
# =========================
def get_latest_price(symbol_raw: str, stock_client: StockHistoricalDataClient, crypto_client: CryptoHistoricalDataClient):
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
    client_order_id_in = payload.get("client_order_id")
    if client_order_id_in:
        client_order_id_in = str(client_order_id_in)[:48]

    percentage = payload.get("percentage", None)   # 0.3 表示 30%
    qty_in = payload.get("qty", None)              # 显式数量优先
    order_type = (payload.get("order_type", "market") or "").lower()
    tif = (payload.get("time_in_force", "gtc") or "gtc").lower()

    # —— 兼容字段名：tp/sl 或 take_profit/stop_loss 或 *_px —— 
    tp = payload.get("tp")
    if tp is None:
        tp = payload.get("take_profit") or payload.get("take_profit_px")
    sl = payload.get("sl")
    if sl is None:
        sl = payload.get("stop_loss") or payload.get("stop_px")

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
        trading_client, stock_client, crypto_client = _get_clients(subaccount)
        # ---------- 方向：BUY / SELL ----------
        if action == "BUY":
            # qty 明确则优先，其次 percentage，最后默认 1
            if qty_in is not None:
                qty_dec = Decimal(str(qty_in))
            elif percentage:
                price = get_latest_price(symbol_raw, stock_client, crypto_client)
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
        # 优先：若传入了 tp/sl，则用 Bracket 订单（多空都支持）
        if tp is not None and sl is not None:
            if order_type == "limit":
                if limit_price is None:
                    return {"success": False, "message": "❌ Missing limit_price for limit bracket order"}
                order_request = LimitOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    limit_price=str(limit_price),
                    client_order_id=client_order_id_in,
                    order_class=OrderClass.BRACKET,
                    take_profit=TakeProfitRequest(limit_price=float(tp)),
                    # 如需止损限价，可加 limit_price=float(sl)
                    stop_loss=StopLossRequest(stop_price=float(sl))
                )
                kind = "LIMIT BRACKET"
            else:
                # 默认用“市价 + bracket”
                order_type = "market"
                order_request = MarketOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    client_order_id=client_order_id_in,
                    order_class=OrderClass.BRACKET,
                    take_profit=TakeProfitRequest(limit_price=float(tp)),
                    stop_loss=StopLossRequest(stop_price=float(sl))
                )
                kind = "MARKET BRACKET"

        # 否则：按你原来的三种单腿类型处理
        elif order_type == "market":
            order_request = MarketOrderRequest(
                symbol=symbol_trade,
                qty=qty_str,
                side=side,
                time_in_force=tif_enum,
                client_order_id=client_order_id_in
            )
            kind = "MARKET"
        elif order_type == "limit":
            if limit_price is None:
                return {"success": False, "message": "❌ Missing limit_price for limit order"}
            order_request = LimitOrderRequest(
                symbol=symbol_trade,
                qty=qty_str,
                side=side,
                time_in_force=tif_enum,
                limit_price=str(limit_price),
                client_order_id=client_order_id_in
            )
            kind = "LIMIT"
        elif order_type == "stop":
            if stop_price is None:
                return {"success": False, "message": "❌ Missing stop_price for stop order"}
            order_request = StopOrderRequest(
                symbol=symbol_trade,
                qty=qty_str,
                side=side,
                time_in_force=tif_enum,
                stop_price=str(stop_price),
                client_order_id=client_order_id_in
            )
            kind = "STOP"
        else:
            return {"success": False, "message": f"❌ Unsupported order type: {order_type}"}

        # ---------- 下单 ----------
        resp = trading_client.submit_order(order_request)
        logbot.logs(f"✅ Submitted {kind} order: {resp.id} {symbol_trade} x {qty_str}")
        return {"success": True, "message": f"✅ Submitted {kind} order for {qty_str} x {symbol_trade}"}

    except Exception as e:
        # 尽量把错误打全，包含规范化后的符号，便于定位
        logbot.logs(f"🚫 Order failed ({symbol_trade}): {type(e).__name__}: {e}", True)
        return {"success": False, "message": f"Error: {e}"}
