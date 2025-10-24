import os, logbot
from decimal import Decimal
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    TakeProfitRequest,   # 新增：Bracket 的 TP 腿
    StopLossRequest,     # 新增：Bracket 的 SL 腿（非 StopOrderRequest）
    GetOrdersRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, QueryOrderStatus  # 新增 OrderClass
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest, StockLatestBarRequest
from config import resolve_alpaca_for_alias

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
    key, secret, _base, paper = resolve_alpaca_for_alias(alias)
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
        request = StockLatestBarRequest(symbol_or_symbols=[symbol_raw])
        bar_map = stock_client.get_stock_latest_bar(request)
        bar = bar_map.get(symbol_raw) or bar_map.get(symbol_raw.upper())
        if bar:
            return float(bar.close)
        raise KeyError(f"No latest bar for {symbol_raw}")
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
    max_slots_raw = payload.get("max_slots", None)
    buffer_ratio_raw = payload.get("buffer_ratio", None)
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
            max_slots = None
            if max_slots_raw is not None:
                try:
                    max_slots_int = int(Decimal(str(max_slots_raw)))
                    if max_slots_int > 0:
                        max_slots = max_slots_int
                    else:
                        logbot.logs(f"[Order] Ignoring non-positive max_slots={max_slots_raw}")
                except Exception:
                    logbot.logs(f"[Order] Invalid max_slots value: {max_slots_raw}")

            if qty_in is not None:
                qty_dec = Decimal(str(qty_in))
            elif percentage:
                price = get_latest_price(symbol_raw, stock_client, crypto_client)
                if not price:
                    raise Exception("No price data")
                buying_power = Decimal(str(trading_client.get_account().cash))
                qty_dec = (buying_power * Decimal(str(percentage))) / Decimal(str(price))
            elif max_slots:
                # ---- max_slots sizing ----
                account = trading_client.get_account()
                try:
                    equity = Decimal(str(account.equity))
                except Exception:
                    raise Exception("Failed to load account equity for max_slots sizing")

                buffer_ratio = Decimal("0.05")
                if buffer_ratio_raw is not None:
                    try:
                        buffer_ratio = Decimal(str(buffer_ratio_raw))
                    except Exception:
                        logbot.logs(f"[Order] Invalid buffer_ratio value: {buffer_ratio_raw}, defaulting to 0.05")
                if buffer_ratio < Decimal("0"):
                    logbot.logs(f"[Order] buffer_ratio < 0 ({buffer_ratio}); clamping to 0")
                    buffer_ratio = Decimal("0")
                if buffer_ratio >= Decimal("1"):
                    logbot.logs(f"[Order] buffer_ratio >= 1 ({buffer_ratio}); clamping to 0.95")
                    buffer_ratio = Decimal("0.95")

                available_equity = equity * (Decimal("1") - buffer_ratio)
                if available_equity <= Decimal("0"):
                    raise Exception("Available equity is non-positive after buffer_ratio adjustment")

                try:
                    positions = trading_client.get_all_positions()
                except Exception as pos_err:
                    raise Exception(f"Failed to fetch open positions: {pos_err}")
                open_slots = sum(
                    1 for p in positions
                    if Decimal(str(getattr(p, "qty", "0"))).copy_abs() > Decimal("0")
                )
                if open_slots >= max_slots:
                    msg = (
                        f"Skipped BUY for {symbol_trade}: open positions {open_slots} "
                        f"already at/above max_slots {max_slots}"
                    )
                else:
                    target_value = available_equity / Decimal(str(max_slots))
                    price = get_latest_price(symbol_raw, stock_client, crypto_client)
                    if not price:
                        raise Exception("No price data")
                    qty_dec = target_value / Decimal(str(price))
                    if qty_dec <= Decimal("0"):
                        raise Exception("Calculated quantity is non-positive with max_slots sizing")
                    logbot.logs(
                        f"[Order] max_slots sizing -> equity {equity} buffer {buffer_ratio} "
                        f"target_value {target_value} qty {qty_dec}"
                    )
                if open_slots >= max_slots:
                    logbot.logs(f"[Order] {msg}")
                    return {"success": True, "message": msg}
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

            # 卖出前：取消该标的的未成交 SELL 方向挂单（通常为 bracket 子单）以避免冲突/过度对冲
            try:
                od_filter = GetOrdersRequest(
                    status=QueryOrderStatus.OPEN,
                    symbols=[symbol_trade],
                    nested=True,
                )
                open_orders = trading_client.get_orders(filter=od_filter)
                canceled_cnt = 0
                for od in open_orders:
                    try:
                        od_side = str(getattr(od, "side", "")).lower()
                        if od_side == "sell":
                            trading_client.cancel_order_by_id(od.id)
                            canceled_cnt += 1
                    except Exception as ce:
                        logbot.logs(f"[Order] cancel child sell order failed for {symbol_trade}: {ce}")
                if canceled_cnt:
                    logbot.logs(f"[Order] Canceled {canceled_cnt} open SELL orders for {symbol_trade} before SELL")
            except Exception as e_can:
                # 非关键路径，失败只记录日志
                logbot.logs(f"[Order] fetch/cancel open orders failed for {symbol_trade}: {e_can}")

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

        # ---------- 构建订单（兼容旧 SDK 的 client_order_id） ----------
        def _build(kind_local: str, with_id, no_id):
            try:
                req = with_id()
                return kind_local, req
            except Exception as e:
                logbot.logs(f"[Order] client_order_id not supported, fallback: {e}")
                return kind_local, no_id()

        # 仅在 BUY 进场时使用 BRACKET；SELL 依旧为平仓，不做空
        if (tp is not None and sl is not None) and side == OrderSide.BUY:
            if order_type == "limit":
                if limit_price is None:
                    return {"success": False, "message": "❌ Missing limit_price for limit bracket order"}
                kind, order_request = _build(
                    "LIMIT BRACKET",
                    lambda: LimitOrderRequest(
                        symbol=symbol_trade,
                        qty=qty_str,
                        side=side,
                        time_in_force=tif_enum,
                        limit_price=str(limit_price),
                        client_order_id=client_order_id_in,
                        order_class=OrderClass.BRACKET,
                        take_profit=TakeProfitRequest(limit_price=float(tp)),
                        stop_loss=StopLossRequest(stop_price=float(sl))
                    ),
                    lambda: LimitOrderRequest(
                        symbol=symbol_trade,
                        qty=qty_str,
                        side=side,
                        time_in_force=tif_enum,
                        limit_price=str(limit_price),
                        order_class=OrderClass.BRACKET,
                        take_profit=TakeProfitRequest(limit_price=float(tp)),
                        stop_loss=StopLossRequest(stop_price=float(sl))
                    )
                )
            else:
                order_type = "market"
                kind, order_request = _build(
                    "MARKET BRACKET",
                    lambda: MarketOrderRequest(
                        symbol=symbol_trade,
                        qty=qty_str,
                        side=side,
                        time_in_force=tif_enum,
                        client_order_id=client_order_id_in,
                        order_class=OrderClass.BRACKET,
                        take_profit=TakeProfitRequest(limit_price=float(tp)),
                        stop_loss=StopLossRequest(stop_price=float(sl))
                    ),
                    lambda: MarketOrderRequest(
                        symbol=symbol_trade,
                        qty=qty_str,
                        side=side,
                        time_in_force=tif_enum,
                        order_class=OrderClass.BRACKET,
                        take_profit=TakeProfitRequest(limit_price=float(tp)),
                        stop_loss=StopLossRequest(stop_price=float(sl))
                    )
                )
        elif order_type == "market":
            kind, order_request = _build(
                "MARKET",
                lambda: MarketOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    client_order_id=client_order_id_in
                ),
                lambda: MarketOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                )
            )
        elif order_type == "limit":
            if limit_price is None:
                return {"success": False, "message": "❌ Missing limit_price for limit order"}
            kind, order_request = _build(
                "LIMIT",
                lambda: LimitOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    limit_price=str(limit_price),
                    client_order_id=client_order_id_in
                ),
                lambda: LimitOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    limit_price=str(limit_price),
                )
            )
        elif order_type == "stop":
            if stop_price is None:
                return {"success": False, "message": "❌ Missing stop_price for stop order"}
            kind, order_request = _build(
                "STOP",
                lambda: StopOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    stop_price=str(stop_price),
                    client_order_id=client_order_id_in
                ),
                lambda: StopOrderRequest(
                    symbol=symbol_trade,
                    qty=qty_str,
                    side=side,
                    time_in_force=tif_enum,
                    stop_price=str(stop_price),
                )
            )
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
