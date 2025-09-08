# -*- coding: utf-8 -*-
"""
Daily Report → Discord Embed
- 多账户（通过 REPORT_ALIASES 指定别名列表）
- 账户权益与当日订单统计(filled/canceled/rejected)
- 可选 Supabase 队列健康度
- 同时打印到 stdout,便于 Render Logs 里查看

需要的环境变量（最少集）：
  # —— Discord ——
  DISCORD_WEBHOOK_URL= https://discord.com/api/webhooks/...

  # —— Alpaca 默认账户（必需）——
  ALPACA_API_KEY=
  ALPACA_SECRET_KEY=
  ALPACA_BASE_URL= https://paper-api.alpaca.markets

  # —— 多账户（可选，别名全部小写；与 subaccount 一致）——
  # ALPACA_API_KEY__crypto_trading_account=
  # ALPACA_SECRET_KEY__crypto_trading_account=
  # ALPACA_BASE_URL__crypto_trading_account=

  # 汇报哪些账户（逗号分隔；默认只报 default)
  # REPORT_ALIASES=default,crypto_trading_account

  # —— Supabase(可选，仅用于队列健康度）——
  # SUPABASE_URL=
  # SUPABASE_SERVICE_ROLE_KEY=
"""

import os, sys, json, datetime, requests
from datetime import timezone
from typing import List, Dict, Any

try:
    from supabase import create_client  # optional
except Exception:
    create_client = None


# -----------------------
# Utils
# -----------------------
def now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=timezone.utc)

def start_of_utc_day(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def normalize_base(base: str) -> str:
    if not base:
        return base
    base = base.strip().rstrip("/")
    return base[:-3] if base.endswith("/v2") else base

def aliases_from_env() -> List[str]:
    raw = os.getenv("REPORT_ALIASES", "default")
    return [a.strip() for a in raw.split(",") if a.strip()]


from config import resolve_alpaca_for_alias

def alpaca_get(base: str, key: str, sec: str, path: str, params=None, timeout=8) -> Any:
    url = f"{base}/v2{path if path.startswith('/') else '/'+path}"
    r = requests.get(
        url,
        headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec},
        params=params or {},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()

def fetch_account_snapshot(alias: str) -> Dict[str, Any]:
    key, sec, base, _paper = resolve_alpaca_for_alias(alias)
    acct = alpaca_get(base, key, sec, "/account")
    equity = float(acct.get("equity") or 0.0)
    last_equity = float(acct.get("last_equity") or equity)

    # 当日订单
    after_iso = start_of_utc_day(now_utc()).isoformat()
    orders = alpaca_get(base, key, sec, "/orders", params={
        "status": "all",
        "after": after_iso,
        "limit": 500
    })
    filled = [o for o in orders if o.get("status") in ("filled", "partially_filled")]
    canceled = [o for o in orders if o.get("status") == "canceled"]
    rejected = [o for o in orders if o.get("status") == "rejected"]

    # 简单的方向计数（可选）
    def side_counts(dd: List[Dict[str, Any]]):
        b = sum(1 for x in dd if (x.get("side") or "").lower() == "buy")
        s = sum(1 for x in dd if (x.get("side") or "").lower() == "sell")
        return b, s

    b_f, s_f = side_counts(filled)

    return {
        "alias": alias,
        "equity": equity,
        "equity_change": equity - last_equity,
        "filled_count": len(filled),
        "filled_buy": b_f,
        "filled_sell": s_f,
        "canceled_count": len(canceled),
        "rejected_count": len(rejected),
    }


# -----------------------
# Supabase health (optional)
# -----------------------
def supabase_health() -> Dict[str, Any] | None:
    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_ROLE_KEY") and create_client):
        return None
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    try:
        # ready 队列
        ready = sb.table("order_queue").select("id", count="exact").eq("status","ready").execute().count or 0
        # 今天失败
        failed_today = sb.table("order_queue").select("id", count="exact") \
            .eq("status","failed") \
            .gte("updated_at", start_of_utc_day(now_utc()).isoformat()) \
            .execute().count or 0
        # DLQ 总数
        dlq = sb.table("order_queue_dlq").select("id", count="exact").execute().count or 0
        return {"queue_ready": ready, "queue_failed_today": failed_today, "dlq_total": dlq}
    except Exception as e:
        return {"error": str(e)}


# -----------------------
# Discord formatting & post
# -----------------------
def to_human_delta(x: float) -> str:
    return f"{x:+.2f}"

def build_discord_embed(rows: List[Dict[str, Any]], sbh: Dict[str, Any] | None) -> Dict[str, Any]:
    # Embed 色条（绿色）
    color = 0x2ecc71

    fields = []
    for r in rows:
        title = f"[{r['alias']}]"
        value = (
            f"**Equity**: `{r['equity']:.2f}`  (**Δ** {to_human_delta(r['equity_change'])})\n"
            f"**Filled**: `{r['filled_count']}`  (B:`{r['filled_buy']}` / S:`{r['filled_sell']}`)\n"
            f"**Canceled**: `{r['canceled_count']}`  •  **Rejected**: `{r['rejected_count']}`"
        )
        fields.append({"name": title, "value": value, "inline": False})

    if sbh:
        if "error" in sbh:
            fields.append({"name": "Supabase", "value": f"⚠️ `{sbh['error']}`", "inline": False})
        else:
            fields.append({
                "name": "Queue Health",
                "value": f"ready: `{sbh['queue_ready']}` • failed_today: `{sbh['queue_failed_today']}` • dlq: `{sbh['dlq_total']}`",
                "inline": False
            })

    embed = {
        "title": "📊 Daily Trading Report",
        "description": f"UTC {now_utc().strftime('%Y-%m-%d %H:%M')}",
        "color": color,
        "fields": fields,
        "footer": {"text": "TradingView → Webhook → Supabase → Worker → Alpaca"},
        "timestamp": now_utc().isoformat(),
    }
    return embed

def post_discord(embed: Dict[str, Any]):
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url:
        print("No DISCORD_WEBHOOK_URL configured; skip posting.", file=sys.stderr)
        return
    payload = {"embeds": [embed]}
    r = requests.post(url, json=payload, timeout=8)
    if r.status_code >= 300:
        raise RuntimeError(f"Discord webhook failed: {r.status_code} {r.text}")


# -----------------------
# Main
# -----------------------
def main():
    rows = []
    errs = []
    for alias in aliases_from_env():
        try:
            rows.append(fetch_account_snapshot(alias))
        except Exception as e:
            errs.append(f"[{alias}] {e}")

    sbh = supabase_health()
    embed = build_discord_embed(rows, sbh)

    # 同时打印到 stdout
    print(json.dumps(embed, ensure_ascii=False, indent=2))

    # Discord
    try:
        post_discord(embed)
        print("Posted to Discord.")
    except Exception as e:
        print(f"Discord post failed: {e}", file=sys.stderr)

    # 如有报错情况，附加一条错误提示（可选）
    if errs:
        try:
            err_embed = {
                "title": "Daily Report – Errors",
                "description": "\n".join(errs),
                "color": 0xe74c3c,
                "timestamp": now_utc().isoformat(),
            }
            post_discord(err_embed)
        except Exception as e:
            print(f"Error embed failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
