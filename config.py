import os
import requests
from datetime import datetime, timezone, date

# Existing placeholders kept for backward compatibility
WEBHOOK_PASSPHRASE = None
WEBHOOK_PASSPHRASE_V2 = None

DISCORD_LOGS_URL = None
DISCORD_ERR_URL = None
DISCORD_AVATAR_URL = None
DISCORD_STUDY_URL = None
DISCORD_STUDY_AVATAR_URL = None

LEVERAGE_TESTING = None
RISK_TESTING = None
API_KEY_TESTING = None
API_SECRET_TESTING = None

LEVERAGE_MYBYBITACCOUNT = None
RISK_MYBYBITACCOUNT = None
API_KEY_MYBYBITACCOUNT = None
API_SECRET_MYBYBITACCOUNT = None


# ===== Centralized config helpers for v2/worker risk guard =====

def _env_first(*names):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def _parse_bool_env(name: str, default: bool = None) -> bool | None:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def resolve_alpaca_for_alias(alias: str):
    """
    Canonical resolver for Alpaca credentials and endpoint by alias.
    Supports these envs (by precedence):
      - Key:   ALPACA_KEY_ID__alias, ALPACA_API_KEY__alias, ALPACA_KEY_ID, ALPACA_API_KEY, APCA_API_KEY_ID
      - Secret:ALPACA_SECRET_KEY__alias, ALPACA_API_SECRET__alias, ALPACA_SECRET_KEY, ALPACA_API_SECRET, APCA_API_SECRET_KEY
      - Base:  ALPACA_BASE_URL__alias, ALPACA_BASE_URL (normalized; no trailing /v2)
      - Paper mode: TRADING_MODE (paper|live) preferred; else infer from base; else USE_PAPER__alias/USE_PAPER

    Returns: (key, secret, base, paper_bool)
    """
    a = (alias or "default").strip().lower()

    key = (
        _env_first(f"ALPACA_KEY_ID__{a}", f"ALPACA_API_KEY__{a}")
        or _env_first("ALPACA_KEY_ID", "ALPACA_API_KEY", "APCA_API_KEY_ID")
    )
    sec = (
        _env_first(f"ALPACA_SECRET_KEY__{a}", f"ALPACA_API_SECRET__{a}")
        or _env_first("ALPACA_SECRET_KEY", "ALPACA_API_SECRET", "APCA_API_SECRET_KEY")
    )
    base = _env_first(f"ALPACA_BASE_URL__{a}", "ALPACA_BASE_URL") or "https://paper-api.alpaca.markets"
    base = base.rstrip("/")
    if base.endswith("/v2"):
        base = base[:-3]

    if not key or not sec:
        raise RuntimeError(f"Missing Alpaca creds for alias '{a}'")

    tm = (os.getenv("TRADING_MODE") or "").strip().lower()
    if tm in ("paper", "live"):
        paper = (tm == "paper")
    else:
        if "paper-api.alpaca.markets" in base:
            paper = True
        else:
            paper = _parse_bool_env(f"USE_PAPER__{a}", _parse_bool_env("USE_PAPER", True))

    return key, sec, base, bool(paper)


def get_day_key_utc(dt: datetime | None = None) -> str:
    dt = dt or datetime.now(tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


# Simple in-module equity cache: { alias: (ts_epoch, equity_float) }
_equity_cache = {}


def get_equity_cached(alias: str, ttl_sec: int = 60) -> float:
    import time
    now = time.time()
    ts, eq = _equity_cache.get(alias, (0, None))
    if eq is not None and (now - ts) < ttl_sec:
        return eq
    key, sec, base, _paper = resolve_alpaca_for_alias(alias)
    r = requests.get(f"{base}/v2/account", headers={
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": sec,
    }, timeout=5)
    r.raise_for_status()
    eq = float(r.json()["equity"])  # string numeric
    _equity_cache[alias] = (now, eq)
    return eq


def load_account_state(sb) -> dict | None:
    try:
        d = sb.table("account_state").select("*").eq("id", 1).execute().data
        return d[0] if d else None
    except Exception:
        return None


def update_account_state(sb, **fields) -> dict | None:
    try:
        res = sb.table("account_state").update(fields).eq("id", 1).execute()
        return res.data[0] if res and res.data else None
    except Exception:
        return None


def get_or_set_day_open_equity(sb, alias: str, now_utc) -> float | None:
    """
    Ensure daily_metrics row exists for (d, alias). Return stored 'equity' (may be None).
    Caller decides when to set equity on first observation after reset window.
    """
    dkey = get_day_key_utc(now_utc)
    try:
        res = sb.table("daily_metrics").select("id,equity").eq("d", dkey).eq("alias", alias).execute()
        rows = res.data or []
        if not rows:
            sb.table("daily_metrics").insert({"d": dkey, "alias": alias, "equity": None}).execute()
            return None
        return rows[0].get("equity")
    except Exception:
        return None


"""
-- SQL migrations (run manually)
ALTER TABLE public.account_state
  ADD COLUMN IF NOT EXISTS daily_dd_limit_pct numeric,
  ADD COLUMN IF NOT EXISTS daily_dd_triggered boolean default false,
  ADD COLUMN IF NOT EXISTS daily_high_watermark numeric,
  ADD COLUMN IF NOT EXISTS daily_loss_cap_usd numeric,
  ADD COLUMN IF NOT EXISTS reset_time_utc time default '00:05:00',
  ADD COLUMN IF NOT EXISTS pause_reason text,
  ADD COLUMN IF NOT EXISTS max_positions_total int;

CREATE TABLE IF NOT EXISTS public.daily_metrics (
  id bigserial primary key,
  d date not null,
  alias text not null,
  equity numeric,
  high_watermark numeric,
  created_at timestamptz default now(),
  unique (d, alias)
);
"""
