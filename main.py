import os
import time
import json
import logging
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import alpaca_trade_api as tradeapi


# ============================================================
# RESEARCH-BACKED INTRADAY MOMENTUM BOT v2
# SPY + QQQ | LONG + SHORT | RAILWAY-READY
# ============================================================
#
# Key upgrades from v1:
#   1) VWAP trailing stop floor (replaces pure ATR trail)
#   2) VIX regime filter on every scan
#   3) Dynamic position sizing vs daily volatility
#   4) Short side: VWAP breakdown setups
#   5) Premarket gap bias filter (morning window)
#   6) 5-min bars for signals, 1-min for position management
#   7) MAX_TRADES raised to 4/symbol/day
#   8) COOLDOWN reduced to 5 min
#   9) Noise Area breakout filter (academic)
#  10) Semi-hourly entry timing gate
#
# Deploy: push to GitHub -> connect Railway -> set env vars
# ============================================================


# ============================================================
# CONFIG
# ============================================================
API_KEY = os.getenv("APCA_API_KEY_ID", "PKSJTVGJZYO7UCP3PO6Q6WBSE4")
API_SECRET = os.getenv("APCA_API_SECRET_KEY", "4VuBmFdgCVoVvA7iuaprqJetZF9Xq3AXY7BmcUHPXQVF")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "Missing Alpaca API credentials. "
        "Set APCA_API_KEY_ID and APCA_API_SECRET_KEY in Railway environment variables."
    )

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

TZ = ZoneInfo("America/New_York")

SYMBOLS = ["SPY", "QQQ"]

# ── Risk / Execution ──────────────────────────────────────
BASE_RISK_PER_TRADE     = 0.006      # 0.6% of equity base risk (scaled dynamically)
MAX_OPEN_POSITIONS      = 4          # 2 long + 2 short possible
MAX_TRADES_PER_SYMBOL   = 4          # per day per symbol (up from 2)
SCAN_INTERVAL_SECONDS   = 30

ATR_STOP_MULT           = 1.5
TRAIL_STOP_MULT         = 1.2
VWAP_TRAIL_BUFFER       = 0.0012     # 0.12% below VWAP as trail floor
MAX_NOTIONAL_PCT        = 0.20
MIN_STOP_DISTANCE_PCT   = 0.0020
COOLDOWN_MINUTES        = 5          # reduced from 10

TARGET_DAILY_VOL        = 0.018      # 1.8% daily vol target for dynamic sizing

# ── VIX Regime Thresholds ─────────────────────────────────
VIX_DEAD_BELOW          = 12         # skip trading entirely
VIX_NORMAL_LOW          = 12
VIX_NORMAL_HIGH         = 35
VIX_HIGH_LOW            = 35
VIX_EXTREME             = 50         # cut size aggressively, widen stops

# ── Time Windows (ET) ─────────────────────────────────────
ORB_START       = (9, 30)
ORB_END         = (9, 45)

MORNING_START   = (9, 45)
MORNING_END     = (11, 30)

AFTERNOON_START = (13, 30)
AFTERNOON_END   = (15, 20)

HARD_FLAT_TIME  = (15, 50)

LUNCH_START     = (11, 30)
LUNCH_END       = (13, 15)

# ── Persistence ───────────────────────────────────────────
STATE_FILE     = "bot_state.json"
TRADE_LOG_FILE = "trade_log.csv"


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("bot_v2")


# ============================================================
# STATE
# ============================================================
DEFAULT_STATE = {
    "opening_ranges":       {},   # symbol -> {date, high, low}
    "premarket_gaps":       {},   # symbol -> {date, gap_pct, bias}  "up"/"down"/"flat"
    "trades_today":         {},   # symbol -> {date, count}
    "entry_prices":         {},   # symbol -> float
    "entry_sides":          {},   # symbol -> "long" | "short"
    "entry_setups":         {},   # symbol -> str
    "trailing_stops":       {},   # symbol -> float
    "last_signal_bar_time": {},   # "SYMBOL:SETUP" -> iso ts
    "cooldowns":            {},   # symbol -> iso ts of last exit
    "daily_atr_baseline":   {},   # symbol -> float (14-day avg ATR for sizing)
}

state = DEFAULT_STATE.copy()


# ============================================================
# PERSISTENCE
# ============================================================
def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            log.info("State loaded from disk.")
        except Exception as e:
            log.warning("Could not load state: %s. Starting fresh.", e)
            state = DEFAULT_STATE.copy()
    else:
        state = DEFAULT_STATE.copy()


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.warning("State save failed: %s", e)


def append_trade_log(row: dict):
    df = pd.DataFrame([row])
    if os.path.exists(TRADE_LOG_FILE):
        df.to_csv(TRADE_LOG_FILE, mode="a", index=False, header=False)
    else:
        df.to_csv(TRADE_LOG_FILE, index=False)


# ============================================================
# TIME HELPERS
# ============================================================
def now_et() -> datetime:
    return datetime.now(TZ)


def today_str() -> str:
    return now_et().strftime("%Y-%m-%d")


def current_minutes_et() -> int:
    n = now_et()
    return n.hour * 60 + n.minute


def hm_to_min(hm: tuple) -> int:
    return hm[0] * 60 + hm[1]


def in_window(start_hm: tuple, end_hm: tuple) -> bool:
    cur = current_minutes_et()
    return hm_to_min(start_hm) <= cur < hm_to_min(end_hm)


def after_hhmm(hm: tuple) -> bool:
    return current_minutes_et() >= hm_to_min(hm)


def is_semi_hourly_gate() -> bool:
    """
    Research finding: only enter at :00 or :30 past the hour.
    Allows a 3-minute window either side to accommodate scan timing.
    """
    minute = now_et().minute
    return minute <= 3 or (30 <= minute <= 33)


def is_market_open() -> bool:
    try:
        return api.get_clock().is_open
    except Exception as e:
        log.warning("Clock check failed: %s", e)
        return False


# ============================================================
# BROKER HELPERS
# ============================================================
def get_account():
    return api.get_account()


def get_positions() -> dict:
    positions = {}
    try:
        for p in api.list_positions():
            positions[p.symbol] = p
    except Exception as e:
        log.warning("list_positions failed: %s", e)
    return positions


def get_open_orders() -> dict:
    orders = {}
    try:
        for o in api.list_orders(status="open"):
            orders[o.symbol] = o
    except Exception as e:
        log.warning("list_orders failed: %s", e)
    return orders


def get_bars(symbol: str, timeframe: str = "5Min", limit: int = 120) -> pd.DataFrame | None:
    try:
        bars = api.get_bars(symbol, timeframe, limit=limit).df
        if bars is None or bars.empty:
            return None
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        return bars.sort_index()
    except Exception as e:
        log.warning("get_bars %s %s: %s", symbol, timeframe, e)
        return None


def get_latest_quote(symbol: str) -> float | None:
    """Fetch latest trade price for premarket gap calc."""
    try:
        trade = api.get_latest_trade(symbol)
        return float(trade.price)
    except Exception as e:
        log.warning("get_latest_quote %s: %s", symbol, e)
        return None


# ============================================================
# VIX FETCH
# ============================================================
def get_vix() -> float | None:
    """
    Fetch VIX via Yahoo Finance as a fallback (no extra API key needed).
    Returns float or None if unavailable.
    """
    try:
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX"
            "?interval=1d&range=1d"
        )
        resp = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        vix = next((v for v in reversed(closes) if v is not None), None)
        return float(vix) if vix else None
    except Exception as e:
        log.warning("VIX fetch failed: %s", e)
        return None


_vix_cache = {"value": None, "fetched_at": None}


def get_vix_cached() -> float | None:
    """Cache VIX for 5 minutes to avoid hammering Yahoo."""
    now = time.time()
    if _vix_cache["fetched_at"] and (now - _vix_cache["fetched_at"]) < 300:
        return _vix_cache["value"]
    vix = get_vix()
    _vix_cache["value"] = vix
    _vix_cache["fetched_at"] = now
    return vix


def vix_regime(vix: float | None) -> str:
    """Returns 'dead' | 'normal' | 'high' | 'extreme'"""
    if vix is None:
        return "normal"   # default to normal if unavailable
    if vix < VIX_DEAD_BELOW:
        return "dead"
    if vix <= VIX_NORMAL_HIGH:
        return "normal"
    if vix <= VIX_EXTREME:
        return "high"
    return "extreme"


# ============================================================
# INDICATORS
# ============================================================
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    rs = gain / (loss + 1e-10)
    return 100 - (100 / (1 + rs))


def atr(bars: pd.DataFrame, period: int = 14) -> pd.Series:
    high  = bars["high"]
    low   = bars["low"]
    close = bars["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def vwap_intraday(bars: pd.DataFrame) -> pd.Series:
    day     = today_str()
    session = bars[bars.index.strftime("%Y-%m-%d") == day].copy()
    if session.empty:
        session = bars.copy()
    typical = (session["high"] + session["low"] + session["close"]) / 3
    return (typical * session["volume"]).cumsum() / session["volume"].cumsum()


def noise_area(bars: pd.DataFrame, lookback: int = 10) -> tuple:
    """
    Noise Area: mean ± 1 std of recent closes.
    Breakout above/below = genuine signal (not noise).
    Returns (upper_bound, lower_bound).
    """
    recent = bars["close"].tail(lookback)
    m = recent.mean()
    s = recent.std()
    return (m + s, m - s)


# ============================================================
# STATE HELPERS
# ============================================================
def reset_daily_if_needed(symbol: str):
    day = today_str()
    if symbol not in state["trades_today"] or state["trades_today"][symbol]["date"] != day:
        state["trades_today"][symbol] = {"date": day, "count": 0}
    if symbol not in state["opening_ranges"] or state["opening_ranges"][symbol]["date"] != day:
        state["opening_ranges"][symbol] = {"date": day, "high": None, "low": None}
    if symbol not in state["premarket_gaps"] or state["premarket_gaps"][symbol]["date"] != day:
        state["premarket_gaps"][symbol] = {"date": day, "gap_pct": 0.0, "bias": "flat"}


def trades_left(symbol: str) -> bool:
    reset_daily_if_needed(symbol)
    return state["trades_today"][symbol]["count"] < MAX_TRADES_PER_SYMBOL


def increment_trade_count(symbol: str):
    reset_daily_if_needed(symbol)
    state["trades_today"][symbol]["count"] += 1
    save_state()


def signal_key(symbol: str, setup: str) -> str:
    return f"{symbol}:{setup}"


def already_signaled_this_bar(symbol: str, setup: str, bar_ts: str) -> bool:
    return state["last_signal_bar_time"].get(signal_key(symbol, setup)) == bar_ts


def mark_signal_bar(symbol: str, setup: str, bar_ts: str):
    state["last_signal_bar_time"][signal_key(symbol, setup)] = bar_ts
    save_state()


def on_cooldown(symbol: str) -> bool:
    ts = state["cooldowns"].get(symbol)
    if not ts:
        return False
    try:
        elapsed = (now_et() - datetime.fromisoformat(ts)).total_seconds() / 60
        return elapsed < COOLDOWN_MINUTES
    except Exception:
        return False


def set_cooldown(symbol: str):
    state["cooldowns"][symbol] = now_et().isoformat()
    save_state()


# ============================================================
# PREMARKET GAP BIAS
# ============================================================
def update_premarket_gap(symbol: str):
    """
    Compares prior session close to current premarket price.
    Bias: 'up' if gap > +0.2%, 'down' if gap < -0.2%, else 'flat'.
    Only runs once per day.
    """
    reset_daily_if_needed(symbol)
    if state["premarket_gaps"][symbol]["bias"] != "flat":
        return  # already computed today

    try:
        # Prior close: use last bar from yesterday
        daily = api.get_bars(symbol, "1Day", limit=2).df
        if daily is None or len(daily) < 2:
            return
        if daily.index.tz is None:
            daily.index = daily.index.tz_localize("UTC").tz_convert(TZ)
        else:
            daily.index = daily.index.tz_convert(TZ)

        prior_close = float(daily["close"].iloc[-2])
        current_price = get_latest_quote(symbol)
        if not current_price or prior_close <= 0:
            return

        gap_pct = (current_price - prior_close) / prior_close
        if gap_pct > 0.002:
            bias = "up"
        elif gap_pct < -0.002:
            bias = "down"
        else:
            bias = "flat"

        state["premarket_gaps"][symbol] = {
            "date": today_str(),
            "gap_pct": round(gap_pct, 5),
            "bias": bias,
        }
        save_state()
        log.info("GAP %s | gap_pct=%.3f%% | bias=%s", symbol, gap_pct * 100, bias)
    except Exception as e:
        log.warning("Gap calc failed %s: %s", symbol, e)


def get_gap_bias(symbol: str) -> str:
    return state["premarket_gaps"].get(symbol, {}).get("bias", "flat")


# ============================================================
# OPENING RANGE
# ============================================================
def update_opening_range(symbol: str, bars: pd.DataFrame):
    reset_daily_if_needed(symbol)
    day = today_str()
    today_bars = bars[bars.index.strftime("%Y-%m-%d") == day]
    if today_bars.empty:
        return
    orb_slice = today_bars.between_time(
        f"{ORB_START[0]:02d}:{ORB_START[1]:02d}",
        f"{ORB_END[0]:02d}:{ORB_END[1]:02d}"
    )
    if orb_slice.empty:
        return
    state["opening_ranges"][symbol]["high"] = float(orb_slice["high"].max())
    state["opening_ranges"][symbol]["low"]  = float(orb_slice["low"].min())
    save_state()


# ============================================================
# DYNAMIC POSITION SIZING
# ============================================================
def calc_qty(symbol: str, entry: float, stop: float, vix_val: float | None) -> int:
    """
    Dynamic sizing:
    - Base risk scaled by (target_daily_vol / current_daily_atr_pct)
    - Further scaled down if VIX is high/extreme
    - Hard notional cap still applies
    """
    try:
        account = get_account()
        equity  = float(account.equity)
    except Exception:
        return 0

    # Dynamic risk scaling
    atr_baseline = state["daily_atr_baseline"].get(symbol, None)
    risk_pct = BASE_RISK_PER_TRADE
    if atr_baseline and atr_baseline > 0 and entry > 0:
        current_atr_pct = atr_baseline / entry
        if current_atr_pct > 0:
            scale = TARGET_DAILY_VOL / current_atr_pct
            scale = max(0.5, min(scale, 2.0))  # cap between 0.5x – 2x
            risk_pct = BASE_RISK_PER_TRADE * scale

    # VIX regime size reduction
    regime = vix_regime(vix_val)
    if regime == "high":
        risk_pct *= 0.65
    elif regime == "extreme":
        risk_pct *= 0.40

    raw_stop_dist = abs(entry - stop)
    min_stop_dist = entry * MIN_STOP_DISTANCE_PCT
    risk_per_share = max(raw_stop_dist, min_stop_dist, 0.01)

    risk_dollars = equity * risk_pct
    qty = int(risk_dollars / risk_per_share)

    max_notional = equity * MAX_NOTIONAL_PCT
    if qty * entry > max_notional:
        qty = int(max_notional / entry)

    return max(qty, 0)


# ============================================================
# ENVIRONMENT CHECKS
# ============================================================
def setup_environment_ok(bars: pd.DataFrame, vix_val: float | None) -> bool:
    if bars is None or len(bars) < 30:
        return False
    regime = vix_regime(vix_val)
    if regime == "dead":
        log.info("VIX too low (%.1f) – skipping trades (dead tape).", vix_val or 0)
        return False
    closes  = bars["close"]
    atr14   = atr(bars, 14)
    price   = float(closes.iloc[-1])
    atr_pct = float(atr14.iloc[-1]) / max(price, 0.01)
    return atr_pct >= 0.0010


def can_open_new_position(symbol: str) -> bool:
    positions   = get_positions()
    open_orders = get_open_orders()
    if symbol in positions or symbol in open_orders:
        return False
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False
    if not trades_left(symbol):
        return False
    if on_cooldown(symbol):
        return False
    return True


# ============================================================
# SETUPS — LONG
# ============================================================
def setup_orb_long(symbol: str, bars: pd.DataFrame) -> dict | None:
    """
    Opening Range Breakout (Long):
    - Price breaks above ORB high
    - Gap bias is 'up' or 'flat' (not fighting a gap-down day)
    - Above VWAP, EMA9 > EMA21
    - Noise Area breakout confirmed
    - Semi-hourly gate
    """
    if not in_window(ORB_END, MORNING_END):
        return None
    if not is_semi_hourly_gate():
        return None

    orb = state["opening_ranges"].get(symbol, {})
    orb_high = orb.get("high")
    if orb_high is None:
        return None

    bias = get_gap_bias(symbol)
    if bias == "down":
        return None  # don't fight a gap-down morning

    closes   = bars["close"]
    volumes  = bars["volume"]
    ema9     = ema(closes, 9)
    ema21    = ema(closes, 21)
    atr14    = atr(bars, 14)
    vw       = vwap_intraday(bars)
    na_upper, _ = noise_area(bars, 10)

    price      = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2])
    vol_now    = float(volumes.iloc[-1])
    vol_avg    = float(volumes.tail(20).mean())
    bar_ts     = bars.index[-1].isoformat()

    breakout  = prev_close <= orb_high and price > orb_high
    trend_ok  = float(ema9.iloc[-1]) > float(ema21.iloc[-1]) and price > float(vw.iloc[-1])
    vol_ok    = vol_now >= vol_avg * 1.05
    noise_ok  = price > na_upper

    if breakout and trend_ok and vol_ok and noise_ok and not already_signaled_this_bar(symbol, "ORB_L", bar_ts):
        stop = min(
            price - float(atr14.iloc[-1]) * ATR_STOP_MULT,
            orb_high - float(atr14.iloc[-1]) * 0.25
        )
        mark_signal_bar(symbol, "ORB_L", bar_ts)
        return {"setup": "ORB_L", "symbol": symbol, "side": "long", "price": price, "stop": stop}
    return None


def setup_pullback_long(symbol: str, bars: pd.DataFrame) -> dict | None:
    """
    Pullback Continuation (Long):
    - In morning window
    - EMA9 > EMA21, above VWAP
    - Prior bar dipped into EMA zone, current bar reclaims
    - RSI 40–75 (widened from v1)
    - Gap bias not 'down'
    """
    if not in_window(MORNING_START, MORNING_END):
        return None
    if not is_semi_hourly_gate():
        return None
    if get_gap_bias(symbol) == "down":
        return None

    closes  = bars["close"]
    highs   = bars["high"]
    lows    = bars["low"]
    volumes = bars["volume"]

    ema9   = ema(closes, 9)
    ema21  = ema(closes, 21)
    rs     = rsi(closes, 14)
    atr14  = atr(bars, 14)
    vw     = vwap_intraday(bars)

    price  = float(closes.iloc[-1])
    bar_ts = bars.index[-1].isoformat()

    trend_ok       = float(ema9.iloc[-1]) > float(ema21.iloc[-1]) and price > float(vw.iloc[-1])
    pullback_zone  = float(lows.iloc[-2]) <= max(float(ema9.iloc[-2]), float(ema21.iloc[-2]))
    reclaim        = price > float(highs.iloc[-2])
    rsi_ok         = 40 <= float(rs.iloc[-1]) <= 75      # widened
    vol_ok         = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.02

    if trend_ok and pullback_zone and reclaim and rsi_ok and vol_ok and not already_signaled_this_bar(symbol, "PB_L", bar_ts):
        stop = min(
            float(lows.iloc[-2]),
            price - float(atr14.iloc[-1]) * ATR_STOP_MULT
        )
        mark_signal_bar(symbol, "PB_L", bar_ts)
        return {"setup": "PB_L", "symbol": symbol, "side": "long", "price": price, "stop": stop}
    return None


def setup_afternoon_long(symbol: str, bars: pd.DataFrame) -> dict | None:
    """
    Afternoon Continuation (Long):
    - Post-lunch window
    - Trend above VWAP, EMA9 > EMA21
    - Breaks above recent 10-bar high
    - Volume spike
    - Noise Area breakout
    """
    if not in_window(AFTERNOON_START, AFTERNOON_END):
        return None
    if not is_semi_hourly_gate():
        return None

    closes  = bars["close"]
    highs   = bars["high"]
    volumes = bars["volume"]

    ema9   = ema(closes, 9)
    ema21  = ema(closes, 21)
    atr14  = atr(bars, 14)
    vw     = vwap_intraday(bars)
    na_upper, _ = noise_area(bars, 10)

    price       = float(closes.iloc[-1])
    recent_high = float(highs.iloc[-11:-1].max())
    bar_ts      = bars.index[-1].isoformat()

    trend_ok = (
        price > float(vw.iloc[-1]) and
        float(ema9.iloc[-1]) > float(ema21.iloc[-1]) and
        float(closes.iloc[-1]) > float(closes.iloc[-10])
    )
    breakout = price > recent_high and price > na_upper
    vol_ok   = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.06

    if trend_ok and breakout and vol_ok and not already_signaled_this_bar(symbol, "AFT_L", bar_ts):
        stop = price - float(atr14.iloc[-1]) * ATR_STOP_MULT
        mark_signal_bar(symbol, "AFT_L", bar_ts)
        return {"setup": "AFT_L", "symbol": symbol, "side": "long", "price": price, "stop": stop}
    return None


# ============================================================
# SETUPS — SHORT
# ============================================================
def setup_orb_short(symbol: str, bars: pd.DataFrame) -> dict | None:
    """
    Opening Range Breakdown (Short):
    - Price breaks below ORB low
    - Gap bias 'down' or 'flat'
    - Below VWAP, EMA9 < EMA21
    - Noise Area breakdown
    """
    if not in_window(ORB_END, MORNING_END):
        return None
    if not is_semi_hourly_gate():
        return None

    orb    = state["opening_ranges"].get(symbol, {})
    orb_low = orb.get("low")
    if orb_low is None:
        return None

    bias = get_gap_bias(symbol)
    if bias == "up":
        return None

    closes  = bars["close"]
    volumes = bars["volume"]
    ema9    = ema(closes, 9)
    ema21   = ema(closes, 21)
    atr14   = atr(bars, 14)
    vw      = vwap_intraday(bars)
    _, na_lower = noise_area(bars, 10)

    price      = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2])
    vol_now    = float(volumes.iloc[-1])
    vol_avg    = float(volumes.tail(20).mean())
    bar_ts     = bars.index[-1].isoformat()

    breakdown = prev_close >= orb_low and price < orb_low
    trend_ok  = float(ema9.iloc[-1]) < float(ema21.iloc[-1]) and price < float(vw.iloc[-1])
    vol_ok    = vol_now >= vol_avg * 1.05
    noise_ok  = price < na_lower

    if breakdown and trend_ok and vol_ok and noise_ok and not already_signaled_this_bar(symbol, "ORB_S", bar_ts):
        stop = max(
            price + float(atr14.iloc[-1]) * ATR_STOP_MULT,
            orb_low + float(atr14.iloc[-1]) * 0.25
        )
        mark_signal_bar(symbol, "ORB_S", bar_ts)
        return {"setup": "ORB_S", "symbol": symbol, "side": "short", "price": price, "stop": stop}
    return None


def setup_pullback_short(symbol: str, bars: pd.DataFrame) -> dict | None:
    """
    Pullback Continuation (Short):
    - Morning window
    - EMA9 < EMA21, below VWAP
    - Prior bar bounced into EMA zone, current bar rolls back below
    - RSI 28–62
    - Gap bias not 'up'
    """
    if not in_window(MORNING_START, MORNING_END):
        return None
    if not is_semi_hourly_gate():
        return None
    if get_gap_bias(symbol) == "up":
        return None

    closes  = bars["close"]
    lows    = bars["low"]
    highs   = bars["high"]
    volumes = bars["volume"]

    ema9   = ema(closes, 9)
    ema21  = ema(closes, 21)
    rs     = rsi(closes, 14)
    atr14  = atr(bars, 14)
    vw     = vwap_intraday(bars)

    price  = float(closes.iloc[-1])
    bar_ts = bars.index[-1].isoformat()

    trend_ok      = float(ema9.iloc[-1]) < float(ema21.iloc[-1]) and price < float(vw.iloc[-1])
    pullback_zone = float(highs.iloc[-2]) >= min(float(ema9.iloc[-2]), float(ema21.iloc[-2]))
    break_down    = price < float(lows.iloc[-2])
    rsi_ok        = 28 <= float(rs.iloc[-1]) <= 62
    vol_ok        = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.02

    if trend_ok and pullback_zone and break_down and rsi_ok and vol_ok and not already_signaled_this_bar(symbol, "PB_S", bar_ts):
        stop = max(
            float(highs.iloc[-2]),
            price + float(atr14.iloc[-1]) * ATR_STOP_MULT
        )
        mark_signal_bar(symbol, "PB_S", bar_ts)
        return {"setup": "PB_S", "symbol": symbol, "side": "short", "price": price, "stop": stop}
    return None


def setup_afternoon_short(symbol: str, bars: pd.DataFrame) -> dict | None:
    """
    Afternoon Breakdown (Short):
    - Post-lunch
    - Below VWAP, EMA9 < EMA21
    - Breaks below recent 10-bar low
    - Volume spike
    """
    if not in_window(AFTERNOON_START, AFTERNOON_END):
        return None
    if not is_semi_hourly_gate():
        return None

    closes  = bars["close"]
    lows    = bars["low"]
    volumes = bars["volume"]

    ema9   = ema(closes, 9)
    ema21  = ema(closes, 21)
    atr14  = atr(bars, 14)
    vw     = vwap_intraday(bars)
    _, na_lower = noise_area(bars, 10)

    price      = float(closes.iloc[-1])
    recent_low = float(lows.iloc[-11:-1].min())
    bar_ts     = bars.index[-1].isoformat()

    trend_ok  = (
        price < float(vw.iloc[-1]) and
        float(ema9.iloc[-1]) < float(ema21.iloc[-1]) and
        float(closes.iloc[-1]) < float(closes.iloc[-10])
    )
    breakdown = price < recent_low and price < na_lower
    vol_ok    = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.06

    if trend_ok and breakdown and vol_ok and not already_signaled_this_bar(symbol, "AFT_S", bar_ts):
        stop = price + float(atr14.iloc[-1]) * ATR_STOP_MULT
        mark_signal_bar(symbol, "AFT_S", bar_ts)
        return {"setup": "AFT_S", "symbol": symbol, "side": "short", "price": price, "stop": stop}
    return None


# ============================================================
# ORDER EXECUTION
# ============================================================
def place_order(signal: dict, vix_val: float | None):
    symbol = signal["symbol"]
    entry  = signal["price"]
    stop   = signal["stop"]
    setup  = signal["setup"]
    side   = signal["side"]

    qty = calc_qty(symbol, entry, stop, vix_val)
    if qty <= 0:
        log.info("Skipping %s %s: qty=0", symbol, setup)
        return

    order_side = "buy" if side == "long" else "sell"

    try:
        api.submit_order(
            symbol=symbol,
            qty=qty,
            side=order_side,
            type="market",
            time_in_force="day"
        )

        state["entry_prices"][symbol]   = entry
        state["entry_sides"][symbol]    = side
        state["entry_setups"][symbol]   = setup
        state["trailing_stops"][symbol] = stop
        increment_trade_count(symbol)
        save_state()

        log.info(
            "ORDER %s %s x%s @ %.2f | setup=%s | stop=%.2f",
            order_side.upper(), symbol, qty, entry, setup, stop
        )
    except Exception as e:
        log.error("Order failed %s %s: %s", side, symbol, e)


def place_exit(symbol: str, reason: str):
    positions = get_positions()
    pos = positions.get(symbol)
    if not pos:
        state["entry_prices"].pop(symbol, None)
        state["entry_sides"].pop(symbol, None)
        state["entry_setups"].pop(symbol, None)
        state["trailing_stops"].pop(symbol, None)
        return

    side  = state["entry_sides"].get(symbol, "long")
    setup = state["entry_setups"].get(symbol, "UNKNOWN")
    entry = float(state["entry_prices"].get(symbol, 0))
    last  = float(pos.current_price)
    qty   = abs(int(float(pos.qty)))
    unreal = float(pos.unrealized_pl)

    exit_side = "sell" if side == "long" else "buy"

    try:
        api.submit_order(
            symbol=symbol,
            qty=qty,
            side=exit_side,
            type="market",
            time_in_force="day"
        )

        append_trade_log({
            "timestamp": now_et().isoformat(),
            "symbol":    symbol,
            "setup":     setup,
            "side":      side,
            "entry":     entry,
            "exit_est":  last,
            "unreal_pnl": unreal,
            "reason":    reason,
        })

        state["entry_prices"].pop(symbol, None)
        state["entry_sides"].pop(symbol, None)
        state["entry_setups"].pop(symbol, None)
        state["trailing_stops"].pop(symbol, None)
        set_cooldown(symbol)
        save_state()

        log.info(
            "EXIT %s %s @ %.2f | setup=%s | reason=%s | pnl=%.2f",
            exit_side.upper(), symbol, last, setup, reason, unreal
        )
    except Exception as e:
        log.error("Exit failed %s: %s", symbol, e)


# ============================================================
# POSITION MANAGEMENT — VWAP TRAIL FLOOR
# ============================================================
def manage_position(symbol: str, bars: pd.DataFrame):
    positions = get_positions()
    pos = positions.get(symbol)
    if not pos:
        return

    closes  = bars["close"]
    ema9    = ema(closes, 9)
    atr14   = atr(bars, 14)
    vw      = vwap_intraday(bars)

    price     = float(closes.iloc[-1])
    atr_now   = float(atr14.iloc[-1])
    vwap_now  = float(vw.iloc[-1])
    cur_stop  = float(state["trailing_stops"].get(symbol, 0))
    side      = state["entry_sides"].get(symbol, "long")
    setup     = state["entry_setups"].get(symbol, "")

    # ── End of day flat ───────────────────────────────────
    if after_hhmm(HARD_FLAT_TIME):
        place_exit(symbol, "end_of_day")
        return

    if side == "long":
        # VWAP trail floor: stop never below (VWAP - buffer)
        vwap_floor    = vwap_now * (1 - VWAP_TRAIL_BUFFER)
        atr_trail     = price - atr_now * TRAIL_STOP_MULT
        new_stop      = max(atr_trail, vwap_floor)  # use whichever is higher

        if new_stop > cur_stop:
            state["trailing_stops"][symbol] = new_stop
            save_state()

        # Hard stop hit
        if price <= float(state["trailing_stops"].get(symbol, 0)):
            place_exit(symbol, "trailing_stop")
            return

        # Setup-specific soft exits
        if setup in ("ORB_L", "PB_L"):
            if price < vwap_now and price < float(ema9.iloc[-1]):
                place_exit(symbol, "lost_vwap_ema9")
                return
        if setup == "AFT_L":
            if price < vwap_now:
                place_exit(symbol, "lost_vwap")
                return

    elif side == "short":
        # Mirror for shorts
        vwap_ceil = vwap_now * (1 + VWAP_TRAIL_BUFFER)
        atr_trail  = price + atr_now * TRAIL_STOP_MULT
        new_stop   = min(atr_trail, vwap_ceil)  # lower stop for shorts

        if new_stop < cur_stop or cur_stop == 0:
            state["trailing_stops"][symbol] = new_stop
            save_state()

        if price >= float(state["trailing_stops"].get(symbol, 0)):
            place_exit(symbol, "trailing_stop")
            return

        if setup in ("ORB_S", "PB_S"):
            if price > vwap_now and price > float(ema9.iloc[-1]):
                place_exit(symbol, "lost_vwap_ema9")
                return
        if setup == "AFT_S":
            if price > vwap_now:
                place_exit(symbol, "lost_vwap")
                return


# ============================================================
# DAILY ATR BASELINE (for dynamic sizing)
# ============================================================
def update_atr_baseline(symbol: str, bars: pd.DataFrame):
    """Store rolling ATR for dynamic position sizing."""
    atr14 = atr(bars, 14)
    if not atr14.isna().iloc[-1]:
        state["daily_atr_baseline"][symbol] = float(atr14.iloc[-1])


# ============================================================
# SCAN LOOP
# ============================================================
def scan_symbol(symbol: str, vix_val: float | None):
    reset_daily_if_needed(symbol)

    # Update premarket gap once per day before market open / at open
    if in_window((9, 28), (9, 50)):
        update_premarket_gap(symbol)

    # Fetch 5-min bars for signals
    bars5 = get_bars(symbol, timeframe="5Min", limit=120)
    if bars5 is None or len(bars5) < 30:
        return

    if not setup_environment_ok(bars5, vix_val):
        return

    update_opening_range(symbol, bars5)
    update_atr_baseline(symbol, bars5)

    # Fetch 1-min bars for tighter position management
    bars1 = get_bars(symbol, timeframe="1Min", limit=60)
    if bars1 is not None and len(bars1) >= 15:
        manage_position(symbol, bars1)
    else:
        manage_position(symbol, bars5)

    # Skip new entries during lunch chop
    if in_window(LUNCH_START, LUNCH_END):
        return

    if not can_open_new_position(symbol):
        return

    # Try all setups, take first signal
    long_setups  = [setup_orb_long, setup_pullback_long, setup_afternoon_long]
    short_setups = [setup_orb_short, setup_pullback_short, setup_afternoon_short]

    for fn in long_setups + short_setups:
        signal = fn(symbol, bars5)
        if signal:
            log.info(
                "SIGNAL %s | %s %s @ %.2f stop=%.2f",
                signal["setup"], signal["side"].upper(),
                symbol, signal["price"], signal["stop"]
            )
            place_order(signal, vix_val)
            return


# ============================================================
# STATUS
# ============================================================
def print_status(vix_val: float | None):
    try:
        account   = get_account()
        positions = get_positions()
        regime    = vix_regime(vix_val)

        log.info(
            "STATUS | equity=%s | cash=%s | open=%d | vix=%.1f (%s) | market=%s",
            account.equity,
            account.cash,
            len(positions),
            vix_val or 0,
            regime,
            is_market_open()
        )

        for sym, pos in positions.items():
            gap   = state["premarket_gaps"].get(sym, {}).get("bias", "?")
            log.info(
                "POS | %s | side=%s | qty=%s | price=%s | unreal=%s | stop=%.2f | setup=%s | gap=%s",
                sym,
                state["entry_sides"].get(sym, "?"),
                pos.qty,
                pos.current_price,
                pos.unrealized_pl,
                float(state["trailing_stops"].get(sym, 0)),
                state["entry_setups"].get(sym, "?"),
                gap,
            )
    except Exception as e:
        log.warning("Status error: %s", e)


# ============================================================
# MAIN LOOP
# ============================================================
def run():
    load_state()
    log.info(
        "Intraday Momentum Bot v2 | symbols=%s | base_url=%s",
        SYMBOLS, BASE_URL
    )

    while True:
        try:
            if not is_market_open():
                log.info("Market closed. Sleeping 60s.")
                time.sleep(60)
                continue

            vix_val = get_vix_cached()

            for symbol in SYMBOLS:
                try:
                    scan_symbol(symbol, vix_val)
                except Exception as e:
                    log.error("Scan error %s: %s", symbol, e)

            print_status(vix_val)
            time.sleep(SCAN_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            log.info("Bot stopped manually.")
            break
        except Exception as e:
            log.error("Main loop error: %s", e)
            time.sleep(30)


if __name__ == "__main__":
    run()
