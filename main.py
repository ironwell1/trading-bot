import os
import time
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import alpaca_trade_api as tradeapi

# ============================================================
# MOMENTUM BOT v1 - SPY/QQQ ONLY
# ============================================================
# Strategy family:
#   1) ORB breakout
#   2) Pullback continuation
#   3) Afternoon continuation
#
# Goal:
#   - always scan during market hours
#   - take a small number of quality trades
#   - avoid lunch chop and major event chaos by simple filters
# ============================================================

# -----------------------------
# CONFIG
# -----------------------------
API_KEY = os.getenv("APCA_API_KEY_ID", "PKSJTVGJZYO7UCP3PO6Q6WBSE4")
API_SECRET = os.getenv("APCA_API_SECRET_KEY", "4VuBmFdgCVoVvA7iuaprqJetZF9Xq3AXY7BmcUHPXQVF")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

SYMBOLS = ["SPY", "QQQ"]

# Risk / behavior
RISK_PER_TRADE = 0.005          # 0.5% of equity
MAX_OPEN_POSITIONS = 2
MAX_TRADES_PER_SYMBOL_PER_DAY = 2
SCAN_INTERVAL_SECONDS = 20
ATR_STOP_MULT = 1.8
TRAIL_STOP_MULT = 1.5

# Time windows (America/New_York)
TZ = ZoneInfo("America/New_York")
ORB_START = (9, 30)
ORB_END = (9, 45)
PULLBACK_START = (9, 50)
PULLBACK_END = (11, 15)
AFTERNOON_START = (13, 30)
AFTERNOON_END = (15, 30)
HARD_FLAT_TIME = (15, 55)

# State files
STATE_FILE = "bot_state.json"
TRADE_LOG_FILE = "trade_log.csv"

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("momentum_bot")

# -----------------------------
# API
# -----------------------------
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

# -----------------------------
# STATE
# -----------------------------
state = {
    "opening_ranges": {},          # symbol -> {"date": "YYYY-MM-DD", "high": x, "low": y}
    "trades_today": {},            # symbol -> {"date": "YYYY-MM-DD", "count": n}
    "entry_prices": {},            # symbol -> float
    "entry_setups": {},            # symbol -> str
    "trailing_stops": {},          # symbol -> float
    "last_signal_bar_time": {},    # symbol+setup -> timestamp string
}

# ============================================================
# PERSISTENCE
# ============================================================

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            log.info("Loaded state.")
        except Exception as e:
            log.warning("Could not load state: %s", e)

def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.warning("Could not save state: %s", e)

def append_trade_log(row: dict):
    df = pd.DataFrame([row])
    if os.path.exists(TRADE_LOG_FILE):
        df.to_csv(TRADE_LOG_FILE, mode="a", index=False, header=False)
    else:
        df.to_csv(TRADE_LOG_FILE, index=False)

# ============================================================
# TIME / SESSION HELPERS
# ============================================================

def now_et() -> datetime:
    return datetime.now(TZ)

def today_str() -> str:
    return now_et().strftime("%Y-%m-%d")

def hhmm_in_range(start_hm, end_hm) -> bool:
    now = now_et()
    cur = now.hour * 60 + now.minute
    start = start_hm[0] * 60 + start_hm[1]
    end = end_hm[0] * 60 + end_hm[1]
    return start <= cur < end

def after_hhmm(hm) -> bool:
    now = now_et()
    cur = now.hour * 60 + now.minute
    mark = hm[0] * 60 + hm[1]
    return cur >= mark

def is_market_open() -> bool:
    try:
        return api.get_clock().is_open
    except Exception as e:
        log.warning("Clock check failed: %s", e)
        return False

# ============================================================
# BROKER / DATA HELPERS
# ============================================================

def get_account():
    return api.get_account()

def get_positions():
    positions = {}
    try:
        for p in api.list_positions():
            positions[p.symbol] = p
    except Exception as e:
        log.warning("Could not list positions: %s", e)
    return positions

def get_open_orders():
    orders = {}
    try:
        for o in api.list_orders(status="open"):
            orders[o.symbol] = o
    except Exception as e:
        log.warning("Could not list open orders: %s", e)
    return orders

def get_bars(symbol: str, timeframe: str = "1Min", limit: int = 200):
    try:
        bars = api.get_bars(symbol, timeframe, limit=limit).df
        if bars is None or bars.empty:
            return None

        # Make sure timestamps are Eastern for time filters
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        return bars
    except Exception as e:
        log.warning("Bars error %s: %s", symbol, e)
        return None

# ============================================================
# INDICATORS
# ============================================================

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-10)
    return 100 - (100 / (1 + rs))

def atr(bars: pd.DataFrame, period: int = 14) -> pd.Series:
    high = bars["high"]
    low = bars["low"]
    close = bars["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def vwap_intraday(bars: pd.DataFrame) -> pd.Series:
    day = today_str()
    session = bars[bars.index.strftime("%Y-%m-%d") == day].copy()
    if session.empty:
        session = bars.copy()
    typical = (session["high"] + session["low"] + session["close"]) / 3
    return (typical * session["volume"]).cumsum() / session["volume"].cumsum()

# ============================================================
# STATE HELPERS
# ============================================================

def reset_daily_state_if_needed(symbol: str):
    day = today_str()

    if symbol not in state["trades_today"] or state["trades_today"][symbol]["date"] != day:
        state["trades_today"][symbol] = {"date": day, "count": 0}

    if symbol not in state["opening_ranges"] or state["opening_ranges"][symbol]["date"] != day:
        state["opening_ranges"][symbol] = {"date": day, "high": None, "low": None}

def trades_left(symbol: str) -> bool:
    reset_daily_state_if_needed(symbol)
    return state["trades_today"][symbol]["count"] < MAX_TRADES_PER_SYMBOL_PER_DAY

def inc_trade_count(symbol: str):
    reset_daily_state_if_needed(symbol)
    state["trades_today"][symbol]["count"] += 1
    save_state()

def already_signaled_this_bar(symbol: str, setup: str, bar_ts: str) -> bool:
    key = f"{symbol}:{setup}"
    return state["last_signal_bar_time"].get(key) == bar_ts

def mark_signal_bar(symbol: str, setup: str, bar_ts: str):
    key = f"{symbol}:{setup}"
    state["last_signal_bar_time"][key] = bar_ts
    save_state()

# ============================================================
# OPENING RANGE
# ============================================================

def update_opening_range(symbol: str, bars: pd.DataFrame):
    reset_daily_state_if_needed(symbol)
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
    state["opening_ranges"][symbol]["low"] = float(orb_slice["low"].min())
    save_state()

# ============================================================
# SETUPS
# ============================================================

def setup_orb(symbol: str, bars: pd.DataFrame):
    if not hhmm_in_range(ORB_END, PULLBACK_END):
        return None

    orb = state["opening_ranges"].get(symbol, {})
    orb_high = orb.get("high")
    if orb_high is None:
        return None

    closes = bars["close"]
    volumes = bars["volume"]
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    atr14 = atr(bars, 14)

    price = float(closes.iloc[-1])
    prev = float(closes.iloc[-2])
    vol_now = float(volumes.iloc[-1])
    vol_avg = float(volumes.tail(20).mean())
    bar_ts = bars.index[-1].isoformat()

    breakout = prev <= orb_high and price > orb_high
    trend_ok = ema9.iloc[-1] > ema21.iloc[-1]
    vol_ok = vol_now > vol_avg * 1.2

    if breakout and trend_ok and vol_ok and not already_signaled_this_bar(symbol, "ORB", bar_ts):
        stop = price - float(atr14.iloc[-1] * ATR_STOP_MULT)
        mark_signal_bar(symbol, "ORB", bar_ts)
        return {
            "setup": "ORB",
            "symbol": symbol,
            "price": price,
            "stop": stop,
            "bar_ts": bar_ts,
        }
    return None

def setup_pullback(symbol: str, bars: pd.DataFrame):
    if not hhmm_in_range(PULLBACK_START, PULLBACK_END):
        return None

    closes = bars["close"]
    highs = bars["high"]
    lows = bars["low"]
    volumes = bars["volume"]

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    atr14 = atr(bars, 14)
    rs = rsi(closes, 14)
    vw = vwap_intraday(bars)

    price = float(closes.iloc[-1])
    bar_ts = bars.index[-1].isoformat()

    trend_ok = ema9.iloc[-1] > ema21.iloc[-1] and price > float(vw.iloc[-1])

    # Pullback resumed upward: previous bar touches zone, current bar reclaims strength
    pullback_zone = (
        float(lows.iloc[-2]) <= max(float(ema9.iloc[-2]), float(ema21.iloc[-2]))
    )
    reclaim = price > float(highs.iloc[-2])
    rsi_ok = 45 <= float(rs.iloc[-1]) <= 68
    vol_ok = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.05

    if trend_ok and pullback_zone and reclaim and rsi_ok and vol_ok and not already_signaled_this_bar(symbol, "PULLBACK", bar_ts):
        stop = min(float(lows.iloc[-2]), price - float(atr14.iloc[-1] * ATR_STOP_MULT))
        mark_signal_bar(symbol, "PULLBACK", bar_ts)
        return {
            "setup": "PULLBACK",
            "symbol": symbol,
            "price": price,
            "stop": stop,
            "bar_ts": bar_ts,
        }
    return None

def setup_afternoon(symbol: str, bars: pd.DataFrame):
    if not hhmm_in_range(AFTERNOON_START, AFTERNOON_END):
        return None

    closes = bars["close"]
    highs = bars["high"]
    volumes = bars["volume"]

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    atr14 = atr(bars, 14)
    vw = vwap_intraday(bars)

    price = float(closes.iloc[-1])
    bar_ts = bars.index[-1].isoformat()

    trend_ok = (
        price > float(vw.iloc[-1]) and
        ema9.iloc[-1] > ema21.iloc[-1] and
        closes.iloc[-1] > closes.iloc[-10]
    )
    mini_breakout = price > float(highs.tail(10).max())
    vol_ok = float(volumes.iloc[-1]) > float(volumes.tail(20).mean()) * 1.1

    if trend_ok and mini_breakout and vol_ok and not already_signaled_this_bar(symbol, "AFTERNOON", bar_ts):
        stop = price - float(atr14.iloc[-1] * ATR_STOP_MULT)
        mark_signal_bar(symbol, "AFTERNOON", bar_ts)
        return {
            "setup": "AFTERNOON",
            "symbol": symbol,
            "price": price,
            "stop": stop,
            "bar_ts": bar_ts,
        }
    return None

# ============================================================
# ORDER / RISK
# ============================================================

def calc_qty(entry: float, stop: float) -> int:
    try:
        account = get_account()
        equity = float(account.equity)
    except Exception:
        return 0

    risk_dollars = equity * RISK_PER_TRADE
    risk_per_share = max(entry - stop, 0.01)
    qty = int(risk_dollars / risk_per_share)

    # Basic notional cap
    max_notional = equity * 0.15
    if qty * entry > max_notional:
        qty = int(max_notional / entry)

    return max(qty, 0)

def can_open_new_position(symbol: str) -> bool:
    positions = get_positions()
    open_orders = get_open_orders()

    if symbol in positions or symbol in open_orders:
        return False
    if len(positions) >= MAX_OPEN_POSITIONS:
        return False
    if not trades_left(symbol):
        return False
    return True

def place_buy(signal: dict):
    symbol = signal["symbol"]
    entry = signal["price"]
    stop = signal["stop"]
    setup = signal["setup"]

    qty = calc_qty(entry, stop)
    if qty <= 0:
        log.info("Skipping %s %s: qty <= 0", symbol, setup)
        return

    try:
        api.submit_order(
            symbol=symbol,
            qty=qty,
            side="buy",
            type="market",
            time_in_force="day"
        )

        state["entry_prices"][symbol] = entry
        state["entry_setups"][symbol] = setup
        state["trailing_stops"][symbol] = stop
        inc_trade_count(symbol)
        save_state()

        log.info("BUY %s x%s @ %.2f | setup=%s | stop=%.2f", symbol, qty, entry, setup, stop)
    except Exception as e:
        log.error("Buy failed %s: %s", symbol, e)

def place_sell(symbol: str, reason: str):
    positions = get_positions()
    pos = positions.get(symbol)
    if not pos:
        return

    setup = state["entry_setups"].get(symbol, "UNKNOWN")
    entry = float(state["entry_prices"].get(symbol, 0))
    last = float(pos.current_price)
    qty = abs(int(float(pos.qty)))
    unreal = float(pos.unrealized_pl)

    try:
        api.submit_order(
            symbol=symbol,
            qty=qty,
            side="sell",
            type="market",
            time_in_force="day"
        )

        append_trade_log({
            "timestamp": now_et().isoformat(),
            "symbol": symbol,
            "setup": setup,
            "entry_price": entry,
            "exit_price_est": last,
            "pnl_unrealized_at_exit": unreal,
            "reason": reason,
        })

        state["entry_prices"].pop(symbol, None)
        state["entry_setups"].pop(symbol, None)
        state["trailing_stops"].pop(symbol, None)
        save_state()

        log.info("SELL %s @ %.2f | setup=%s | reason=%s | unreal=%.2f",
                 symbol, last, setup, reason, unreal)
    except Exception as e:
        log.error("Sell failed %s: %s", symbol, e)

# ============================================================
# POSITION MANAGEMENT
# ============================================================

def manage_position(symbol: str, bars: pd.DataFrame):
    positions = get_positions()
    pos = positions.get(symbol)
    if not pos:
        return

    closes = bars["close"]
    atr14 = atr(bars, 14)

    price = float(closes.iloc[-1])
    atr_now = float(atr14.iloc[-1])
    current_stop = float(state["trailing_stops"].get(symbol, 0))

    # Raise stop only, never lower it
    new_stop = price - atr_now * TRAIL_STOP_MULT
    if new_stop > current_stop:
        state["trailing_stops"][symbol] = new_stop
        save_state()

    if price <= float(state["trailing_stops"].get(symbol, 0)):
        place_sell(symbol, "trailing_stop")
        return

    # End of day flat
    if after_hhmm(HARD_FLAT_TIME):
        place_sell(symbol, "end_of_day")
        return

# ============================================================
# NEWS / EVENT FILTER
# ============================================================

def high_risk_keyword_present(symbol: str) -> bool:
    """
    Very simple filter:
    skip entries if very recent headlines suggest unusual macro/geopolitical chaos.
    This is a FILTER, not a prediction model.
    """
    try:
        news = api.get_news(symbol, limit=5)
        if not news:
            return False

        bad_words = [
            "war", "missile", "attack", "sanction", "oil spike",
            "emergency", "invasion", "retaliation", "explosion"
        ]
        for item in news:
            text = f"{getattr(item, 'headline', '')} {getattr(item, 'summary', '')}".lower()
            if any(word in text for word in bad_words):
                return True
        return False
    except Exception:
        return False

# ============================================================
# SCAN
# ============================================================

def scan_symbol(symbol: str):
    reset_daily_state_if_needed(symbol)

    bars = get_bars(symbol, timeframe="1Min", limit=220)
    if bars is None or len(bars) < 60:
        return

    update_opening_range(symbol, bars)
    manage_position(symbol, bars)

    if not can_open_new_position(symbol):
        return

    if high_risk_keyword_present(symbol):
        log.info("Skipping %s due to high-risk headline filter", symbol)
        return

    for setup_fn in (setup_orb, setup_pullback, setup_afternoon):
        signal = setup_fn(symbol, bars)
        if signal:
            log.info("Signal %s | %s @ %.2f", signal["setup"], symbol, signal["price"])
            place_buy(signal)
            return

# ============================================================
# MAIN LOOP
# ============================================================

def print_status():
    try:
        account = get_account()
        positions = get_positions()
        log.info(
            "STATUS | equity=%s cash=%s open_positions=%d market_open=%s",
            account.equity,
            account.cash,
            len(positions),
            is_market_open()
        )
        for sym, pos in positions.items():
            log.info(
                "POS | %s qty=%s current=%s unreal=%s stop=%.2f",
                sym,
                pos.qty,
                pos.current_price,
                pos.unrealized_pl,
                float(state["trailing_stops"].get(sym, 0))
            )
    except Exception as e:
        log.warning("Status error: %s", e)

def run():
    load_state()
    log.info("Momentum Bot v1 started | symbols=%s | base_url=%s", SYMBOLS, BASE_URL)

    while True:
        try:
            if not is_market_open():
                log.info("Market closed. Sleeping 60s.")
                time.sleep(60)
                continue

            for symbol in SYMBOLS:
                try:
                    scan_symbol(symbol)
                except Exception as e:
                    log.error("Scan failed for %s: %s", symbol, e)

            print_status()
            time.sleep(SCAN_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            log.info("Stopped manually.")
            break
        except Exception as e:
            log.error("Main loop error: %s", e)
            time.sleep(30)

if __name__ == "__main__":
    run()
