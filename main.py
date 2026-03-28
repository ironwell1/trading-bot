import os
import time
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import alpaca_trade_api as tradeapi


# ============================================================
# INTRADAY MOMENTUM BOT v3
# SPY + QQQ | LONG ONLY | RAILWAY READY
# ============================================================
#
# Changes from prior version:
#   1) Removed headline/news blocker entirely
#   2) Added trend filter to avoid chop
#   3) Increased cooldown to 30 minutes
#   4) Added breakout confirmation buffer
#   5) Simplified environment logic
#   6) Cleaner state handling and safer risk logic
#
# Goal:
#   - continuous scanning
#   - fewer bad chop trades
#   - more selective entries
#   - cleaner exits
# ============================================================


# ============================================================
# CONFIG
# ============================================================
API_KEY = os.getenv("APCA_API_KEY_ID", "PKSJTVGJZYO7UCP3PO6Q6WBSE4")
API_SECRET = os.getenv("APCA_API_SECRET_KEY", "4VuBmFdgCVoVvA7iuaprqJetZF9Xq3AXY7BmcUHPXQVF")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "Missing Alpaca API credentials. Set APCA_API_KEY_ID and APCA_API_SECRET_KEY."
    )

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

TZ = ZoneInfo("America/New_York")
SYMBOLS = ["SPY", "QQQ"]

# Risk / execution
RISK_PER_TRADE = 0.005
MAX_OPEN_POSITIONS = 2
MAX_TRADES_PER_SYMBOL_PER_DAY = 3
SCAN_INTERVAL_SECONDS = 20

ATR_STOP_MULT = 1.6
TRAIL_STOP_MULT = 1.3
MAX_NOTIONAL_PCT = 0.18
MIN_STOP_DISTANCE_PCT = 0.0025

COOLDOWN_MINUTES = 30
BREAKOUT_CONFIRM_PCT = 0.0005

# Time windows (ET)
ORB_START = (9, 30)
ORB_END = (9, 45)

PULLBACK_START = (9, 50)
PULLBACK_END = (11, 15)

AFTERNOON_START = (13, 30)
AFTERNOON_END = (15, 20)

LUNCH_START = (11, 30)
LUNCH_END = (13, 15)

HARD_FLAT_TIME = (15, 55)

# Persistence
STATE_FILE = "bot_state.json"
TRADE_LOG_FILE = "trade_log.csv"


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("intraday_bot_v3")


# ============================================================
# STATE
# ============================================================
DEFAULT_STATE = {
    "opening_ranges": {},
    "trades_today": {},
    "entry_prices": {},
    "entry_setups": {},
    "trailing_stops": {},
    "last_signal_bar_time": {},
    "cooldowns": {},
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
            log.info("Loaded state from disk.")
        except Exception as e:
            log.warning("Could not load state: %s", e)
            state = DEFAULT_STATE.copy()
    else:
        state = DEFAULT_STATE.copy()


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
# TIME HELPERS
# ============================================================
def now_et() -> datetime:
    return datetime.now(TZ)


def today_str() -> str:
    return now_et().strftime("%Y-%m-%d")


def current_minutes_et() -> int:
    now = now_et()
    return now.hour * 60 + now.minute


def hm_to_minutes(hm: tuple[int, int]) -> int:
    return hm[0] * 60 + hm[1]


def hhmm_in_range(start_hm: tuple[int, int], end_hm: tuple[int, int]) -> bool:
    cur = current_minutes_et()
    return hm_to_minutes(start_hm) <= cur < hm_to_minutes(end_hm)


def after_hhmm(hm: tuple[int, int]) -> bool:
    return current_minutes_et() >= hm_to_minutes(hm)


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
        log.warning("Could not list positions: %s", e)
    return positions


def get_open_orders() -> dict:
    orders = {}
    try:
        for o in api.list_orders(status="open"):
            orders[o.symbol] = o
    except Exception as e:
        log.warning("Could not list open orders: %s", e)
    return orders


def get_bars(symbol: str, timeframe: str = "1Min", limit: int = 240):
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
        log.warning("Bars error %s: %s", symbol, e)
        return None


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
    high = bars["high"]
    low = bars["low"]
    close = bars["close"]

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)

    return tr.ewm(alpha=1 / period, adjust=False).mean()


def vwap_intraday(bars: pd.DataFrame) -> pd.Series:
    day = today_str()
    session = bars[bars.index.strftime("%Y-%m-%d") == day].copy()
    if session.empty:
        session = bars.copy()

    typical = (session["high"] + session["low"] + session["close"]) / 3
    cumulative_vol = session["volume"].cumsum()
    return (typical * session["volume"]).cumsum() / cumulative_vol


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


def increment_trade_count(symbol: str):
    reset_daily_state_if_needed(symbol)
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
        last_exit = datetime.fromisoformat(ts)
        elapsed_minutes = (now_et() - last_exit).total_seconds() / 60
        return elapsed_minutes < COOLDOWN_MINUTES
    except Exception:
        return False


def set_cooldown(symbol: str):
    state["cooldowns"][symbol] = now_et().isoformat()
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
# MARKET REGIME / TREND FILTER
# ============================================================
def market_is_trending(bars: pd.DataFrame) -> bool:
    closes = bars["close"]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)

    if len(closes) < 50:
        return False

    separation = abs(float(ema20.iloc[-1]) - float(ema50.iloc[-1])) / max(float(closes.iloc[-1]), 0.01)
    slope_ok = abs(float(ema20.iloc[-1]) - float(ema20.iloc[-5])) / max(float(closes.iloc[-1]), 0.01) > 0.0008

    return separation > 0.0008


def is_lunch_chop() -> bool:
    return False


def setup_environment_ok(bars: pd.DataFrame) -> bool:
    if bars is None or len(bars) < 60:
        return False

    closes = bars["close"]
    atr14 = atr(bars, 14)

    if atr14.isna().iloc[-1]:
        return False

    price = float(closes.iloc[-1])
    atr_pct = float(atr14.iloc[-1]) / max(price, 0.01)

    if atr_pct < 0.0008:
        return False

    if not market_is_trending(bars):
        return False

    return True


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
    vw = vwap_intraday(bars)

    price = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2])
    vol_now = float(volumes.iloc[-1])
    vol_avg = float(volumes.tail(20).mean())
    bar_ts = bars.index[-1].isoformat()

    confirmed_breakout_level = orb_high * (1 + BREAKOUT_CONFIRM_PCT)

    breakout = prev_close <= orb_high and price > confirmed_breakout_level
    trend_ok = ema9.iloc[-1] > ema21.iloc[-1] and price > float(vw.iloc[-1])
    vol_ok = vol_now >= vol_avg * 1.05

    if breakout and trend_ok and vol_ok and not already_signaled_this_bar(symbol, "ORB", bar_ts):
        stop = min(
            price - float(atr14.iloc[-1] * ATR_STOP_MULT),
            orb_high - float(atr14.iloc[-1] * 0.25)
        )
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
    rs = rsi(closes, 14)
    atr14 = atr(bars, 14)
    vw = vwap_intraday(bars)

    price = float(closes.iloc[-1])
    bar_ts = bars.index[-1].isoformat()

    trend_ok = ema9.iloc[-1] > ema21.iloc[-1] and price > float(vw.iloc[-1])
    pullback_into_zone = float(lows.iloc[-2]) <= max(float(ema9.iloc[-2]), float(ema21.iloc[-2]))
    reclaim = price > float(highs.iloc[-2])
    rsi_ok = 45 <= float(rs.iloc[-1]) <= 72
    vol_ok = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.02

    if trend_ok and pullback_into_zone and reclaim and rsi_ok and vol_ok and not already_signaled_this_bar(symbol, "PULLBACK", bar_ts):
        stop = min(
            float(lows.iloc[-2]),
            price - float(atr14.iloc[-1] * ATR_STOP_MULT)
        )
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
    recent_high = float(highs.iloc[-11:-1].max())
    bar_ts = bars.index[-1].isoformat()

    confirmed_breakout_level = recent_high * (1 + BREAKOUT_CONFIRM_PCT)

    trend_ok = (
        price > float(vw.iloc[-1]) and
        ema9.iloc[-1] > ema21.iloc[-1] and
        closes.iloc[-1] > closes.iloc[-10]
    )
    breakout = price > confirmed_breakout_level
    vol_ok = float(volumes.iloc[-1]) >= float(volumes.tail(20).mean()) * 1.05

    if trend_ok and breakout and vol_ok and not already_signaled_this_bar(symbol, "AFTERNOON", bar_ts):
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

    raw_stop_distance = entry - stop
    min_stop_distance = entry * MIN_STOP_DISTANCE_PCT
    risk_per_share = max(raw_stop_distance, min_stop_distance, 0.01)

    risk_dollars = equity * RISK_PER_TRADE
    qty = int(risk_dollars / risk_per_share)

    max_notional = equity * MAX_NOTIONAL_PCT
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
    if on_cooldown(symbol):
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
        increment_trade_count(symbol)
        save_state()

        log.info(
            "BUY %s x%s @ %.2f | setup=%s | stop=%.2f",
            symbol, qty, entry, setup, stop
        )
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
        set_cooldown(symbol)
        save_state()

        log.info(
            "SELL %s @ %.2f | setup=%s | reason=%s | unreal=%.2f",
            symbol, last, setup, reason, unreal
        )
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
    ema9 = ema(closes, 9)
    atr14 = atr(bars, 14)
    vw = vwap_intraday(bars)

    price = float(closes.iloc[-1])
    atr_now = float(atr14.iloc[-1])
    current_stop = float(state["trailing_stops"].get(symbol, 0))
    setup = state["entry_setups"].get(symbol, "")

    new_stop = price - atr_now * TRAIL_STOP_MULT
    if new_stop > current_stop:
        state["trailing_stops"][symbol] = new_stop
        save_state()

    if price <= float(state["trailing_stops"].get(symbol, 0)):
        place_sell(symbol, "trailing_stop")
        return

    if setup in ("ORB", "PULLBACK"):
        if price < float(vw.iloc[-1]) and price < float(ema9.iloc[-1]):
            place_sell(symbol, "lost_vwap_and_ema9")
            return

    if setup == "AFTERNOON":
        if price < float(vw.iloc[-1]):
            place_sell(symbol, "lost_vwap")
            return

    if after_hhmm(HARD_FLAT_TIME):
        place_sell(symbol, "end_of_day")
        return


# ============================================================
# SCAN
# ============================================================
def scan_symbol(symbol: str):
    reset_daily_state_if_needed(symbol)

    bars = get_bars(symbol, timeframe="1Min", limit=240)
    if bars is None or len(bars) < 60:
        return

    update_opening_range(symbol, bars)
    manage_position(symbol, bars)

    if is_lunch_chop():
        return

    if not setup_environment_ok(bars):
        log.info("%s skipped - market not trending or too quiet", symbol)
        return

    if not can_open_new_position(symbol):
        return

    for setup_fn in (setup_orb, setup_pullback, setup_afternoon):
        signal = setup_fn(symbol, bars)
        if signal:
            log.info(
                "SIGNAL %s | %s @ %.2f stop=%.2f",
                signal["setup"],
                symbol,
                signal["price"],
                signal["stop"]
            )
            place_buy(signal)
            return


# ============================================================
# STATUS
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
                "POS | %s qty=%s current=%s unreal=%s stop=%.2f setup=%s",
                sym,
                pos.qty,
                pos.current_price,
                pos.unrealized_pl,
                float(state["trailing_stops"].get(sym, 0)),
                state["entry_setups"].get(sym, "?")
            )
    except Exception as e:
        log.warning("Status error: %s", e)


# ============================================================
# MAIN LOOP
# ============================================================
def run():
    load_state()
    log.info(
        "Intraday Momentum Bot v3 started | symbols=%s | base_url=%s",
        SYMBOLS,
        BASE_URL
    )

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
