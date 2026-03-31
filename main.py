import os
import time
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import alpaca_trade_api as tradeapi

# ============================================================
# INTRADAY ORB BOT v5 — STOCKS IN PLAY EDITION
# ============================================================
#
# Strategy: Swiss Finance Institute Paper (Zarattini, Barbon, Aziz 2024)
#   "A Profitable Day Trading Strategy For The U.S. Equity Market"
#
# Core Logic:
#   1. Every morning at 9:35 ET, scan all liquid US stocks
#   2. Calculate Relative Volume (first 5-min vol vs 14-day avg)
#   3. Select TOP 20 stocks with highest RelVol (must be > 100%)
#   4. Determine direction from first 5-min candle:
#       - Bullish candle (close > open) → LONG ONLY
#       - Bearish candle (close < open) → SHORT ONLY
#       - Dojo (close == open) → SKIP
#   5. Enter on breakout of ORB high (long) or ORB low (short)
#   6. Stop loss: 10% of 14-day ATR from entry
#   7. Profit target: End of Day (let winners run)
#   8. Risk per trade: 1% of equity, max 4x leverage
#
# Why this works:
#   - High RelVol = institutional imbalance = real trend
#   - Direction-locked = no fighting the tape
#   - Tight stop, wide target = asymmetric R:R
#   - 20 stocks/day = statistically meaningful sample
#
# Paper results (2016-2023):
#   - 1,637% total return vs 198% S&P 500
#   - Sharpe 2.81, Alpha 36%/yr, Beta ~0
# ============================================================


# ============================================================
# CONFIG
# ============================================================
API_KEY = os.getenv("APCA_API_KEY_ID", "PKSJTVGJZYO7UCP3PO6Q6WBSE4")
API_SECRET = os.getenv("APCA_API_SECRET_KEY", "4VuBmFdgCVoVvA7iuaprqJetZF9Xq3AXY7BmcUHPXQVF")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing Alpaca credentials.")

api    = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

TZ = ZoneInfo("America/New_York")

# ---- Universe filters (from paper) ----
MIN_PRICE          = 5.0        # stock must be > $5
MIN_AVG_VOLUME     = 1_000_000  # 14-day avg volume > 1M shares
MIN_ATR            = 0.50       # 14-day ATR > $0.50
MIN_REL_VOL        = 1.00       # relative volume > 100%
TOP_N_STOCKS       = 20         # trade the top 20 by RelVol

# ---- Risk (from paper) ----
RISK_PER_TRADE_PCT = 0.01       # risk 1% of equity per trade
MAX_LEVERAGE       = 4.0        # max 4x per FINRA rules
STOP_ATR_PCT       = 0.10       # stop = 10% of ATR from entry

# ---- Time gates (ET) ----
ORB_WINDOW_START   = (9, 30)
ORB_WINDOW_END     = (9, 35)    # 5-min ORB
SCAN_TIME          = (9, 36)    # scan starts right after ORB closes
ENTRY_CUTOFF       = (15, 30)   # no new entries after 3:30 PM
HARD_FLAT          = (15, 55)   # close everything by 3:55 PM

# ---- Operational ----
SCAN_INTERVAL      = 30         # seconds between main loop ticks
STATE_FILE         = "bot_state_v5.json"
TRADE_LOG          = "trade_log_v5.csv"
DECISION_LOG       = "decision_log_v5.txt"

# Stocks to exclude (ETFs, funds, etc — we want single stocks in play)
EXCLUDE_TICKERS = {
    "SPY", "QQQ", "IWM", "DIA", "GLD", "SLV", "TLT", "HYG", "LQD",
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE",
    "VXX", "UVXY", "SQQQ", "TQQQ", "SPXU", "SPXL", "UPRO", "SDOW",
}


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("orb_v5")


# ============================================================
# STATE
# ============================================================
DEFAULT_STATE = {
    "date":              "",
    "scan_complete":     False,
    "candidates":        [],      # top 20 stocks selected today
    "active_trades":     {},      # symbol → trade info
    "closed_trades":     [],      # completed trades today
    "total_trades_today": 0,
}
state = DEFAULT_STATE.copy()


def load_state():
    global state
    today = now_et().strftime("%Y-%m-%d")
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                loaded = json.load(f)
            if loaded.get("date") == today:
                state = loaded
                log.info("Resumed today's state: %d candidates, %d active trades",
                         len(state["candidates"]), len(state["active_trades"]))
            else:
                log.info("New trading day — resetting state.")
                state = {**DEFAULT_STATE.copy(), "date": today}
        except Exception as e:
            log.warning("State load failed: %s", e)
            state = {**DEFAULT_STATE.copy(), "date": today}
    else:
        state = {**DEFAULT_STATE.copy(), "date": today}


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        log.warning("State save failed: %s", e)


def reset_if_new_day():
    today = now_et().strftime("%Y-%m-%d")
    if state.get("date") != today:
        log.info("New day detected — resetting state.")
        state.clear()
        state.update({**DEFAULT_STATE.copy(), "date": today})
        save_state()


# ============================================================
# TIME HELPERS
# ============================================================
def now_et():
    return datetime.now(TZ)

def mins_et():
    n = now_et()
    return n.hour * 60 + n.minute

def hm(t): return t[0] * 60 + t[1]

def today_str():
    return now_et().strftime("%Y-%m-%d")

def is_market_open():
    try:
        return api.get_clock().is_open
    except Exception:
        return False

def orb_is_closed():
    """True after 9:35 ET — ORB window has closed."""
    return mins_et() >= hm(ORB_WINDOW_END)

def scan_window_open():
    """Scan for candidates between 9:36 and 9:45."""
    return hm(SCAN_TIME) <= mins_et() <= hm((9, 45))

def too_late_to_enter():
    return mins_et() >= hm(ENTRY_CUTOFF)

def should_flatten_all():
    return mins_et() >= hm(HARD_FLAT)


# ============================================================
# BROKER HELPERS
# ============================================================
def get_account():
    return api.get_account()

def get_positions():
    try:
        return {p.symbol: p for p in api.list_positions()}
    except Exception:
        return {}

def get_equity():
    try:
        return float(get_account().equity)
    except Exception:
        return 25000.0


# ============================================================
# MARKET DATA
# ============================================================
def get_bars_daily(symbol, days=20):
    """Get daily bars for baseline ATR and volume calculation."""
    try:
        end   = now_et().strftime("%Y-%m-%d")
        start = (now_et() - timedelta(days=days + 10)).strftime("%Y-%m-%d")
        bars  = api.get_bars(symbol, "1Day", start=start, end=end, limit=days + 5).df
        if bars is None or bars.empty:
            return None
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        return bars.sort_index()
    except Exception:
        return None

def get_bars_intraday(symbol, timeframe="1Min", limit=30):
    """Get intraday minute bars."""
    try:
        bars = api.get_bars(symbol, timeframe, limit=limit).df
        if bars is None or bars.empty:
            return None
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        return bars.sort_index()
    except Exception:
        return None

def get_first_5min_bar(symbol):
    """
    Get the aggregated first 5-minute bar (9:30-9:35).
    Returns: open, close, high, low, volume of the ORB window.
    """
    try:
        bars = api.get_bars(symbol, "5Min", limit=5).df
        if bars is None or bars.empty:
            return None
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        bars = bars.sort_index()

        today = today_str()
        today_bars = bars[bars.index.strftime("%Y-%m-%d") == today]
        if today_bars.empty:
            return None

        # The 9:30 bar
        orb_bar = today_bars[today_bars.index.hour == 9]
        orb_bar = orb_bar[orb_bar.index.minute == 30]
        if orb_bar.empty:
            return None

        row = orb_bar.iloc[0]
        return {
            "open":   float(row["open"]),
            "close":  float(row["close"]),
            "high":   float(row["high"]),
            "low":    float(row["low"]),
            "volume": float(row["volume"]),
        }
    except Exception as e:
        return None


# ============================================================
# INDICATORS
# ============================================================
def calc_atr14_daily(daily_bars):
    """14-day ATR from daily bars."""
    if daily_bars is None or len(daily_bars) < 15:
        return None
    h = daily_bars["high"]
    l = daily_bars["low"]
    c = daily_bars["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1])

def calc_avg_volume_14(daily_bars):
    """14-day average daily volume."""
    if daily_bars is None or len(daily_bars) < 14:
        return 0
    return float(daily_bars["volume"].tail(14).mean())

def calc_relative_volume(orb_volume, avg_daily_volume):
    """
    RelVol = ORB 5-min volume / (avg daily volume / 14)
    Compares today's first 5 min volume to average first-5-min volume.
    Paper uses: ORV_today / avg(ORV_last_14_days)
    We approximate: since we don't have historical 5-min data per stock,
    we use a heuristic: first 5 min typically ~4% of daily volume.
    So avg_5min_vol ≈ avg_daily_vol * 0.04
    """
    avg_5min_vol = avg_daily_volume * 0.04
    if avg_5min_vol <= 0:
        return 0
    return orb_volume / avg_5min_vol


# ============================================================
# UNIVERSE SCANNER
# ============================================================
def get_tradeable_universe():
    """
    Get all active, tradeable US equity assets from Alpaca.
    Filter to stocks only (no ETFs, no OTC).
    """
    try:
        assets = api.list_assets(status="active", asset_class="us_equity")
        symbols = []
        for a in assets:
            if not a.tradable:
                continue
            if not a.fractionable and a.symbol in EXCLUDE_TICKERS:
                continue
            # Skip obvious ETFs by checking exchange and name patterns
            sym = a.symbol
            if sym in EXCLUDE_TICKERS:
                continue
            if len(sym) > 5:  # most ETFs/funds have short symbols, skip very long ones
                continue
            symbols.append(sym)
        log.info("Universe: %d tradeable symbols", len(symbols))
        return symbols
    except Exception as e:
        log.error("Universe fetch failed: %s", e)
        return []


def scan_for_candidates():
    """
    Core scan: find top 20 Stocks in Play by Relative Volume.
    Runs once per day right after the ORB window closes (9:35 ET).
    
    Returns list of candidate dicts with all trade parameters pre-calculated.
    """
    log.info("=" * 60)
    log.info("MORNING SCAN — Finding Stocks in Play...")
    log.info("=" * 60)

    symbols = get_tradeable_universe()
    if not symbols:
        log.error("No symbols in universe. Aborting scan.")
        return []

    candidates = []
    checked    = 0
    skipped    = 0

    # We can't check all 8000+ stocks — use snapshot API for fast price/volume screening
    # First pass: get snapshots for all symbols to pre-filter by price and volume
    try:
        # Alpaca snapshot API — gets latest quote + bar data for many symbols at once
        chunk_size = 1000
        snapshots  = {}

        for i in range(0, min(len(symbols), 5000), chunk_size):
            chunk = symbols[i:i+chunk_size]
            try:
                snaps = api.get_snapshots(chunk)
                snapshots.update(snaps)
            except Exception as e:
                log.warning("Snapshot chunk failed: %s", e)
                continue

        log.info("Got snapshots for %d symbols", len(snapshots))

    except Exception as e:
        log.error("Snapshot fetch failed: %s", e)
        snapshots = {}

    # Second pass: for each symbol in snapshot, apply paper's filters
    pre_filtered = []
    for sym, snap in snapshots.items():
        if sym in EXCLUDE_TICKERS:
            continue
        try:
            # Get latest price from snapshot
            latest_price = None
            if snap.latest_trade:
                latest_price = float(snap.latest_trade.price)
            elif snap.latest_quote:
                latest_price = float(snap.latest_quote.ask_price)

            if latest_price is None or latest_price < MIN_PRICE:
                continue

            # Get today's 5-min bar volume from snapshot
            daily_bar = snap.daily_bar
            if daily_bar is None:
                continue

            # Quick volume check using day's volume so far
            vol_today = float(daily_bar.volume) if daily_bar.volume else 0
            if vol_today < 50_000:  # must have at least some activity
                continue

            pre_filtered.append(sym)

        except Exception:
            continue

    log.info("Pre-filtered to %d symbols with price > $%.0f and activity", len(pre_filtered), MIN_PRICE)

    # Third pass: deep analysis on pre-filtered symbols
    for sym in pre_filtered:
        try:
            checked += 1
            if checked % 100 == 0:
                log.info("  Scanning... %d/%d checked, %d candidates so far",
                         checked, len(pre_filtered), len(candidates))

            # Get daily bars for ATR and avg volume
            daily = get_bars_daily(sym, days=20)
            if daily is None or len(daily) < 15:
                skipped += 1
                continue

            avg_vol  = calc_avg_volume_14(daily)
            atr14    = calc_atr14_daily(daily)

            # Paper filter 1: avg daily volume > 1M
            if avg_vol < MIN_AVG_VOLUME:
                skipped += 1
                continue

            # Paper filter 2: ATR > $0.50
            if atr14 is None or atr14 < MIN_ATR:
                skipped += 1
                continue

            # Get the first 5-min bar
            orb = get_first_5min_bar(sym)
            if orb is None:
                skipped += 1
                continue

            # Skip doji (no direction)
            if abs(orb["close"] - orb["open"]) < 0.001:
                skipped += 1
                continue

            # Paper filter 3: relative volume > 100%
            rel_vol = calc_relative_volume(orb["volume"], avg_vol)
            if rel_vol < MIN_REL_VOL:
                skipped += 1
                continue

            # Determine direction
            bullish = orb["close"] > orb["open"]
            direction = "LONG" if bullish else "SHORT"

            # Entry level = ORB high (long) or ORB low (short)
            entry_trigger = orb["high"] if bullish else orb["low"]

            # Stop = 10% ATR from entry (paper's exact rule)
            stop_distance = atr14 * STOP_ATR_PCT
            stop = (entry_trigger - stop_distance) if bullish else (entry_trigger + stop_distance)

            candidates.append({
                "symbol":        sym,
                "direction":     direction,
                "orb_open":      orb["open"],
                "orb_close":     orb["close"],
                "orb_high":      orb["high"],
                "orb_low":       orb["low"],
                "orb_volume":    orb["volume"],
                "avg_volume":    avg_vol,
                "atr14":         atr14,
                "rel_vol":       rel_vol,
                "entry_trigger": entry_trigger,
                "stop":          stop,
                "stop_distance": stop_distance,
                "triggered":     False,
                "in_trade":      False,
            })

        except Exception as e:
            skipped += 1
            continue

    # Sort by relative volume descending — highest RelVol = most in play
    candidates.sort(key=lambda x: x["rel_vol"], reverse=True)

    # Take top N
    top = candidates[:TOP_N_STOCKS]

    log.info("=" * 60)
    log.info("SCAN COMPLETE")
    log.info("Checked: %d | Skipped: %d | Candidates found: %d | Selected: %d",
             checked, skipped, len(candidates), len(top))
    log.info("=" * 60)

    for i, c in enumerate(top):
        log.info(
            "  #%02d %s | %s | RelVol=%.1fx | Entry @ %.2f | Stop @ %.2f | ATR=%.2f",
            i+1, c["symbol"], c["direction"],
            c["rel_vol"], c["entry_trigger"], c["stop"], c["atr14"]
        )

    # Log to decision file
    try:
        with open(DECISION_LOG, "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"DATE: {today_str()} | SCAN AT: {now_et().strftime('%H:%M ET')}\n")
            f.write(f"TOP {TOP_N_STOCKS} STOCKS IN PLAY:\n")
            for i, c in enumerate(top):
                f.write(f"  #{i+1:02d} {c['symbol']:6s} | {c['direction']:5s} | "
                        f"RelVol={c['rel_vol']:.1f}x | Entry={c['entry_trigger']:.2f} | "
                        f"Stop={c['stop']:.2f} | ATR={c['atr14']:.2f}\n")
    except Exception:
        pass

    return top


# ============================================================
# POSITION SIZING (paper's exact method)
# ============================================================
def calc_qty(entry_price, stop_distance, direction):
    """
    Paper: risk 1% of equity per trade, max 4x leverage.
    qty = (equity * 0.01) / stop_distance
    Capped by: qty * price <= equity * 4
    """
    equity       = get_equity()
    risk_dollars = equity * RISK_PER_TRADE_PCT
    qty          = int(risk_dollars / max(stop_distance, 0.01))

    max_notional = equity * MAX_LEVERAGE
    max_qty      = int(max_notional / max(entry_price, 0.01))
    qty          = min(qty, max_qty)

    return max(qty, 1)


# ============================================================
# ORDER EXECUTION
# ============================================================
def place_entry(candidate, current_price):
    sym       = candidate["symbol"]
    direction = candidate["direction"]
    stop      = candidate["stop"]
    stop_dist = candidate["stop_distance"]
    entry     = current_price

    qty = calc_qty(entry, stop_dist, direction)
    if qty <= 0:
        log.info("Skip %s — qty=0", sym)
        return False

    side = "buy" if direction == "LONG" else "sell"

    try:
        # Check we have shorting enabled for short trades
        if direction == "SHORT":
            try:
                account = get_account()
                if not getattr(account, 'shorting_enabled', False):
                    log.info("Skipping %s SHORT — shorting not enabled on account", sym)
                    return False
            except Exception:
                pass

        api.submit_order(
            symbol=sym,
            qty=qty,
            side=side,
            type="market",
            time_in_force="day"
        )

        trade_info = {
            "symbol":      sym,
            "direction":   direction,
            "entry_price": entry,
            "stop":        stop,
            "qty":         qty,
            "entry_time":  now_et().isoformat(),
            "atr14":       candidate["atr14"],
            "rel_vol":     candidate["rel_vol"],
        }

        state["active_trades"][sym]    = trade_info
        state["total_trades_today"]   += 1
        candidate["in_trade"]          = True
        save_state()

        log.info(
            "✅ ENTRY %s %s x%d @ %.2f | stop=%.2f | RelVol=%.1fx | risk=$%.2f",
            direction, sym, qty, entry, stop,
            candidate["rel_vol"],
            qty * stop_dist
        )

        # Log entry
        _log_trade({
            "timestamp":   now_et().isoformat(),
            "symbol":      sym,
            "side":        side,
            "qty":         qty,
            "price":       entry,
            "stop":        stop,
            "direction":   direction,
            "rel_vol":     candidate["rel_vol"],
            "type":        "ENTRY",
        })

        return True

    except Exception as e:
        log.error("Entry failed %s: %s", sym, e)
        return False


def place_exit(sym, reason):
    positions = get_positions()
    pos = positions.get(sym)
    if not pos:
        state["active_trades"].pop(sym, None)
        save_state()
        return

    qty       = abs(int(float(pos.qty)))
    price     = float(pos.current_price)
    unreal    = float(pos.unrealized_pl)
    trade     = state["active_trades"].get(sym, {})
    direction = trade.get("direction", "LONG")
    entry     = float(trade.get("entry_price", price))
    atr14     = float(trade.get("atr14", 1))

    # Close the position
    close_side = "sell" if direction == "LONG" else "buy"
    try:
        api.submit_order(
            symbol=sym,
            qty=qty,
            side=close_side,
            type="market",
            time_in_force="day"
        )

        r_multiple = (price - entry) / max(atr14 * STOP_ATR_PCT, 0.01)
        if direction == "SHORT":
            r_multiple = (entry - price) / max(atr14 * STOP_ATR_PCT, 0.01)

        log.info(
            "🔴 EXIT %s %s @ %.2f | reason=%s | pnl=$%.2f | R=%.2fR",
            sym, direction, price, reason, unreal, r_multiple
        )

        _log_trade({
            "timestamp":   now_et().isoformat(),
            "symbol":      sym,
            "side":        close_side,
            "qty":         qty,
            "price":       price,
            "entry":       entry,
            "pnl":         unreal,
            "r_multiple":  r_multiple,
            "reason":      reason,
            "direction":   direction,
            "type":        "EXIT",
        })

        state["closed_trades"].append({**trade, "exit_price": price, "pnl": unreal,
                                        "r_multiple": r_multiple, "reason": reason})
        state["active_trades"].pop(sym, None)
        save_state()

    except Exception as e:
        log.error("Exit failed %s: %s", sym, e)


def _log_trade(row):
    try:
        df = pd.DataFrame([row])
        if os.path.exists(TRADE_LOG):
            df.to_csv(TRADE_LOG, mode="a", header=False, index=False)
        else:
            df.to_csv(TRADE_LOG, index=False)
    except Exception:
        pass


# ============================================================
# POSITION MONITOR
# ============================================================
def monitor_open_positions():
    """
    For each active trade, check stop loss.
    Profit target = EOD (paper's rule — let winners run all day).
    """
    positions = get_positions()

    for sym, trade in list(state["active_trades"].items()):
        pos = positions.get(sym)
        if not pos:
            # Position gone — probably filled/cancelled externally
            log.warning("%s not found in positions — removing from state", sym)
            state["active_trades"].pop(sym, None)
            save_state()
            continue

        price     = float(pos.current_price)
        stop      = float(trade["stop"])
        direction = trade["direction"]

        # EOD flatten
        if should_flatten_all():
            place_exit(sym, "end_of_day")
            continue

        # Stop loss check
        if direction == "LONG" and price <= stop:
            place_exit(sym, "stop_loss")
        elif direction == "SHORT" and price >= stop:
            place_exit(sym, "stop_loss")


# ============================================================
# ENTRY TRIGGER MONITOR
# ============================================================
def check_entry_triggers():
    """
    For each candidate not yet in a trade, check if price
    has broken out of the ORB level. If so, enter.
    """
    if too_late_to_enter():
        return
    if should_flatten_all():
        return

    positions    = get_positions()
    active_syms  = set(state["active_trades"].keys())

    for c in state["candidates"]:
        sym = c["symbol"]

        # Skip if already in trade or already has position
        if c.get("in_trade") or c.get("triggered"):
            continue
        if sym in active_syms or sym in positions:
            continue

        try:
            # Get latest price
            snap = api.get_snapshot(sym)
            if snap is None:
                continue

            price = None
            if snap.latest_trade:
                price = float(snap.latest_trade.price)
            elif snap.latest_quote:
                price = float(snap.latest_quote.ask_price)

            if price is None:
                continue

            trigger = c["entry_trigger"]

            if c["direction"] == "LONG" and price >= trigger:
                log.info("🎯 BREAKOUT: %s crossed above %.2f (current: %.2f)", sym, trigger, price)
                success = place_entry(c, price)
                if success:
                    c["triggered"] = True

            elif c["direction"] == "SHORT" and price <= trigger:
                log.info("🎯 BREAKDOWN: %s crossed below %.2f (current: %.2f)", sym, trigger, price)
                success = place_entry(c, price)
                if success:
                    c["triggered"] = True

        except Exception as e:
            continue


# ============================================================
# DAILY SUMMARY
# ============================================================
def print_daily_summary():
    closed = state.get("closed_trades", [])
    if not closed:
        return

    total_pnl   = sum(t.get("pnl", 0) for t in closed)
    wins        = [t for t in closed if t.get("pnl", 0) > 0]
    losses      = [t for t in closed if t.get("pnl", 0) <= 0]
    avg_r       = sum(t.get("r_multiple", 0) for t in closed) / max(len(closed), 1)
    win_rate    = len(wins) / max(len(closed), 1) * 100

    log.info("=" * 60)
    log.info("📊 DAILY SUMMARY")
    log.info("  Trades: %d | Wins: %d | Losses: %d | Win Rate: %.0f%%",
             len(closed), len(wins), len(losses), win_rate)
    log.info("  Total P&L: $%.2f | Avg R: %.2fR", total_pnl, avg_r)
    log.info("=" * 60)


# ============================================================
# STATUS PRINT
# ============================================================
def print_status():
    try:
        account   = get_account()
        positions = get_positions()
        active    = state.get("active_trades", {})
        cands     = state.get("candidates", [])
        triggered = sum(1 for c in cands if c.get("triggered"))

        log.info(
            "📊 STATUS | equity=$%s | positions=%d | trades_today=%d | candidates=%d (%d triggered) | time=%s ET",
            account.equity,
            len(positions),
            state.get("total_trades_today", 0),
            len(cands),
            triggered,
            now_et().strftime("%H:%M")
        )

        for sym, trade in active.items():
            pos = positions.get(sym)
            if pos:
                log.info(
                    "  📈 %s %s | qty=%s | entry=%.2f | current=%s | pnl=%s | stop=%.2f",
                    trade["direction"], sym,
                    pos.qty, trade["entry_price"],
                    pos.current_price, pos.unrealized_pl,
                    trade["stop"]
                )
    except Exception as e:
        log.warning("Status error: %s", e)


# ============================================================
# MAIN LOOP
# ============================================================
def run():
    load_state()

    log.info("=" * 60)
    log.info("ORB Bot v5 — Stocks in Play Edition")
    log.info("Based on: Zarattini, Barbon, Aziz (2024)")
    log.info("Strategy: 5-min ORB on Top-%d RelVol Stocks", TOP_N_STOCKS)
    log.info("Risk: %.0f%% per trade | Stop: %.0f%% ATR | Target: EOD",
             RISK_PER_TRADE_PCT * 100, STOP_ATR_PCT * 100)
    log.info("=" * 60)

    loop_count = 0

    while True:
        try:
            reset_if_new_day()

            if not is_market_open():
                log.info("Market closed. Sleeping 60s.")
                time.sleep(60)
                continue

            # ---- PHASE 1: Morning scan (runs once after ORB closes) ----
            if orb_is_closed() and not state["scan_complete"]:
                candidates = scan_for_candidates()
                state["candidates"]    = candidates
                state["scan_complete"] = True
                save_state()

                if not candidates:
                    log.warning("No candidates found today. Will retry next loop.")
                    state["scan_complete"] = False  # allow retry

            # ---- PHASE 2: Trading loop ----
            elif state["scan_complete"]:

                # Monitor and manage open positions (stop loss, EOD)
                if state["active_trades"]:
                    monitor_open_positions()

                # Check for new entry triggers on candidates
                if not too_late_to_enter() and not should_flatten_all():
                    check_entry_triggers()

                # Print status every ~5 minutes
                if loop_count % 10 == 0:
                    print_status()

                # Print summary at end of day
                if should_flatten_all() and loop_count % 20 == 0:
                    print_daily_summary()

            # ---- PHASE 3: Waiting for ORB to close ----
            else:
                m = mins_et()
                if m < hm(ORB_WINDOW_END):
                    remaining = hm(ORB_WINDOW_END) - m
                    log.info("Waiting for ORB window to close... %d min remaining (9:35 ET)", remaining)

            loop_count += 1
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log.info("Stopped by user.")
            print_daily_summary()
            break
        except Exception as e:
            log.error("Main loop error: %s", e)
            time.sleep(30)


if __name__ == "__main__":
    run()
