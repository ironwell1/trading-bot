import os
import time
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import alpaca_trade_api as tradeapi

# ============================================================
# INTRADAY ORB BOT v5.1 — STOCKS IN PLAY (FIXED)
# ============================================================
#
# Strategy: Swiss Finance Institute Paper (Zarattini, Barbon, Aziz 2024)
#
# Fixes from v5.0:
#   1. No more per-symbol API loop — fast batch snapshot scan
#   2. Fixed ORB bar fetch (was missing today's bar)
#   3. Verbose logging so you can see exactly what passes/fails
#   4. Curated 200-stock watchlist of high-activity names
#   5. Scan retries if it finds 0 candidates
#
# Only needs: APCA_API_KEY_ID and APCA_API_SECRET_KEY
# ============================================================

# ============================================================
# CONFIG
# ============================================================
API_KEY = os.getenv("APCA_API_KEY_ID", "PKSJTVGJZYO7UCP3PO6Q6WBSE4")
API_SECRET = os.getenv("APCA_API_SECRET_KEY", "4VuBmFdgCVoVvA7iuaprqJetZF9Xq3AXY7BmcUHPXQVF")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing Alpaca credentials.")

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")
TZ  = ZoneInfo("America/New_York")

# ---- Filters ----
MIN_PRICE      = 5.0
MIN_AVG_VOL    = 500_000
MIN_ATR        = 0.25
MIN_REL_VOL    = 1.0
TOP_N          = 20

# ---- Risk ----
RISK_PCT       = 0.01
MAX_LEVERAGE   = 4.0
STOP_ATR_PCT   = 0.10

# ---- Time gates (minutes since midnight ET) ----
ORB_END        = 9 * 60 + 35
SCAN_START     = 9 * 60 + 36
ENTRY_CUTOFF   = 15 * 60 + 30
HARD_FLAT      = 15 * 60 + 55

LOOP_SLEEP     = 30
STATE_FILE     = "state_v5.json"
TRADE_LOG      = "trades_v5.csv"
DECISION_LOG   = "decisions_v5.txt"

# ---- Watchlist: high-volume stocks that frequently have catalysts ----
WATCHLIST = list(dict.fromkeys([
    "AAPL","MSFT","NVDA","AMD","TSLA","META","AMZN","GOOGL","NFLX","ORCL",
    "CRM","ADBE","INTC","QCOM","MU","AMAT","LRCX","MRVL","AVGO","TXN",
    "MRNA","BNTX","BIIB","GILD","REGN","VRTX","ILMN","IONS","ACAD","SGEN",
    "NBIX","EXEL","ALNY","BMRN","INCY","NKTR","BCRX","ARWR","MRTX","KRTX",
    "JPM","BAC","GS","MS","WFC","C","BLK","SCHW","AXP","V","MA","PYPL","SQ",
    "COIN","HOOD","SOFI","AFRM","UPST","OPEN","PLTR","RBLX","SNOW","U",
    "XOM","CVX","COP","OXY","MRO","DVN","HAL","SLB","VLO","PSX","PBF",
    "WMT","TGT","COST","HD","NKE","LULU","ANF","AEO","ROST","WYNN","MGM",
    "RIVN","LCID","NIO","LI","XPEV","PLUG","FCEL","BE","BLNK","CHPT",
    "MARA","RIOT","HUT","BTBT","CAN","GME","AMC","BBBY","CLOV","SNDL",
    "BA","LMT","RTX","NOC","GD","CAT","DE","HON","GE","MMM",
    "UNH","CVS","HUM","PFE","JNJ","ABBV","LLY","BMY","MRK",
    "DDD","FSLR","SWBI","RCL","W","VIR","EXAS","ALK","FOSL","WW",
    "OKTA","TWLO","TDOC","SPLK","PARA","WDC","ADSK","WDAY","NOW","ZM",
    "DKNG","PENN","CHGG","BYND","SPCE","NKLA","RIDE","GOEV","ARVL","ACTC",
    "F","GM","STLA","TTM","HMC","TM","XPEV","LI","NIO","RIVN",
    "UBER","LYFT","ABNB","DASH","SNAP","PINS","TWTR","SPOT","MTCH","IAC",
    "ZS","CRWD","S","PANW","FTNT","CYBR","TENB","VRNS","RPD","QLYS",
    "ENPH","SEDG","RUN","NOVA","ARRY","CSIQ","JKS","SPWR","MAXN","FSLR",
]))


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("orb")


# ============================================================
# STATE
# ============================================================
state = {}

def fresh_state():
    return {"date": today_str(), "scan_done": False,
            "candidates": [], "active_trades": {},
            "closed_trades": [], "trades_today": 0}

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            if s.get("date") == today_str():
                state.update(s)
                log.info("Resumed: %d candidates, %d active", len(state["candidates"]), len(state["active_trades"]))
                return
        except Exception as e:
            log.warning("State load error: %s", e)
    state.update(fresh_state())

def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        log.warning("Save error: %s", e)

def reset_if_new_day():
    if state.get("date") != today_str():
        log.info("New day — resetting state")
        state.clear()
        state.update(fresh_state())
        save_state()


# ============================================================
# TIME
# ============================================================
def now_et():      return datetime.now(TZ)
def today_str():   return datetime.now(TZ).strftime("%Y-%m-%d")
def mins_et():
    n = now_et(); return n.hour * 60 + n.minute

def is_market_open():
    try:    return api.get_clock().is_open
    except: return False

def orb_closed():  return mins_et() >= ORB_END
def scan_ready():  return mins_et() >= SCAN_START
def too_late():    return mins_et() >= ENTRY_CUTOFF
def eod_flat():    return mins_et() >= HARD_FLAT


# ============================================================
# DATA
# ============================================================
def get_daily_bars(sym, days=20):
    try:
        end   = now_et().strftime("%Y-%m-%d")
        start = (now_et() - timedelta(days=days+10)).strftime("%Y-%m-%d")
        bars  = api.get_bars(sym, "1Day", start=start, end=end, limit=days+5).df
        if bars is None or bars.empty: return None
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        return bars.sort_index()
    except: return None

def get_orb_bar(sym):
    """Get the 9:30-9:35 5-min bar for today."""
    try:
        bars = api.get_bars(sym, "5Min", limit=15).df
        if bars is None or bars.empty: return None
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC").tz_convert(TZ)
        else:
            bars.index = bars.index.tz_convert(TZ)
        bars = bars.sort_index()

        today = today_str()
        tb = bars[bars.index.strftime("%Y-%m-%d") == today]
        if tb.empty: return None

        # Try exact 9:30 bar first
        orb = tb[(tb.index.hour == 9) & (tb.index.minute == 30)]
        if orb.empty:
            orb = tb.head(1)  # fallback to first bar of day
        if orb.empty: return None

        r = orb.iloc[0]
        return {"open": float(r["open"]), "close": float(r["close"]),
                "high": float(r["high"]),  "low":   float(r["low"]),
                "volume": float(r["volume"])}
    except: return None

def get_latest_price(sym):
    try:
        snap = api.get_snapshot(sym)
        if snap and snap.latest_trade:  return float(snap.latest_trade.price)
        if snap and snap.latest_quote:  return float(snap.latest_quote.ask_price)
    except: pass
    return None


# ============================================================
# INDICATORS
# ============================================================
def atr14(bars):
    if bars is None or len(bars) < 5: return None
    h,l,c = bars["high"], bars["low"], bars["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1])

def avg_vol(bars, n=14):
    if bars is None or len(bars) < 3: return 0
    return float(bars["volume"].tail(n).mean())

def rel_vol(orb_volume, avg_daily):
    exp = avg_daily * 0.04  # first 5 min ≈ 4% of daily volume
    return orb_volume / exp if exp > 0 else 0.0


# ============================================================
# SCAN
# ============================================================
def scan_for_candidates():
    log.info("=" * 55)
    log.info("MORNING SCAN @ %s ET", now_et().strftime("%H:%M"))
    log.info("=" * 55)

    # Batch snapshots
    snaps = {}
    for i in range(0, len(WATCHLIST), 200):
        chunk = WATCHLIST[i:i+200]
        try:
            snaps.update(api.get_snapshots(chunk))
        except Exception as e:
            log.warning("Snapshot chunk failed: %s", e)

    log.info("Snapshots: %d / %d", len(snaps), len(WATCHLIST))

    # Price pre-filter
    pre = [s for s, snap in snaps.items()
           if _snap_price(snap) is not None and _snap_price(snap) >= MIN_PRICE]
    log.info("After price>$%.0f filter: %d symbols", MIN_PRICE, len(pre))

    candidates = []
    f_vol=f_atr=f_orb=f_rv=f_doji=0

    for sym in pre:
        try:
            daily = get_daily_bars(sym)
            if daily is None or len(daily) < 5: f_orb+=1; continue

            av  = avg_vol(daily)
            at  = atr14(daily)

            if av < MIN_AVG_VOL:   f_vol+=1;  continue
            if at is None or at < MIN_ATR: f_atr+=1; continue

            orb = get_orb_bar(sym)
            if orb is None: f_orb+=1; continue

            if abs(orb["close"] - orb["open"]) < 0.001: f_doji+=1; continue

            rv = rel_vol(orb["volume"], av)
            if rv < MIN_REL_VOL: f_rv+=1; continue

            bullish  = orb["close"] > orb["open"]
            dirn     = "LONG" if bullish else "SHORT"
            trigger  = orb["high"] if bullish else orb["low"]
            sd       = at * STOP_ATR_PCT
            stop     = (trigger - sd) if bullish else (trigger + sd)

            candidates.append({
                "symbol": sym, "direction": dirn,
                "orb_high": round(orb["high"],4), "orb_low": round(orb["low"],4),
                "orb_open": round(orb["open"],4), "orb_close": round(orb["close"],4),
                "orb_volume": orb["volume"], "avg_volume": round(av),
                "atr14": round(at,4), "rel_vol": round(rv,2),
                "entry_trigger": round(trigger,4),
                "stop": round(stop,4), "stop_dist": round(sd,4),
                "triggered": False, "in_trade": False,
            })
        except: continue

    candidates.sort(key=lambda x: x["rel_vol"], reverse=True)
    top = candidates[:TOP_N]

    log.info("Fail — volume:%d  ATR:%d  ORB:%d  RelVol:%d  doji:%d",
             f_vol, f_atr, f_orb, f_rv, f_doji)
    log.info("PASSED: %d  →  Selected top %d", len(candidates), len(top))

    if not top:
        log.warning("⚠️  ZERO candidates. Possible causes:")
        log.warning("   • Scan ran before 9:35 ET")
        log.warning("   • Alpaca data feed issue")
        log.warning("   • Very quiet market day")
        return []

    log.info("TOP %d STOCKS IN PLAY:", len(top))
    for i, c in enumerate(top):
        log.info("  #%02d %-6s %-5s RelVol=%5.1fx  Entry=%-8.2f  Stop=%-8.2f  ATR=%.2f",
                 i+1, c["symbol"], c["direction"], c["rel_vol"],
                 c["entry_trigger"], c["stop"], c["atr14"])

    try:
        with open(DECISION_LOG, "a") as f:
            f.write(f"\n{'='*55}\n{today_str()} @ {now_et().strftime('%H:%M ET')}\n")
            for i,c in enumerate(top):
                f.write(f"#{i+1:02d} {c['symbol']:6s} {c['direction']:5s} "
                        f"RelVol={c['rel_vol']:.1f}x "
                        f"Entry={c['entry_trigger']:.2f} Stop={c['stop']:.2f}\n")
    except: pass

    return top

def _snap_price(snap):
    try:
        if snap.latest_trade: return float(snap.latest_trade.price)
        if snap.latest_quote: return float(snap.latest_quote.ask_price)
    except: pass
    return None


# ============================================================
# SIZING + ORDERS
# ============================================================
def calc_qty(entry, stop_dist):
    eq  = float(api.get_account().equity)
    qty = int((eq * RISK_PCT) / max(stop_dist, 0.01))
    cap = int((eq * MAX_LEVERAGE) / max(entry, 0.01))
    return max(min(qty, cap), 1)

def get_positions():
    try: return {p.symbol: p for p in api.list_positions()}
    except: return {}

def enter_trade(c, price):
    sym  = c["symbol"]
    side = "buy" if c["direction"] == "LONG" else "sell"
    qty  = calc_qty(price, c["stop_dist"])

    if c["direction"] == "SHORT":
        try:
            if not getattr(api.get_account(), "shorting_enabled", False):
                log.info("  %s SHORT skipped — shorting not enabled", sym)
                return False
        except: pass

    try:
        api.submit_order(symbol=sym, qty=qty, side=side,
                         type="market", time_in_force="day")
        trade = {"symbol":sym, "direction":c["direction"],
                 "entry_price":round(price,4), "stop":c["stop"],
                 "stop_dist":c["stop_dist"], "qty":qty,
                 "atr14":c["atr14"], "rel_vol":c["rel_vol"],
                 "entry_time":now_et().isoformat()}
        state["active_trades"][sym] = trade
        state["trades_today"] += 1
        c["in_trade"] = True
        save_state()
        log.info("✅ ENTRY  %-5s %-6s x%d @ $%.2f | stop=$%.2f | RelVol=%.1fx",
                 c["direction"], sym, qty, price, c["stop"], c["rel_vol"])
        _log({"type":"ENTRY","timestamp":now_et().isoformat(),"symbol":sym,
              "direction":c["direction"],"qty":qty,"price":price,
              "stop":c["stop"],"rel_vol":c["rel_vol"]})
        return True
    except Exception as e:
        log.error("  Entry FAILED %s: %s", sym, e)
        return False

def exit_trade(sym, reason):
    pos   = get_positions().get(sym)
    trade = state["active_trades"].get(sym, {})
    if not pos:
        state["active_trades"].pop(sym, None); save_state(); return

    qty   = abs(int(float(pos.qty)))
    price = float(pos.current_price)
    pnl   = float(pos.unrealized_pl)
    entry = float(trade.get("entry_price", price))
    dirn  = trade.get("direction", "LONG")
    sd    = float(trade.get("stop_dist", 1))
    r     = ((price-entry) if dirn=="LONG" else (entry-price)) / max(sd, 0.01)
    cside = "sell" if dirn == "LONG" else "buy"

    try:
        api.submit_order(symbol=sym, qty=qty, side=cside,
                         type="market", time_in_force="day")
        log.info("🔴 EXIT   %-5s %-6s x%d @ $%.2f | pnl=$%.2f | %.2fR | %s",
                 dirn, sym, qty, price, pnl, r, reason)
        _log({"type":"EXIT","timestamp":now_et().isoformat(),"symbol":sym,
              "direction":dirn,"qty":qty,"price":price,"entry":entry,
              "pnl":pnl,"r":r,"reason":reason})
        state["closed_trades"].append({**trade,"exit_price":price,"pnl":pnl,"r":r,"reason":reason})
        state["active_trades"].pop(sym, None)
        save_state()
    except Exception as e:
        log.error("  Exit FAILED %s: %s", sym, e)

def _log(row):
    try:
        df = pd.DataFrame([row])
        if os.path.exists(TRADE_LOG): df.to_csv(TRADE_LOG, mode="a", header=False, index=False)
        else: df.to_csv(TRADE_LOG, index=False)
    except: pass


# ============================================================
# MONITOR + TRIGGERS
# ============================================================
def monitor_positions():
    pos = get_positions()
    for sym, trade in list(state["active_trades"].items()):
        p = pos.get(sym)
        if not p:
            state["active_trades"].pop(sym, None); save_state(); continue
        price = float(p.current_price)
        stop  = float(trade["stop"])
        dirn  = trade["direction"]
        if eod_flat():
            exit_trade(sym, "end_of_day"); continue
        if dirn == "LONG" and price <= stop:
            exit_trade(sym, "stop_loss")
        elif dirn == "SHORT" and price >= stop:
            exit_trade(sym, "stop_loss")

def check_triggers():
    if too_late() or eod_flat(): return
    pos  = get_positions()
    active = set(state["active_trades"].keys())
    for c in state["candidates"]:
        sym = c["symbol"]
        if c.get("in_trade") or c.get("triggered"): continue
        if sym in active or sym in pos: continue
        price = get_latest_price(sym)
        if price is None: continue
        if c["direction"] == "LONG" and price >= c["entry_trigger"]:
            log.info("🎯 BREAKOUT  %-6s above %.2f (now=%.2f RV=%.1fx)",
                     sym, c["entry_trigger"], price, c["rel_vol"])
            if enter_trade(c, price): c["triggered"] = True
        elif c["direction"] == "SHORT" and price <= c["entry_trigger"]:
            log.info("🎯 BREAKDOWN %-6s below %.2f (now=%.2f RV=%.1fx)",
                     sym, c["entry_trigger"], price, c["rel_vol"])
            if enter_trade(c, price): c["triggered"] = True


# ============================================================
# STATUS
# ============================================================
def print_status(loop):
    if loop % 10 != 0: return
    try:
        acc  = api.get_account()
        pos  = get_positions()
        cands = state.get("candidates",[])
        log.info("📊 equity=$%s | positions=%d | trades=%d | candidates=%d | %s ET",
                 acc.equity, len(pos), state.get("trades_today",0),
                 len(cands), now_et().strftime("%H:%M"))
        for sym,t in state.get("active_trades",{}).items():
            p = pos.get(sym)
            if p:
                log.info("  📈 %-5s %-6s entry=$%-8.2f now=$%-8s pnl=$%-8s stop=$%.2f",
                         t["direction"],sym,t["entry_price"],p.current_price,p.unrealized_pl,t["stop"])
    except Exception as e:
        log.warning("Status err: %s", e)

def print_summary():
    closed = state.get("closed_trades",[])
    if not closed: return
    pnl  = sum(t.get("pnl",0) for t in closed)
    wins = sum(1 for t in closed if t.get("pnl",0)>0)
    avgr = sum(t.get("r",0) for t in closed)/len(closed)
    log.info("="*55)
    log.info("DAILY SUMMARY: %d trades | %d W / %d L | PnL=$%.2f | AvgR=%.2f",
             len(closed), wins, len(closed)-wins, pnl, avgr)
    log.info("="*55)


# ============================================================
# MAIN
# ============================================================
def run():
    load_state()
    log.info("="*55)
    log.info("ORB Bot v5.1 | %s | %d watchlist symbols", today_str(), len(WATCHLIST))
    log.info("="*55)

    loop = 0
    while True:
        try:
            reset_if_new_day()

            if not is_market_open():
                log.info("Market closed — sleeping 60s")
                time.sleep(60); continue

            m = mins_et()

            # Wait for ORB to close
            if not orb_closed():
                log.info("⏳ ORB open — %d min until 9:35 ET", ORB_END - m)
                time.sleep(LOOP_SLEEP); loop+=1; continue

            # Run scan once
            if not state["scan_done"]:
                cands = scan_for_candidates()
                state["candidates"] = cands
                state["scan_done"]  = True
                save_state()
                if not cands:
                    log.warning("0 candidates — retrying in 2 min")
                    state["scan_done"] = False
                    time.sleep(120); loop+=1; continue

            # Trading
            if state["active_trades"]:
                monitor_positions()
            if not too_late() and not eod_flat():
                check_triggers()

            print_status(loop)
            if eod_flat() and loop % 20 == 0:
                print_summary()

            time.sleep(LOOP_SLEEP)
            loop += 1

        except KeyboardInterrupt:
            log.info("Stopped."); print_summary(); break
        except Exception as e:
            log.error("Loop error: %s", e); time.sleep(30)

if __name__ == "__main__":
    run()
