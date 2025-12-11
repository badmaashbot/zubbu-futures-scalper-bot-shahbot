#!/usr/bin/env python3
"""
ZUBBU_REVERSAL_V4_BALANCED_A
Reversal Bot – Balanced Mode:
- Bollinger Band Extremes
- RSI Curl Confirmation
- Band-Walk Protection
- Volatility Spike Guard
- Wick-Strength Filter
- Orderflow Slowdown Check
- 0.35s Confirmation Delay
- TP = 0.25% , SL = 0.18%
- Balanced Reversal Mode (10–18 trades/day)
"""

import os, time, json, math, asyncio, aiohttp
import numpy as np
from collections import deque
import ccxt

# ---------------- CONFIG -----------------
API_KEY     = os.getenv("BYBIT_API_KEY")
API_SECRET  = os.getenv("BYBIT_API_SECRET")
TESTNET     = os.getenv("BYBIT_TESTNET","0") in ("1","true","True")

SYMBOLS = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","DOGEUSDT"]
MAP = {
    "BTCUSDT":"BTC/USDT:USDT",
    "ETHUSDT":"ETH/USDT:USDT",
    "SOLUSDT":"SOL/USDT:USDT",
    "BNBUSDT":"BNB/USDT:USDT",
    "DOGEUSDT":"DOGE/USDT:USDT",
}

WS_URL = (
    "wss://stream-testnet.bybit.com/v5/public/linear"
    if TESTNET else
    "wss://stream.bybit.com/v5/public/linear"
)

TP_PCT = 0.0025     # 0.25%
SL_PCT = 0.0018     # 0.18%
CONFIRM_DELAY = 0.35
COOLDOWN = 12
LEVERAGE = 5
USE_EQUITY = 0.97

BB_PERIOD = 20
BB_STD    = 2.0
RSI_PERIOD = 14

VOLATILITY_THRESHOLD = 0.0035    # 0.35% 1m guard
WICK_MIN_RATIO = 0.40           # wick must be at least 40% candle

# -----------------------------------------
# INDICATORS
# -----------------------------------------
def rsi(values, period=14):
    if len(values) < period + 2:
        return None
    diff = np.diff(values)
    up = diff.clip(min=0)
    dn = -diff.clip(max=0)
    avg_up = np.mean(up[-period:])
    avg_dn = np.mean(dn[-period:])
    if avg_dn == 0:
        return 100
    rs = avg_up / avg_dn
    return 100 - (100/(1+rs))

def bollinger(values, period=20, std=2.0):
    if len(values) < period:
        return None
    arr = np.array(values[-period:])
    mid = np.mean(arr)
    dev = np.std(arr)
    return mid + std*dev, mid, mid - std*dev

# -----------------------------------------
# EXCHANGE WRAPPER
# -----------------------------------------
class EX:
    def __init__(self):
        cfg = {
            "apiKey":API_KEY,
            "secret":API_SECRET,
            "enableRateLimit":True,
            "options":{"defaultType":"swap"},
        }
        if TESTNET:
            cfg["urls"]={"api":{
                "public":"https://api-testnet.bybit.com",
                "private":"https://api-testnet.bybit.com"
            }}
        self.c = ccxt.bybit(cfg)

    async def leverage(self, s):
        def w():
            try: self.c.set_leverage(LEVERAGE, MAP[s], params={"category":"linear"})
            except: pass
        return await asyncio.to_thread(w)

    async def equity(self):
        def w():
            bal = self.c.fetch_balance()
            return float(bal["USDT"]["total"])
        return await asyncio.to_thread(w)

    async def pos_size(self, s):
        def w():
            try:
                ps = self.c.fetch_positions([MAP[s]])
                for p in ps:
                    return abs(float(p.get("contracts",0)))
            except:
                return 0
            return 0
        return await asyncio.to_thread(w)

    async def market(self, s, side, qty):
        def w():
            return self.c.create_order(
                MAP[s], "market", side, qty, None, {"category":"linear"}
            )
        return await asyncio.to_thread(w)

    async def close_all(self, s):
        def w():
            try:
                ps = self.c.fetch_positions([MAP[s]])
                for p in ps:
                    qty = abs(float(p.get("contracts",0)))
                    if qty > 0:
                        sd = "sell" if p["side"]=="long" else "buy"
                        self.c.create_order(
                            MAP[s],"market",sd,qty,None,{"reduceOnly":True}
                        )
            except: pass
        return await asyncio.to_thread(w)

# -----------------------------------------
# STATE
# -----------------------------------------
class State:
    def __init__(self):
        self.prices = {s:deque(maxlen=500) for s in SYMBOLS}
        self.trades = {s:deque(maxlen=2000) for s in SYMBOLS}
        self.candles = {s:deque(maxlen=120) for s in SYMBOLS}  # 2 minutes buffer

st = State()
ex = EX()

position = None
pending = None
cooldown_until = 0

# -----------------------------------------
# WEBSOCKET LOOP
# -----------------------------------------
async def ws_loop():
    topics = []
    for s in SYMBOLS:
        topics.append(f"publicTrade.{s}")

    print("WS Connecting…")

    while True:
        try:
            async with aiohttp.ClientSession() as ses:
                async with ses.ws_connect(WS_URL, heartbeat=20) as ws:
                    await ws.send_json({"op":"subscribe","args":topics})
                    print("WS Subscribed.")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            topic = data.get("topic")
                            if not topic: continue

                            if topic.startswith("publicTrade"):
                                sym = topic.split(".")[-1]
                                arr = data.get("data",[])
                                now = time.time()
                                for t in arr:
                                    px = float(t["p"])
                                    st.prices[sym].append(px)
                                    st.candles[sym].append(px)

        except Exception as e:
            print("WS Error:", e)
            await asyncio.sleep(1)


 # -----------------------------------------
# CORE REVERSAL LOGIC + FILTERS
# -----------------------------------------
def last_n_prices(sym, n=60):
    arr = list(st.prices[sym])
    if not arr:
        return []
    return arr[-n:]

def minute_candle_from_buffer(sym, window_secs=60):
    """
    Approximate a 1-minute candle from recent price buffer.
    Returns dict with open, high, low, close, body_size, wick_ratio.
    """
    prices = list(st.candles[sym])
    if not prices:
        return None
    # use last up-to window_secs samples (we store frequent updates; approximate)
    arr = prices[-int(min(len(prices), 60)):]
    if len(arr) < 2:
        return None
    open_p = arr[0]
    close_p = arr[-1]
    high = max(arr)
    low = min(arr)
    body = abs(close_p - open_p)
    upper_wick = max(0.0, high - max(open_p, close_p))
    lower_wick = max(0.0, min(open_p, close_p) - low)
    wick = max(upper_wick, lower_wick)
    wick_ratio = (wick / body) if body > 1e-9 else (1.0 if wick>0 else 0.0)
    candle = {
        "open": open_p,
        "close": close_p,
        "high": high,
        "low": low,
        "body": body,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "wick_ratio": wick_ratio
    }
    return candle

def is_bandwalk(sym):
    """
    Band-walk protection:
    If price repeatedly touches the same outer band without strong rejection,
    treat as band-walk (trend hugging band) and skip entries.
    Simple heuristic: last BB_PERIOD prices > mid and > 0.9*(upper band distance)
    """
    prices = last_n_prices(sym, BB_PERIOD)
    if len(prices) < BB_PERIOD:
        return False
    bb = bollinger(prices, BB_PERIOD, BB_STD)
    if not bb:
        return False
    upper, mid, lower = bb
    # count times price within 0.5% of upper
    near_upper = sum(1 for p in prices if p >= upper * 0.995)
    near_lower = sum(1 for p in prices if p <= lower * 1.005)
    # if many recent prices hug upper or lower, bandwalk
    if near_upper >= BB_PERIOD * 0.6 or near_lower >= BB_PERIOD * 0.6:
        return True
    return False

def volatility_spike(sym):
    """
    Volatility guard based on 1-min approx candle % move.
    """
    c = minute_candle_from_buffer(sym)
    if not c:
        return False
    if c["high"] <= 0:
        return False
    move_pct = (c["high"] - c["low"]) / ((c["high"] + c["low"]) / 2.0)
    return move_pct > VOLATILITY_THRESHOLD

def wick_strength_ok(sym, side):
    """
    Ensure there is a rejection wick on the band-touch candle.
    For short (upper band) require upper wick >= WICK_MIN_RATIO * candle_size
    For long (lower band) require lower wick >= WICK_MIN_RATIO * candle_size
    """
    c = minute_candle_from_buffer(sym)
    if not c:
        return False
    body = max(c["body"], 1e-9)
    if side == "sell":
        return (c["upper_wick"] / body) >= WICK_MIN_RATIO
    else:
        return (c["lower_wick"] / body) >= WICK_MIN_RATIO

def microflow_momentum(sym, window=0.4):
    """
    Sum buy - sell volume in last window seconds.
    Positive -> net buying, Negative -> net selling.
    Uses st.trades entries where each trade is {price,size,side,ts}.
    """
    now = time.time()
    trades = [t for t in st.trades[sym] if now - t["ts"] <= window]
    buy = sum(t["size"] for t in trades if t["side"]=="buy")
    sell = sum(t["size"] for t in trades if t["side"]=="sell")
    return buy - sell

def check_reversal_signal(sym):
    """
    Returns signal dict {"side":"buy"/"sell","mid":mid, "reason":...} or None.
    """
    prices = list(st.prices[sym])
    if len(prices) < BB_PERIOD + 3:
        return None

    bb = bollinger(prices, BB_PERIOD, BB_STD)
    if not bb:
        return None
    upper, midbb, lower = bb
    mid_price = prices[-1]

    # Band-walk or volatility guards
    if is_bandwalk(sym):
        # skip trading during band-walk
        return None
    if volatility_spike(sym):
        return None

    # Determine side by band touch (allow slight pierce)
    touched_upper = mid_price >= upper * 0.995
    touched_lower = mid_price <= lower * 1.005

    if not (touched_upper or touched_lower):
        return None

    # Microflow: require momentum fade against band
    mf = microflow_momentum(sym, window=0.4)
    # For short (upper) we want microflow to be negative (selling pressure) or cooling buys
    if touched_upper:
        if mf >= 0:
            return None
        side = "sell"
    else:
        if mf <= 0:
            return None
        side = "buy"

    # RSI curl: compare RSI now vs RSI without last sample
    r_now = rsi(prices, RSI_PERIOD)
    if r_now is None:
        return None
    r_prev = rsi(prices[:-1], RSI_PERIOD) if len(prices) > RSI_PERIOD+2 else None
    if r_prev is None:
        return None
    # For sell, RSI should curl down (r_now < r_prev). For buy, r_now > r_prev
    if side == "sell" and not (r_now < r_prev):
        return None
    if side == "buy" and not (r_now > r_prev):
        return None

    # Wick strength
    if not wick_strength_ok(sym, side):
        return None

    # All conditions passed
    return {"side": side, "mid": mid_price, "bb": (upper, midbb, lower)}

# -----------------------------------------
# ENTRY / CONFIRMATION
# -----------------------------------------
async def set_pending(sym, sig):
    global pending
    # respect cooldown
    if time.time() < cooldown_until:
        return
    pending = {
        "sym": sym,
        "side": sig["side"],
        "mid": sig["mid"],
        "ts": time.time()
    }
    print(f"[PENDING] {sym} {sig['side'].upper()} at {sig['mid']:.4f}")

async def confirm_and_entry():
    """
    Called frequently to confirm pending entry after CONFIRM_DELAY and open market order.
    """
    global pending, position, cooldown_until
    if not pending:
        return
    if time.time() - pending["ts"] < CONFIRM_DELAY:
        return

    sym = pending["sym"]
    side = pending["side"]

    # Re-evaluate signal at confirmation time
    sig = check_reversal_signal(sym)
    if not sig or sig["side"] != side:
        print(f"[PENDING DROPPED] {sym} {side} confirmation failed")
        pending = None
        return

    # Compute notional/qty
    try:
        eq = await ex.equity()
    except Exception as e:
        print("Equity fetch failed:", e)
        pending = None
        return
    if eq <= 0:
        pending = None
        return
    notional = eq * USE_EQUITY * LEVERAGE
    mid_price = sig["mid"]
    qty = notional / mid_price
    qty = round(qty, 3)
    if qty <= 0:
        pending = None
        return

    # Final pre-entry checks (one more microflow check)
    mf = microflow_momentum(sym, window=0.35)
    if side == "sell" and mf >= 0:
        print("[ENTRY BLOCKED] microflow flipped to buy")
        pending = None
        return
    if side == "buy" and mf <= 0:
        print("[ENTRY BLOCKED] microflow flipped to sell")
        pending = None
        return

    # Place market order
    try:
        ord = await ex.market(sym, side, qty)
        entry_price = float(ord.get("average") or ord.get("price") or mid_price)
    except Exception as e:
        print("Market order failed:", e)
        pending = None
        return

    # compute TP/SL
    if side == "buy":
        tp = entry_price * (1.0 + TP_PCT)
        sl = entry_price * (1.0 - SL_PCT)
    else:
        tp = entry_price * (1.0 - TP_PCT)
        sl = entry_price * (1.0 + SL_PCT)

    position = {
        "sym": sym,
        "side": side,
        "qty": qty,
        "entry": entry_price,
        "tp": tp,
        "sl": sl,
        "ts": time.time()
    }

    print(f"[ENTRY] {sym} {side.upper()} entry={entry_price:.4f} tp={tp:.4f} sl={sl:.4f}")
    pending = None

# -----------------------------------------
# WATCHDOG + MAIN LOOP
# -----------------------------------------
async def watchdog():
    """
    Monitors open position for TP/SL using mid-price from recent buffer.
    """
    global position, cooldown_until
    if not position:
        return
    sym = position["sym"]
    if len(st.prices[sym]) == 0:
        return
    mid = st.prices[sym][-1]
    side = position["side"]

    # TP
    if (side == "buy" and mid >= position["tp"]) or (side == "sell" and mid <= position["tp"]):
        print(f"[TP] {sym} hit TP at {mid:.4f}")
        try:
            await ex.close_all(sym)
        except:
            pass
        position = None
        cooldown_until = time.time() + COOLDOWN
        return

    # SL
    if (side == "buy" and mid <= position["sl"]) or (side == "sell" and mid >= position["sl"]):
        print(f"[SL] {sym} hit SL at {mid:.4f}")
        try:
            await ex.close_all(sym)
        except:
            pass
        position = None
        cooldown_until = time.time() + COOLDOWN
        return

async def main_loop():
    # warm up: set leverage
    for s in SYMBOLS:
        await ex.leverage(s)
    # start WS task
    ws_task = asyncio.create_task(ws_loop())

    try:
        while True:
            # if no position, scan for signals
            if not position:
                # iterate symbols quickly to find candidates
                for s in SYMBOLS:
                    try:
                        sig = check_reversal_signal(s)
                    except Exception as e:
                        sig = None
                    if sig:
                        await set_pending(s, sig)
                        # after setting first pending, break to allow confirmation stage
                        break
                # try confirming any pending
                await confirm_and_entry()
            else:
                # watchdog for active pos
                await watchdog()

            await asyncio.sleep(0.12)  # main tick ~120ms
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except:
            pass

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except:
        pass
    asyncio.run(main_loop())
