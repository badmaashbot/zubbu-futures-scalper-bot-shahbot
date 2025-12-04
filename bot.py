#!/usr/bin/env python3
"""
Combined bot.py
- Restored L2 orderbook parser (snapshot/delta)
- Skip logging always printed
- 5-pillar engine (burst, imbalance, microtrend, 1m SR breakout, 5m room)
- Option to require 3-of-5 pillars (REQUIRED_PILLARS)
- Dynamic ladder SL (50% lock at each TP milestone)
- Market entry only, TP posted as reduce-only post-only limit (maker)
- SL enforced by watchdog (market close on cross)
"""

import asyncio
import logging
import traceback
import sys
import os
import time
import json
import math
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
sys.excepthook = lambda t, v, tb: traceback.print_exception(t, v, tb)

import aiohttp
import ccxt  # sync ccxt used with asyncio.to_thread

# -------------------------
# CONFIG
# -------------------------
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

# Symbols we trade (Bybit WebSocket symbol names)
SYMBOLS_WS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
SYMBOL_MAP = {
    "BTCUSDT": "BTC/USDT:USDT",
    "ETHUSDT": "ETH/USDT:USDT",
    "BNBUSDT": "BNB/USDT:USDT",
    "SOLUSDT": "SOL/USDT:USDT",
    "DOGEUSDT": "DOGE/USDT:USDT",
}

if TESTNET:
    WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"
else:
    WS_URL = "wss://stream.bybit.com/v5/public/linear"

# Trading / risk
LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95
SL_PCT = 0.0035   # initial stop loss percent (0.35%)

# Engine parameters (tweakable)
RECENT_TRADE_WINDOW = 0.15  # 150 ms for burst
BURST_STRONG = 0.020
BURST_MEDIUM = 0.012

IMB_STRONG = 0.014
IMB_MEDIUM = 0.008

MICROTREND_UP = 0.00008
MICROTREND_DOWN = -0.00008

SR_1M_CANDLES = 12
SR_5M_CANDLES = 20

MIN_ROOM_PCT = 0.004  # 0.4% minimum room to next SR
MAX_SPREAD = 0.015    # <- you requested 0.015 (1.5%) safer default; edit as needed
BOOK_STALE_SEC = 6.0

# Ladder SL: 50% lock rules
# When price reaches milestone -> set SL to entry + 50% of milestone
# e.g. at +0.4%     => SL = entry + 0.2%
#       at +0.8%     => SL = entry + 0.4%
#       at +1.0%     => SL = entry + 0.5%

KILL_SWITCH_DD = 0.05
HEARTBEAT_IDLE_SEC = 1800

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# Pillar requirement: require N of 5 pillars to open trade (set to 3 per request)
REQUIRED_PILLARS = 3

# Telegram rate limit
_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0

# -------------------------
# UTIL
# -------------------------
def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def send_log(msg: str):
    print(msg, flush=True)

async def send_telegram(msg: str):
    global _last_tg_ts
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    now = time.time()
    if now - _last_tg_ts < TG_MIN_INTERVAL:
        return
    _last_tg_ts = now
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3.0)) as session:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            )
    except Exception:
        pass

def log_skip(sym: str, reason: str, feat: dict = None):
    feat = feat or {}
    print(
        f"[SKIP] {sym}: {reason} | spread={feat.get('spread',0):.6f} "
        f"imb={feat.get('imbalance',0):.4f} burst={feat.get('burst',0):.4f} "
        f"range={feat.get('range_pct',0):.6f}",
        flush=True
    )

# -------------------------
# Exchange wrapper (ccxt)
# -------------------------
class ExchangeClient:
    def __init__(self):
        cfg = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if TESTNET:
            cfg["urls"] = {
                "api": {
                    "public": "https://api-testnet.bybit.com",
                    "private": "https://api-testnet.bybit.com",
                }
            }
        self.client = ccxt.bybit(cfg)

    async def set_leverage(self, sym_ws: str, lev: int):
        symbol = SYMBOL_MAP[sym_ws]
        def _work():
            try:
                self.client.set_leverage(lev, symbol, params={"category": "linear"})
            except Exception:
                pass
        await asyncio.to_thread(_work)

    async def fetch_equity(self) -> float:
        def _work():
            try:
                bal = self.client.fetch_balance()
                total = safe_float(bal.get("USDT", {}).get("total"), 0.0) or 0.0
                upnl = 0.0
                try:
                    positions = self.client.fetch_positions()
                    for p in positions:
                        upnl += safe_float(p.get("unrealizedPnl"), 0.0) or 0.0
                except Exception:
                    pass
                return total + upnl
            except Exception:
                return 0.0
        return await asyncio.to_thread(_work)

    async def create_market_order(self, sym_ws: str, side: str, qty: float, reduce_only: bool=False):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True
        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)
        return await asyncio.to_thread(_work)

    async def create_limit_order(self, sym_ws: str, side: str, qty: float, price: float, reduce_only=False, post_only=False):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear", "timeInForce": "GTC"}
        if reduce_only:
            params["reduceOnly"] = True
        if post_only:
            params["timeInForce"] = "PostOnly"
        def _work():
            return self.client.create_order(symbol, "limit", side, qty, price, params)
        return await asyncio.to_thread(_work)

    async def close_position_market(self, sym_ws: str):
        symbol = SYMBOL_MAP[sym_ws]
        def _work():
            try:
                positions = self.client.fetch_positions([symbol])
            except Exception:
                return
            for p in positions:
                contracts = safe_float(p.get("contracts"), 0.0) or 0.0
                if contracts == 0:
                    continue
                side = "sell" if p.get("side") == "long" else "buy"
                params = {"category": "linear", "reduceOnly": True}
                try:
                    self.client.create_order(symbol, "market", side, abs(contracts), None, params)
                except Exception:
                    pass
        return await asyncio.to_thread(_work)

# -------------------------
# Data structures & MarketState
# -------------------------
@dataclass
class Position:
    symbol_ws: str
    side: str
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    opened_ts: float

class MarketState:
    def __init__(self):
        # keep your old parser behavior: L2 book stored as dict price->qty for top levels
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}
        self.microtrend = {s: deque(maxlen=60) for s in SYMBOLS_WS}
        self.candles_1m = {s: deque(maxlen=SR_1M_CANDLES * 3) for s in SYMBOLS_WS}
        self.candles_5m = {s: deque(maxlen=SR_5M_CANDLES * 3) for s in SYMBOLS_WS}
        self._last_1m_ts = {s: 0 for s in SYMBOLS_WS}
        self._last_5m_ts = {s: 0 for s in SYMBOLS_WS}

    # ORIGINAL-LIKE ORDERBOOK PARSER (snapshot/delta) — DO NOT CHANGE
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]
        ts_raw = data.get("ts")
        ts = safe_float(ts_raw, time.time() * 1000.0) / 1000.0
        book["ts"] = ts

        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()
            for px, qty in data.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None:
                    continue
                book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None:
                    continue
                book["asks"][p] = q
            return

        # incremental updates preserved exactly (delete/update/insert)
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None:
                    continue
                if q == 0:
                    book["bids"].pop(p, None)
                else:
                    book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None:
                    continue
                if q == 0:
                    book["asks"].pop(p, None)
                else:
                    book["asks"][p] = q

    def add_trade(self, symbol: str, trade: dict):
        self.trades[symbol].append(trade)

    def get_best_bid_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        best_bid = max(book["bids"].keys()); best_ask = min(book["asks"].keys())
        return best_bid, best_ask

    def compute_features(self, symbol: str) -> Optional[dict]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC:
            return None

        bids_sorted = sorted(book["bids"].items(), key=lambda x: -x[0])[:5]
        asks_sorted = sorted(book["asks"].items(), key=lambda x: x[0])[:5]
        bid_vol = sum(q for _, q in bids_sorted)
        ask_vol = sum(q for _, q in asks_sorted)
        if bid_vol + ask_vol == 0:
            return None
        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)

        best_bid = bids_sorted[0][0]; best_ask = asks_sorted[0][0]
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None
        spread = abs(best_ask - best_bid) / mid

        # RECENT TRADES BURST
        cutoff = now - RECENT_TRADE_WINDOW
        burst = 0.0
        last_price = None
        recent_prices = []
        for t in reversed(self.trades[symbol]):
            if t["ts"] < cutoff:
                break
            sz = t["size"]
            if t["side"] == "buy":
                burst += sz
            else:
                burst -= sz
            last_price = t["price"]
            recent_prices.append(t["price"])
        if last_price is None:
            return None
        if len(recent_prices) >= 2:
            high = max(recent_prices); low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0
        else:
            rng = 0.0

        # microtrend + candle update (from mid)
        self.microtrend[symbol].append(mid)
        _update_candles_from_mid(self, symbol, now, mid)

        return {"mid": mid, "spread": spread, "imbalance": imbalance, "burst": burst, "range_pct": rng}

# -------------------------
# candles/microtrend helpers
# -------------------------
def compute_microtrend(mid_list: deque):
    if len(mid_list) < 6:
        return 0.0
    first = mid_list[0]; last = mid_list[-1]
    if first == 0:
        return 0.0
    return (last - first) / first

def last_swing_levels(candle_deque: deque, lookback: int):
    if not candle_deque or len(candle_deque) < 2:
        return None, None
    arr = list(candle_deque)[-lookback:]
    highs = [c["high"] for c in arr]; lows = [c["low"] for c in arr]
    return max(highs), min(lows)

def _update_candles_from_mid(market: MarketState, symbol: str, ts: float, mid: float):
    minute = int(ts // 60) * 60
    last1 = market._last_1m_ts[symbol]
    if minute != last1:
        market.candles_1m[symbol].append({"ts": minute, "open": mid, "high": mid, "low": mid, "close": mid})
        market._last_1m_ts[symbol] = minute
    else:
        c1 = market.candles_1m[symbol][-1]
        c1["high"] = max(c1["high"], mid)
        c1["low"] = min(c1["low"], mid)
        c1["close"] = mid

    five_min = int(ts // 300) * 300
    last5 = market._last_5m_ts[symbol]
    if five_min != last5:
        market.candles_5m[symbol].append({"ts": five_min, "open": mid, "high": mid, "low": mid, "close": mid})
        market._last_5m_ts[symbol] = five_min
    else:
        c5 = market.candles_5m[symbol][-1]
        c5["high"] = max(c5["high"], mid)
        c5["low"] = min(c5["low"], mid)
        c5["close"] = mid

# -------------------------
# ScalperBot core
# -------------------------
class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time = time.time()
        self.last_heartbeat_ts = 0.0

    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        send_log(f"[INIT] Equity: {eq:.2f} USDT")
        await send_telegram(f"🟢 Bot started. Equity: {eq:.2f} USDT.")
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        send_log("[INIT] Leverage set for all symbols.")

    async def maybe_kill_switch(self):
        if self.start_equity is None:
            return
        eq = await self.exchange.fetch_equity()
        if self.start_equity <= 0:
            return
        dd = (self.start_equity - eq) / self.start_equity
        if dd >= KILL_SWITCH_DD:
            if self.position:
                await self.exchange.close_position_market(self.position.symbol_ws)
                await send_telegram(f"🚨 Kill-switch: equity drawdown {dd*100:.2f}%. Position closed.")
                self.position = None
            raise SystemExit("Kill-switch triggered")

    async def open_position(self, sym_ws: str, feat: dict):
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0:
            return
        mid = feat["mid"]; imb = feat["imbalance"]; burst = feat["burst"]
        direction = feat.get("direction")
        if direction not in ("buy", "sell"):
            if imb > 0 and burst > 0:
                direction = "buy"
            elif imb < 0 and burst < 0:
                direction = "sell"
            else:
                log_skip(sym_ws, "no clear direction at entry", feat); return

        notional = equity * EQUITY_USE_FRACTION * LEVERAGE
        if mid <= 0:
            return
        qty = notional / mid
        min_qty = MIN_QTY_MAP.get(sym_ws, 0.0)
        if qty < min_qty:
            send_log(f"[SKIP ENTRY] {sym_ws}: qty {qty} < min_qty {min_qty}"); return
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        burst_mag = abs(burst)
        if burst_mag >= BURST_STRONG:
            score_tp = 0.01
        elif burst_mag >= BURST_MEDIUM:
            score_tp = 0.006
        else:
            score_tp = 0.004

        sl_pct = SL_PCT
        if direction == "buy":
            tp_price = mid * (1.0 + score_tp)
            sl_price = mid * (1.0 - sl_pct)
            side = "buy"
        else:
            tp_price = mid * (1.0 - score_tp)
            sl_price = mid * (1.0 + sl_pct)
            side = "sell"

        # market entry
        try:
            order = await self.exchange.create_market_order(sym_ws, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            send_log(f"[ENTRY ERROR] {sym_ws}: {e}"); await send_telegram(f"❌ Entry failed for {sym_ws}: {e}"); return

        # TP place as maker reduce-only post-only
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(sym_ws, opp_side, qty, tp_price, reduce_only=True, post_only=True)
        except Exception as e:
            send_log(f"[TP ERROR] {sym_ws}: {e}"); await send_telegram(f"⚠️ TP order placement failed for {sym_ws}: {e}")

        self.position = Position(symbol_ws=sym_ws, side=side, qty=qty, entry_price=entry_price, tp_price=tp_price, sl_price=sl_price, opened_ts=time.time())
        self.last_trade_time = time.time()
        send_log(f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.6f} TP={tp_price:.6f} SL≈{sl_price:.6f}")
        await send_telegram(f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.6f} TP={tp_price:.6f} SL≈{sl_price:.6f}")

    # WATCHDOG: enforce ladder SL and actual closing (market) if SL crossed
    async def watchdog_position(self):
        if not self.position:
            return
        sym = self.position.symbol_ws
        feat = self.mkt.compute_features(sym)
        if not feat:
            return
        mid = feat["mid"]
        if mid <= 0:
            return
        pos = self.position
        EP = pos.entry_price

        # compute pnl percent (long)
        if pos.side == "buy":
            pnl_pct = (mid - EP) / EP
        else:
            pnl_pct = (EP - mid) / EP

        # initial losing SL enforcement (if price went against)
        if pnl_pct < 0 and abs(pnl_pct) >= SL_PCT * 1.1:
            await self.exchange.close_position_market(sym)
            send_log(f"[SL] {sym} {pos.side.upper()} entry={EP:.6f} now={mid:.6f} DD={pnl_pct*100:.2f}%")
            await send_telegram(f"🛑 SL (watchdog) {sym} {pos.side.upper()} entry={EP:.6f} now={mid:.6f} DD={pnl_pct*100:.2f}%")
            self.position = None
            return

        # LADDER RULES: 50% lock at milestone
        # milestone 0.4% -> set SL to entry + 0.2%
        # milestone 0.8% -> set SL to entry + 0.4%
        # milestone 1.0% -> set SL to entry + 0.5%

        # compute target SLs for long/short
        if pos.side == "buy":
            t1 = EP * (1.0 + 0.002)   # at 0.4% -> 0.2%
            t2 = EP * (1.0 + 0.004)   # at 0.8% -> 0.4%
            t3 = EP * (1.0 + 0.005)   # at 1.0% -> 0.5%
        else:
            t1 = EP * (1.0 - 0.002)
            t2 = EP * (1.0 - 0.004)
            t3 = EP * (1.0 - 0.005)

        # apply ladder updates
        # milestone 0.4%
        if pnl_pct >= 0.004:
            # if move to this hasn't been applied yet, move SL up
            if (pos.side == "buy" and pos.sl_price < t1) or (pos.side == "sell" and pos.sl_price > t1):
                pos.sl_price = t1
                send_log(f"[MOVE SL] {sym} moved SL to 50% of 0.4 (=> {pos.sl_price:.6f})")

        # milestone 0.8%
        if pnl_pct >= 0.008:
            if (pos.side == "buy" and pos.sl_price < t2) or (pos.side == "sell" and pos.sl_price > t2):
                pos.sl_price = t2
                send_log(f"[MOVE SL] {sym} moved SL to 50% of 0.8 (=> {pos.sl_price:.6f})")

        # milestone 1.0%
        if pnl_pct >= 0.010:
            if (pos.side == "buy" and pos.sl_price < t3) or (pos.side == "sell" and pos.sl_price > t3):
                pos.sl_price = t3
                send_log(f"[MOVE SL] {sym} moved SL to 50% of 1.0 (=> {pos.sl_price:.6f})")

        # final check: if price crosses SL -> close market (taker)
        if pos.side == "buy" and mid <= pos.sl_price:
            await self.exchange.close_position_market(sym)
            send_log(f"[SL HIT] {sym} final SL at {pos.sl_price:.6f} (pnl={pnl_pct*100:.3f}%)")
            self.position = None
            return
        if pos.side == "sell" and mid >= pos.sl_price:
            await self.exchange.close_position_market(sym)
            send_log(f"[SL HIT] {sym} final SL at {pos.sl_price:.6f} (pnl={pnl_pct*100:.3f}%)")
            self.position = None
            return

    async def maybe_heartbeat(self):
        now = time.time()
        if now - self.last_trade_time < HEARTBEAT_IDLE_SEC:
            return
        if now - self.last_heartbeat_ts < HEARTBEAT_IDLE_SEC:
            return
        self.last_heartbeat_ts = now
        eq = await self.exchange.fetch_equity()
        send_log(f"[HEARTBEAT] idle, equity={eq:.2f}")
        await send_telegram(f"💤 Bot idle {HEARTBEAT_IDLE_SEC/60:.0f} min. Equity={eq:.2f} USDT")

    # EVAL / ENTRY: use pillars and allow REQUIRED_PILLARS to trigger
    async def eval_symbols_and_maybe_enter(self):
        if self.position is not None:
            return

        now = time.time()
        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                log_skip(sym, "no usable book/trades (feat==None)", {"spread":0,"imbalance":0,"burst":0,"range_pct":0})
                continue

            mid = feat["mid"]; burst = feat["burst"]; imb = feat["imbalance"]; spread = feat["spread"]; rng = feat["range_pct"]

            # spread safety
            if spread <= 0 or spread > MAX_SPREAD:
                log_skip(sym, f"spread {spread:.6f} > MAX_SPREAD {MAX_SPREAD}", feat); continue

            # pillar checks
            pillars_true = 0
            pillar_reasons = []

            # Burst pillar
            if burst >= 0:
                burst_mag = burst; burst_dir = "buy"
            else:
                burst_mag = -burst; burst_dir = "sell"
            if burst_mag >= BURST_MEDIUM:
                pillars_true += 1
                pillar_reasons.append("burst")

            # Imbalance pillar
            if imb >= 0:
                imb_mag = imb; imb_dir = "buy"
            else:
                imb_mag = -imb; imb_dir = "sell"
            if imb_mag >= IMB_MEDIUM:
                pillars_true += 1
                pillar_reasons.append("imbalance")

            # Microtrend pillar
            slope = compute_microtrend(self.mkt.microtrend[sym])
            if (burst_dir == "buy" and slope >= MICROTREND_UP) or (burst_dir == "sell" and slope <= MICROTREND_DOWN):
                pillars_true += 1
                pillar_reasons.append("microtrend")

            # 1m SR breakout pillar
            h1, l1 = last_swing_levels(self.mkt.candles_1m[sym], SR_1M_CANDLES)
            sr_ok = False
            if h1 and l1:
                if burst_dir == "buy" and mid > h1:
                    sr_ok = True; pillars_true += 1; pillar_reasons.append("1m_break")
                if burst_dir == "sell" and mid < l1:
                    sr_ok = True; pillars_true += 1; pillar_reasons.append("1m_break")

            # 5m room pillar
            h5, l5 = last_swing_levels(self.mkt.candles_5m[sym], SR_5M_CANDLES)
            room_ok = False
            if h5 and l5:
                if burst_dir == "buy":
                    room = (h5 - mid) / mid
                    if room >= MIN_ROOM_PCT:
                        room_ok = True; pillars_true += 1; pillar_reasons.append("5m_room")
                else:
                    room = (mid - l5) / mid
                    if room >= MIN_ROOM_PCT:
                        room_ok = True; pillars_true += 1; pillar_reasons.append("5m_room")

            # direction agreement required: burst_dir and imb_dir must match for direction
            if burst_dir != imb_dir:
                # they disagree: treat as partial (still count if other pillars compensate)
                pass

            # We allow opening if pillars_true >= REQUIRED_PILLARS AND direction not ambiguous
            if pillars_true < REQUIRED_PILLARS:
                log_skip(sym, f"not enough pillars ({pillars_true}/{REQUIRED_PILLARS})", {"spread":spread,"imbalance":imb,"burst":burst,"range_pct":rng})
                continue

            # direction determination
            if burst_dir == imb_dir:
                direction = "buy" if burst_dir == "buy" else "sell"
            else:
                # if they disagree but pillars satisfied, choose burst_dir as tie-breaker
                direction = "buy" if burst_dir == "buy" else "sell"

            # anti-spam cooldown
            if now - self.mkt.last_signal_ts.get(sym, 0) < 0.7:
                continue
            self.mkt.last_signal_ts[sym] = now

            # final fresh guard: require some recent trades
            if len(self.mkt.trades[sym]) < 3:
                log_skip(sym, "not enough recent trades for confidence", {"spread":spread,"imbalance":imb,"burst":burst,"range_pct":rng})
                continue

            # Good candidate — log debug and open
            send_log(f"[ENTRY DEBUG] {sym} direction={direction} pillars={pillar_reasons} pillars_true={pillars_true} mid={mid:.6f} burst={burst:.4f} imb={imb:.4f} slope={slope:.6f}")
            feat["direction"] = direction
            await self.open_position(sym, feat)
            return  # only one position at a time

# -------------------------
# WEBSOCKET (public) loop
# -------------------------
ws_ready: Dict[str,bool] = {s: False for s in SYMBOLS_WS}

async def ws_loop(mkt: MarketState):
    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")

    send_log("⚡ WS loop started... connecting...")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, receive_timeout=40, heartbeat=20) as ws:
                    send_log("📡 Connected to WS server, subscribing...")
                    await ws.send_json({"op": "subscribe", "args": topics})
                    for sym in SYMBOLS_WS:
                        ws_ready[sym] = True
                    await send_telegram("📡 WS Connected: All symbols")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue
                            if data.get("success") is True:
                                continue
                            topic = data.get("topic")
                            if not topic:
                                continue

                            # ORDERBOOK: identical to your original parser (snapshot + delta)
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                if isinstance(payload, list):
                                    payload = payload[0]
                                if isinstance(payload, dict):
                                    mkt.update_book(sym, payload)

                            # TRADES
                            elif topic.startswith("publicTrade"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                trades = payload if isinstance(payload, list) else [payload]
                                now_ts = time.time()
                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(t.get("v") or t.get("q") or t.get("size"))
                                    side = (t.get("S") or t.get("side") or "Buy").lower()
                                    if price is None or qty is None:
                                        continue
                                    # append trade
                                    mkt.add_trade(sym, {"price": price, "size": qty, "side": "buy" if side == "buy" else "sell", "ts": now_ts})
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            send_log("⚠ WS ERROR — reconnecting")
                            break
        except Exception as e:
            send_log(f"❌ WS Loop Crashed: {e}")
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)

# -------------------------
# debug console (tmux)
# -------------------------
def debug_console(mkt: MarketState, bot: ScalperBot):
    help_text = ("[DEBUG] Commands:\n"
                 "  ws      - show websocket ready flags\n"
                 "  book    - show last orderbook timestamps\n"
                 "  trades  - show count of recent trades per symbol\n"
                 "  pos     - show current open position\n"
                 "  help    - show this message\n")
    print(help_text, flush=True)
    while True:
        try:
            cmd = input("")
        except EOFError:
            return
        cmd = cmd.strip().lower()
        if cmd == "ws":
            print("[DEBUG] ws_ready =", ws_ready, flush=True)
        elif cmd == "book":
            ts_map = {s: mkt.books[s]["ts"] for s in SYMBOLS_WS}
            print("[DEBUG] book ts =", ts_map, flush=True)
        elif cmd == "trades":
            counts = {s: len(mkt.trades[s]) for s in SYMBOLS_WS}
            print("[DEBUG] trades len =", counts, flush=True)
        elif cmd == "pos":
            print("[DEBUG] position =", bot.position, flush=True)
        elif cmd == "help":
            print(help_text, flush=True)
        elif cmd == "":
            continue
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)

# -------------------------
# main
# -------------------------
async def main():
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

    try:
        import threading
        threading.Thread(target=debug_console, args=(mkt, bot), daemon=True).start()
    except Exception:
        pass

    await bot.init_equity_and_leverage()
    ws_task = asyncio.create_task(ws_loop(mkt))

    try:
        while True:
            await bot.maybe_kill_switch()
            await bot.eval_symbols_and_maybe_enter()
            await bot.watchdog_position()
            await asyncio.sleep(1.0)
    except Exception as e:
        logging.exception(">>> RUN CRASH <<<")
        raise
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except Exception:
            pass

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
