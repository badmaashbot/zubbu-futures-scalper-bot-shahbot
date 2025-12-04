#!/usr/bin/env python3
"""
bot.py - 5-pillar engine with "3-of-5" entry & 50% ladder SL locks
- Keeps original orderbook parser behavior (no parser rewrite)
- Skip logs enabled
- MAX_SPREAD = 0.015
- Dynamic TP (0.4 / 0.6 / 1.0) by burst
- Ladder SL: when +0.4 -> lock 50% profit (SL -> +0.2), +0.8 -> SL +0.4, +1.0 -> SL +0.5
- No forced market close at ladder stages (only SL moved). Watchdog will close when SL hit.
- Public WS as before. Private WS placeholder commented.
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
import ccxt

# -------- ENV / CONFIG --------
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

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

LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95
SL_PCT = 0.0035

# 5-PILLAR / SCALPER CONFIG
RECENT_TRADE_WINDOW = 0.15  # 150ms
BURST_STRONG = 0.020
BURST_MEDIUM = 0.012
IMB_STRONG = 0.014
IMB_MEDIUM = 0.008
MICROTREND_UP = 0.00008
MICROTREND_DOWN = -0.00008
SR_1M_CANDLES = 12
SR_5M_CANDLES = 20
MIN_ROOM_PCT = 0.004  # 0.4% room required
MAX_SPREAD = 0.015    # user requested 1.5% safety
BOOK_STALE_SEC = 6.0
KILL_SWITCH_DD = 0.05
HEARTBEAT_IDLE_SEC = 1800

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0

# -------- TELEGRAM (rate-limited) --------
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
            await session.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                               data={"chat_id": TG_CHAT_ID, "text": msg})
    except Exception:
        pass

# -------- UTILS --------
def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def log_skip(sym: str, reason: str, feat: dict = None):
    feat = feat or {}
    print(f"[SKIP] {sym}: {reason} | spread={feat.get('spread',0):.6f}, "
          f"imb={feat.get('imbalance',0):.4f}, burst={feat.get('burst',0):.4f}, "
          f"range={feat.get('range_pct',0):.6f}", flush=True)

# -------- EXCHANGE WRAPPER --------
class ExchangeClient:
    def __init__(self):
        cfg = {"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True, "options": {"defaultType": "swap"}}
        if TESTNET:
            cfg["urls"] = {"api": {"public": "https://api-testnet.bybit.com", "private": "https://api-testnet.bybit.com"}}
        self.client = ccxt.bybit(cfg)

    async def set_leverage(self, sym_ws: str, lev: int) -> None:
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

    async def create_market_order(self, sym_ws: str, side: str, qty: float, reduce_only: bool = False):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True
        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)
        return await asyncio.to_thread(_work)

    async def create_limit_order(self, sym_ws: str, side: str, qty: float, price: float, reduce_only: bool = False, post_only: bool = False):
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

# -------- DATA STRUCTURES --------
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
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}
        self.microtrend = {s: deque(maxlen=30) for s in SYMBOLS_WS}
        self.candles_1m = {s: deque(maxlen=SR_1M_CANDLES * 3) for s in SYMBOLS_WS}
        self.candles_5m = {s: deque(maxlen=SR_5M_CANDLES * 3) for s in SYMBOLS_WS}
        self._last_1m_ts = {s: 0 for s in SYMBOLS_WS}
        self._last_5m_ts = {s: 0 for s in SYMBOLS_WS}

    # KEEP your original orderbook parser semantics (snapshot & delta applied)
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
                if p is None or q is None: continue
                book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None: continue
                book["asks"][p] = q
            return
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None: continue
                if q == 0:
                    book["bids"].pop(p, None)
                else:
                    book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None: continue
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
        best_bid = max(book["bids"].keys())
        best_ask = min(book["asks"].keys())
        return best_bid, best_ask

    def compute_features(self, symbol: str) -> Optional[dict]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC:
            log_skip(symbol, "book stale", {"spread": 0, "imbalance": 0, "burst": 0, "range_pct": 0})
            return None

        bids_sorted = sorted(book["bids"].items(), key=lambda x: -x[0])[:5]
        asks_sorted = sorted(book["asks"].items(), key=lambda x: x[0])[:5]
        bid_vol = sum(q for _, q in bids_sorted)
        ask_vol = sum(q for _, q in asks_sorted)
        if bid_vol + ask_vol == 0:
            return None
        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)

        best_bid = bids_sorted[0][0]
        best_ask = asks_sorted[0][0]
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None

        spread = abs(best_ask - best_bid) / mid
        if spread > MAX_SPREAD:
            log_skip(symbol, f"spread {spread:.6f} > MAX_SPREAD {MAX_SPREAD}", {"spread": spread, "imbalance": imbalance, "burst": 0, "range_pct": 0})
            # we keep spread as safety but do not auto-block in most cases; here we log and skip.
            return None

        cutoff = now - RECENT_TRADE_WINDOW
        burst = 0.0
        last_price = None
        recent_prices: List[float] = []
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
            log_skip(symbol, "no recent trades", {"spread": spread, "imbalance": imbalance, "burst": 0, "range_pct": 0})
            return None

        if len(recent_prices) >= 2:
            high = max(recent_prices); low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0
        else:
            rng = 0.0

        # microtrend & candles
        if symbol not in self.microtrend:
            self.microtrend[symbol] = deque(maxlen=30)
        self.microtrend[symbol].append(mid)
        _update_candles_from_mid(self, symbol, now, mid)

        return {"mid": mid, "spread": spread, "imbalance": imbalance, "burst": burst, "range_pct": rng}

# -------- HELPERS (microtrend + SR) --------
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
    highs = [c["high"] for c in arr]
    lows = [c["low"] for c in arr]
    return max(highs), min(lows)

def _update_candles_from_mid(market: MarketState, symbol: str, ts: float, mid: float):
    minute = int(ts // 60) * 60
    last1 = market._last_1m_ts[symbol]
    if minute != last1:
        market.candles_1m[symbol].append({"ts": minute, "open": mid, "high": mid, "low": mid, "close": mid})
        market._last_1m_ts[symbol] = minute
    else:
        c1 = market.candles_1m[symbol][-1]; c1["high"] = max(c1["high"], mid); c1["low"] = min(c1["low"], mid); c1["close"] = mid
    five_min = int(ts // 300) * 300
    last5 = market._last_5m_ts[symbol]
    if five_min != last5:
        market.candles_5m[symbol].append({"ts": five_min, "open": mid, "high": mid, "low": mid, "close": mid})
        market._last_5m_ts[symbol] = five_min
    else:
        c5 = market.candles_5m[symbol][-1]; c5["high"] = max(c5["high"], mid); c5["low"] = min(c5["low"], mid); c5["close"] = mid

# -------- CORE BOT --------
class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange; self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time = time.time(); self.last_heartbeat_ts = 0.0

    async def init_equity_and_leverage(self):
        try:
            eq = await self.exchange.fetch_equity()
        except Exception:
            logging.exception(">>> FIRST EQUITY CALL REJECTED <<<"); raise
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT", flush=True)
        await send_telegram(f"🟢 Bot started. Equity: {eq:.2f} USDT. Kill at {KILL_SWITCH_DD*100:.1f}% DD.")
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.", flush=True)
        await send_telegram("⚙️ Leverage set for all symbols.")

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
        if mid <= 0: return
        qty = notional / mid
        min_qty = MIN_QTY_MAP.get(sym_ws, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym_ws}: qty {qty} < min_qty {min_qty}", flush=True); return
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0: return

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

        try:
            order = await self.exchange.create_market_order(sym_ws, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"❌ Entry failed for {sym_ws}: {e}")
            return

        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(sym_ws, opp_side, qty, tp_price, reduce_only=True, post_only=True)
        except Exception as e:
            print(f"[TP ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"⚠️ TP order placement failed for {sym_ws}: {e}")

        self.position = Position(symbol_ws=sym_ws, side=side, qty=qty, entry_price=entry_price, tp_price=tp_price, sl_price=sl_price, opened_ts=time.time())
        self.last_trade_time = time.time()
        print(f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} TP={tp_price:.4f} SL≈{sl_price:.4f}", flush=True)
        await send_telegram(f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} TP={tp_price:.4f} SL≈{sl_price:.4f}")

    # WATCHDOG with ladder SL (50% profit lock)
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

        # compute pnl percent (long positive when price up)
        if pos.side == "buy":
            pnl_pct = (mid - EP) / EP
        else:
            pnl_pct = (EP - mid) / EP

        # initial hard-loss SL (unchanged)
        if pnl_pct < 0 and abs(pnl_pct) >= SL_PCT * 1.1:
            await self.exchange.close_position_market(sym)
            print(f"[SL] {sym} LOSS exit {pnl_pct*100:.3f}%", flush=True)
            await send_telegram(f"🛑 SL (watchdog) {sym} loss={pnl_pct*100:.3f}%")
            self.position = None
            return

        # LADDER RULES: 50% profit lock at thresholds
        # When price reaches +0.4% -> set SL to entry + 50% of profit (i.e. EP + 0.2%)
        # When price reaches +0.8% -> SL -> EP + 0.4%
        # When price reaches +1.0% -> SL -> EP + 0.5%
        # For shorts, symmetric

        # helper to compute target SL given current realized profit pct and 50% lock
        def ladder_sl_for(pct):
            # returns price to set as SL (long)
            return EP * (1.0 + pct * 0.5)

        # stage 0.4
        if pnl_pct >= 0.004:
            # target SL at +0.2%
            target = ladder_sl_for(0.004) if pos.side == "buy" else EP * (1.0 - 0.004 * 0.5)
            # for long: EP*(1+0.002)
            if pos.side == "buy":
                if target > pos.sl_price:
                    pos.sl_price = target
                    print(f"[MOVE_SL] {sym} reached +0.4% -> SL moved to +0.20% (pos.sl={pos.sl_price:.6f})", flush=True)
            else:
                if target < pos.sl_price:
                    pos.sl_price = target
                    print(f"[MOVE_SL] {sym} reached -0.4% -> SL moved to -0.20% (pos.sl={pos.sl_price:.6f})", flush=True)

        # stage 0.8
        if pnl_pct >= 0.008:
            target = ladder_sl_for(0.008) if pos.side == "buy" else EP * (1.0 - 0.008 * 0.5)
            # EP*(1+0.004)
            if pos.side == "buy":
                if target > pos.sl_price:
                    pos.sl_price = target
                    print(f"[MOVE_SL] {sym} reached +0.8% -> SL moved to +0.40% (pos.sl={pos.sl_price:.6f})", flush=True)
            else:
                if target < pos.sl_price:
                    pos.sl_price = target
                    print(f"[MOVE_SL] {sym} reached -0.8% -> SL moved to -0.40% (pos.sl={pos.sl_price:.6f})", flush=True)

        # stage 1.0
        if pnl_pct >= 0.010:
            target = ladder_sl_for(0.010) if pos.side == "buy" else EP * (1.0 - 0.010 * 0.5)
            # EP*(1+0.005)
            if pos.side == "buy":
                if target > pos.sl_price:
                    pos.sl_price = target
                    print(f"[MOVE_SL] {sym} reached +1.0% -> SL moved to +0.50% (pos.sl={pos.sl_price:.6f})", flush=True)
            else:
                if target < pos.sl_price:
                    pos.sl_price = target
                    print(f"[MOVE_SL] {sym} reached -1.0% -> SL moved to -0.50% (pos.sl={pos.sl_price:.6f})", flush=True)

        # final check: if price hits SL (moved or original) -> close at market (taker fee applies)
        if pos.side == "buy" and mid <= pos.sl_price:
            await self.exchange.close_position_market(sym)
            print(f"[SL HIT] {sym} SL hit. Booked pnl approx={(pos.sl_price - EP)/EP*100:.3f}%", flush=True)
            await send_telegram(f"🔒 SL HIT {sym} booked {(pos.sl_price - EP)/EP*100:.3f}%")
            self.position = None
            return
        if pos.side == "sell" and mid >= pos.sl_price:
            await self.exchange.close_position_market(sym)
            print(f"[SL HIT] {sym} SL hit. Booked pnl approx={(EP - pos.sl_price)/EP*100:.3f}%", flush=True)
            await send_telegram(f"🔒 SL HIT {sym} booked {(EP - pos.sl_price)/EP*100:.3f}%")
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
        print(f"[HEARTBEAT] idle, equity={eq:.2f}", flush=True)
        await send_telegram(f"💤 Bot idle {HEARTBEAT_IDLE_SEC/60:.0f} min. Equity={eq:.2f} USDT")

    # 3-of-5 entry: count pillars and accept if >=3 align
    async def eval_symbols_and_maybe_enter(self):
        if self.position is not None:
            return
        now = time.time()
        chosen = None
        chosen_feat = None

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            mid = feat["mid"]; burst = feat["burst"]; imb = feat["imbalance"]
            # pillar flags
            pillars = {"burst": False, "imbalance": False, "micro": False, "sr1": False, "sr5": False}
            dir_vote = {"buy": 0, "sell": 0}

            # burst pillar
            if burst >= 0:
                bmag = burst; bdir = "buy"
            else:
                bmag = -burst; bdir = "sell"
            if bmag >= BURST_MEDIUM:
                pillars["burst"] = True; dir_vote[bdir] += 1
            else:
                log_skip(sym, f"burst too small {bmag:.6f}", feat)

            # imbalance pillar
            if imb >= 0:
                imag = imb; idir = "buy"
            else:
                imag = -imb; idir = "sell"
            if imag >= IMB_MEDIUM:
                pillars["imbalance"] = True; dir_vote[idir] += 1
            else:
                log_skip(sym, f"imbalance too small {imag:.6f}", feat)

            # microtrend pillar
            slope = compute_microtrend(self.mkt.microtrend[sym])
            if slope >= MICROTREND_UP:
                pillars["micro"] = True; dir_vote["buy"] += 1
            elif slope <= MICROTREND_DOWN:
                pillars["micro"] = True; dir_vote["sell"] += 1
            else:
                log_skip(sym, f"microtrend flat {slope:.8f}", feat)

            # 1m SR breakout pillar
            h1, l1 = last_swing_levels(self.mkt.candles_1m[sym], SR_1M_CANDLES)
            if h1 and l1:
                if mid > h1:
                    pillars["sr1"] = True; dir_vote["buy"] += 1
                elif mid < l1:
                    pillars["sr1"] = True; dir_vote["sell"] += 1
                else:
                    log_skip(sym, "1m not broken", feat)
            else:
                # not enough candles yet => don't penalize heavily: treat as neutral (no pillar)
                log_skip(sym, "1m SR missing", feat)

            # 5m room pillar
            h5, l5 = last_swing_levels(self.mkt.candles_5m[sym], SR_5M_CANDLES)
            if h5 and l5:
                # next level room check
                room_buy = (h5 - mid) / mid
                room_sell = (mid - l5) / mid
                if room_buy >= MIN_ROOM_PCT:
                    pillars["sr5"] = True; dir_vote["buy"] += 1
                elif room_sell >= MIN_ROOM_PCT:
                    pillars["sr5"] = True; dir_vote["sell"] += 1
                else:
                    log_skip(sym, f"5m room too small (buy:{room_buy:.6f} sell:{room_sell:.6f})", feat)
            else:
                log_skip(sym, "5m SR missing", feat)

            # decide direction from votes
            direction = "buy" if dir_vote["buy"] > dir_vote["sell"] else "sell" if dir_vote["sell"] > dir_vote["buy"] else None
            # count true pillars that agree with direction
            agree = 0
            for p, ok in pillars.items():
                if not ok: continue
                # check which pillars imply which direction
                if p == "burst":
                    if (burst >= 0 and direction == "buy") or (burst < 0 and direction == "sell"):
                        agree += 1
                elif p == "imbalance":
                    if (imb >= 0 and direction == "buy") or (imb < 0 and direction == "sell"):
                        agree += 1
                elif p == "micro":
                    if (direction == "buy" and slope >= MICROTREND_UP) or (direction == "sell" and slope <= MICROTREND_DOWN):
                        agree += 1
                elif p == "sr1":
                    if (direction == "buy" and h1 and mid > h1) or (direction == "sell" and l1 and mid < l1):
                        agree += 1
                elif p == "sr5":
                    if (direction == "buy" and h5 and (h5 - mid)/mid >= MIN_ROOM_PCT) or (direction == "sell" and l5 and (mid - l5)/mid >= MIN_ROOM_PCT):
                        agree += 1

            # require >=3 agreeing pillars
            if agree >= 3 and direction is not None:
                chosen = sym
                chosen_feat = feat
                chosen_feat["direction"] = "buy" if direction == "buy" else "sell"
                # diagnostic
                print(f"[CANDIDATE] {sym} agree={agree} dir={chosen_feat['direction']} burst={burst:.5f} imb={imb:.5f} slope={slope:.6f}", flush=True)
                break
            else:
                # log candidate summary for debug
                print(f"[CANDIDATE SKIP] {sym} agree={agree} dir_vote={dir_vote} pillars={pillars}", flush=True)

        if not chosen:
            await self.maybe_heartbeat()
            return

        # cooldown and final fresh guard
        if now - self.mkt.last_signal_ts.get(chosen, 0) < 0.7:
            return
        self.mkt.last_signal_ts[chosen] = now

        feat_now = self.mkt.compute_features(chosen)
        if not feat_now:
            return
        # final direction agreement check
        if chosen_feat["direction"] == "buy" and not (feat_now["burst"] > 0 and feat_now["imbalance"] > 0):
            log_skip(chosen, "final direction mismatch (buy)", feat_now); return
        if chosen_feat["direction"] == "sell" and not (feat_now["burst"] < 0 and feat_now["imbalance"] < 0):
            log_skip(chosen, "final direction mismatch (sell)", feat_now); return
        if len(self.mkt.trades[chosen]) < 3:
            log_skip(chosen, "too few trades final", feat_now); return

        print(f"[ENTRY DEBUG] {chosen} dir={chosen_feat['direction']} mid={feat_now['mid']:.6f} burst={feat_now['burst']:.5f} imb={feat_now['imbalance']:.5f} slope={compute_microtrend(self.mkt.microtrend[chosen]):.6f}", flush=True)

        await self.open_position(chosen, chosen_feat)

# -------- WEBSOCKET LOOP (public) --------
ws_ready: Dict[str, bool] = {s: False for s in SYMBOLS_WS}
async def ws_loop(mkt: MarketState):
    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")
    print("⚡ WS loop started... connecting...", flush=True)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, receive_timeout=40, heartbeat=20) as ws:
                    print("📡 Connected to WS server, subscribing...", flush=True)
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
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                if isinstance(payload, list):
                                    payload = payload[0]
                                if isinstance(payload, dict):
                                    # KEEP original parser semantics
                                    mkt.update_book(sym, payload)
                            elif topic.startswith("publicTrade"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                trades = payload if isinstance(payload, list) else [payload]
                                now = time.time()
                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(t.get("v") or t.get("q") or t.get("size"))
                                    side = (t.get("S") or t.get("side") or "Buy").lower()
                                    if price is None or qty is None:
                                        continue
                                    mkt.add_trade(sym, {"price": price, "size": qty, "side": "buy" if side == "buy" else "sell", "ts": now})
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting", flush=True)
                            break
        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)

# -------- DEBUG CONSOLE --------
def debug_console(mkt: MarketState, bot: ScalperBot):
    help_text = ("[DEBUG] Commands:\n  ws - show websocket ready flags\n  book - show last orderbook timestamps\n  trades - show count of recent trades per symbol\n  pos - show current open position\n  help - show this message\n")
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
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)

# -------- MAIN --------
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
    except Exception:
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
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
