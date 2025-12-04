#!/usr/bin/env python3
"""
ZUBBU_SCALPER_V4.1 — Full bot.py (Option-B logic)

Features:
- Bybit v5 public WS (orderbook.1 + publicTrade)
- Robust orderbook parser (new compact format + legacy)
- Impulse scoring from imbalance + burst + spread
- 2x burst confirmation required to enter (reduces false entries)
- Dynamic TP (based on score + trend)
- Improved dynamic SL:
    - +0.25% -> move SL to break-even
    - +0.40% -> move SL to entry +0.20% profit
    - +0.50% -> enable trailing (SL follows a safe distance)
- Debug skip logger (rate-limited)
- Simple 1m/5m context (support/resistance + trend)
- Single position at a time
- Telegram optional notifications
"""

import os
import time
import json
import math
import asyncio
import threading
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp
import ccxt  # sync ccxt; heavy calls run in thread

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_SCALPER_V4.1"

# --------------- ENV / BASIC CONFIG -----------------
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

# symbols
SYMBOLS_WS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
ws_ready: Dict[str, bool] = {s: False for s in SYMBOLS_WS}

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

# --------------- TRADING CONFIG -----------------
LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95

SL_PCT = 0.0035  # 0.35% default hard SL (client-side watchdog)

# --- Orderflow filters (balanced–aggressive / Option-B) ---
SCORE_MIN        = 0.75
IMBALANCE_THRESH = 0.020
BURST_THRESH     = 0.030
MAX_SPREAD       = 0.0012
MIN_RANGE_PCT    = 0.00025

# Market data timing
RECENT_TRADE_WINDOW = 0.35     # 350 ms burst window
BOOK_STALE_SEC      = 6.0

# Dynamic SL thresholds (decimal)
BE_MOVE = 0.0025   # +0.25% -> move SL to break-even
SL_MOVE1 = 0.004   # +0.40% -> set SL to entry +0.20%
TRAIL_START = 0.005  # +0.50% -> enable tighter trailing

# rate-limit logs
LAST_SKIP_LOG: Dict[str, float] = {}
SKIP_LOG_COOLDOWN = 5.0

KILL_SWITCH_DD = 0.05
HEARTBEAT_IDLE_SEC = 1800

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# --------------- TELEGRAM ---------------
_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0


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
        # never raise from TG
        pass

# --------------- UTIL -----------------


def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# --------------- EXCHANGE CLIENT -----------------


class ExchangeClient:
    def __init__(self):
        cfg = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
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
            except Exception:
                total = 0.0
            upnl = 0.0
            try:
                positions = self.client.fetch_positions()
                for p in positions:
                    upnl += safe_float(p.get("unrealizedPnl"), 0.0) or 0.0
            except Exception:
                pass
            return total + upnl

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

    async def create_limit_order(self, sym_ws: str, side: str, qty: float, price: float,
                                 reduce_only: bool = False, post_only: bool = False):
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

        await asyncio.to_thread(_work)


# --------------- DATA STRUCTURES -----------------
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
    """
    Holds orderbook + trades for symbols; robust update_book for Bybit v5 + legacy.
    """

    def __init__(self):
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=4000) for s in SYMBOLS_WS}
        # last signal ts to avoid spam
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (supports new Bybit format + legacy) --------
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]

        # NEW compact format with "b"/"a"
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000.0)
            book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0

            # snapshot refresh when both present
            if "b" in data and "a" in data:
                book["bids"].clear()
                book["asks"].clear()
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None or q is None:
                        continue
                    if q == 0:
                        continue
                    book["bids"][p] = q
                for px, qty in data.get("a", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None or q is None:
                        continue
                    if q == 0:
                        continue
                    book["asks"][p] = q
                return

            # otherwise delta update
            if "b" in data:
                for px, qty in data["b"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None:
                        continue
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q
            if "a" in data:
                for px, qty in data["a"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None:
                        continue
                    if q == 0:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q
            return

        # LEGACY format fallback
        ts_raw = data.get("ts")
        book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0

        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()
            for px, qty in data.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None:
                    continue
                if q == 0:
                    continue
                book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None or q is None:
                    continue
                if q == 0:
                    continue
                book["asks"][p] = q
            return

        # legacy incremental
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None:
                    continue
                if q == 0:
                    book["bids"].pop(p, None)
                else:
                    book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p is None:
                    continue
                if q == 0:
                    book["asks"].pop(p, None)
                else:
                    book["asks"][p] = q

    def add_trade(self, symbol: str, trade: dict):
        # trade: {price, size, side, ts}
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
        spread = (best_ask - best_bid) / mid

        # recent burst from trades within RECENT_TRADE_WINDOW seconds
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
            return None

        if len(recent_prices) >= 2:
            high = max(recent_prices)
            low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0
        else:
            rng = 0.0

        return {
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "burst": burst,
            "range_pct": rng,
        }


# =====================================================
# MOMENTUM HELPERS
# =====================================================

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    if spread <= 0:
        spread = 1e-6
    return abs(imbalance) * abs(burst) / spread


def choose_dynamic_tp(imbalance: float, burst: float, spread: float, range_pct: float) -> float:
    score = compute_momentum_score(imbalance, burst, spread)
    if score < 2.0:
        return 0.004
    if score < 4.0:
        return 0.006
    if score < 8.0:
        return 0.009
    return 0.012


# ---------------- CORE BOT LOGIC ----------------
class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

        # price buffers for 1m and 5m context
        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_5m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}

        # burst confirmation tracker: {sym: (count, last_ts, last_dir)}
        self.burst_confirm: Dict[str, Tuple[int, float, int]] = {s: (0, 0.0, 0) for s in SYMBOLS_WS}

        # skip logging cooldown stored inside global LAST_SKIP_LOG

    # ------------------ helpers: price context & skip log ------------------
    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        b1 = self.price_1m[sym]
        b5 = self.price_5m[sym]
        b1.append((now, mid))
        b5.append((now, mid))
        cutoff1 = now - 60.0
        cutoff5 = now - 300.0
        while b1 and b1[0][0] < cutoff1:
            b1.popleft()
        while b5 and b5[0][0] < cutoff5:
            b5.popleft()

    def _get_1m_context(self, sym: str) -> Optional[dict]:
        buf = self.price_1m[sym]
        if len(buf) < 5:
            return None
        prices = [p for _, p in buf]
        high = max(prices); low = min(prices)
        rng = high - low
        if rng <= 0:
            return None
        last = prices[-1]
        pos = (last - low) / rng
        return {"high": high, "low": low, "range": rng, "pos": pos,
                "near_support": pos <= 0.2, "near_resistance": pos >= 0.8}

    def _get_5m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_5m[sym]
        if len(buf) < 10:
            return None
        prices = [p for _, p in buf]
        first = prices[0]; last = prices[-1]
        if first <= 0:
            return None
        change = (last - first) / first
        if change > 0.002:
            trend = "up"
        elif change < -0.002:
            trend = "down"
        else:
            trend = "flat"
        return {"trend": trend, "change": change}

    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        now = time.time()
        key = f"{sym}:{reason}"
        last = LAST_SKIP_LOG.get(key, 0.0)
        if now - last < SKIP_LOG_COOLDOWN:
            return
        LAST_SKIP_LOG[key] = now
        mid = feat.get("mid", 0.0)
        spread = feat.get("spread", 0.0)
        imb = feat.get("imbalance", 0.0)
        burst = feat.get("burst", 0.0)
        rng = feat.get("range_pct", 0.0)
        msg = (f"[SKIP] {sym} {reason} {extra} | mid={mid:.4f} spread={spread:.6f} "
               f"imb={imb:.4f} burst={burst:.4f} rng={rng:.6f}")
        print(msg, flush=True)

    # ---------------- lifecycle ----------------
    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT.")
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.")

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
                await send_telegram(f"🚨 Kill-switch: drawdown {dd*100:.2f}% - position closed.")
                self.position = None
            raise SystemExit("Kill-switch triggered")

    # ---------------- burst confirmation helper ----------------
    def _update_burst_confirmation(self, sym: str, imb: float, burst: float) -> bool:
        """
        Returns True when burst has been confirmed enough times to allow an entry.
        Logic:
          - Requires sign(burst) == sign(imb)
          - Requires |burst| >= BURST_THRESH
          - If same direction as last confirmation and within window -> increment count
          - Need count >= 2 to confirm
        """
        now = time.time()
        count, last_ts, last_dir = self.burst_confirm.get(sym, (0, 0.0, 0))
        # direction of burst relative to price (1=buy, -1=sell)
        dir_curr = 1 if burst > 0 else -1 if burst < 0 else 0
        # require direction consistent with imbalance
        if dir_curr == 0 or (imb > 0 and dir_curr != 1) or (imb < 0 and dir_curr != -1):
            # reset
            self.burst_confirm[sym] = (0, now, 0)
            return False
        if abs(burst) < BURST_THRESH:
            # not strong enough
            # don't increment but keep last_ts for short window
            self.burst_confirm[sym] = (0, now, dir_curr)
            return False
        # if previous direction same and within 2*RECENT_TRADE_WINDOW seconds, increment
        if dir_curr == last_dir and (now - last_ts) <= (RECENT_TRADE_WINDOW * 2):
            count = count + 1
        else:
            count = 1
        self.burst_confirm[sym] = (count, now, dir_curr)
        return count >= 2

    # ---------------- main decision loop ----------------
    async def eval_symbols_and_maybe_enter(self):
        if self.position is not None:
            return

        best_score = 0.0
        best_sym: Optional[str] = None
        best_feat: Optional[dict] = None
        best_side: Optional[str] = None
        now = time.time()

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            imb = feat["imbalance"]
            burst = feat["burst"]
            spread = feat["spread"]
            rng = feat["range_pct"]
            mid = feat["mid"]

            # update price buffers
            self._update_price_buffers(sym, mid, now)
            ctx_1m = self._get_1m_context(sym)
            ctx_5m = self._get_5m_trend(sym)

            # basic filters
            if spread <= 0 or spread > MAX_SPREAD:
                self._log_skip(sym, "spread", feat, f"> MAX_SPREAD({MAX_SPREAD})")
                continue
            if rng < MIN_RANGE_PCT:
                self._log_skip(sym, "range", feat, f"< MIN_RANGE_PCT({MIN_RANGE_PCT})")
                continue

            # score
            score = abs(imb) * abs(burst) / max(spread, 1e-6)
            if score < SCORE_MIN:
                self._log_skip(sym, "score", feat, f"{score:.3f} < {SCORE_MIN}")
                continue
            if abs(imb) < IMBALANCE_THRESH:
                self._log_skip(sym, "imbalance", feat, f"< {IMBALANCE_THRESH}")
                continue
            # burst threshold handled in confirmation helper
            if abs(burst) < (BURST_THRESH * 0.5):
                # too tiny burst to consider even confirming
                self._log_skip(sym, "burst_small", feat, f"abs(burst) {abs(burst):.4f} small")
                continue

            # direction from orderflow
            if imb > 0 and burst > 0:
                side = "buy"
            elif imb < 0 and burst < 0:
                side = "sell"
            else:
                self._log_skip(sym, "direction", feat, "imb & burst disagree")
                continue

            # structure filter (1m)
            if ctx_1m:
                if side == "buy" and ctx_1m["near_resistance"]:
                    self._log_skip(sym, "structure", feat, "long at 1m resistance")
                    continue
                if side == "sell" and ctx_1m["near_support"]:
                    self._log_skip(sym, "structure", feat, "short at 1m support")
                    continue

            # trend filter (5m)
            if ctx_5m:
                trend = ctx_5m["trend"]
                # if counter-trend, require stronger score
                if trend == "up" and side == "sell" and score < SCORE_MIN * 1.8:
                    self._log_skip(sym, "trend", feat, "uptrend, short too weak")
                    continue
                if trend == "down" and side == "buy" and score < SCORE_MIN * 1.8:
                    self._log_skip(sym, "trend", feat, "downtrend, long too weak")
                    continue

            # burst confirmation: require 2 confirmations to avoid early entry
            confirmed = self._update_burst_confirmation(sym, imb, burst)
            if not confirmed:
                self._log_skip(sym, "burst_confirm", feat, "not confirmed (need 2x)")
                continue

            # choose best symbol by score
            if score > best_score:
                best_score = score
                best_sym = sym
                best_feat = feat
                best_side = side

        if not best_sym or not best_feat or not best_side:
            await self.maybe_heartbeat()
            return

        # anti-spam same symbol
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        await self.open_position(best_sym, best_feat, best_side)

    # ---------------- order execution ----------------
    async def open_position(self, sym_ws: str, feat: dict, side: str):
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0:
            return

        mid = feat["mid"]
        notional = equity * EQUITY_USE_FRACTION * LEVERAGE
        if mid <= 0:
            return
        qty = notional / mid

        min_qty = MIN_QTY_MAP.get(sym_ws, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym_ws} qty {qty:.6f} < min {min_qty}", flush=True)
            return

        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        # dynamic TP
        base_tp_pct = choose_dynamic_tp(feat["imbalance"], feat["burst"], feat["spread"], feat["range_pct"])
        # trend adjust
        trend_ctx = self._get_5m_trend(sym_ws)
        if trend_ctx:
            t = trend_ctx["trend"]
            if (t == "up" and side == "buy") or (t == "down" and side == "sell"):
                base_tp_pct *= 1.12
            elif (t == "up" and side == "sell") or (t == "down" and side == "buy"):
                base_tp_pct *= 0.85
        tp_pct = max(0.0035, min(base_tp_pct, 0.015))
        sl_pct = SL_PCT

        if side == "buy":
            tp_price = mid * (1.0 + tp_pct)
            sl_price = mid * (1.0 - sl_pct)
        else:
            tp_price = mid * (1.0 - tp_pct)
            sl_price = mid * (1.0 + sl_pct)

        # ENTRY market
        try:
            order = await self.exchange.create_market_order(sym_ws, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # place TP limit (reduce-only post-only maker)
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(sym_ws, opp_side, qty, tp_price, reduce_only=True, post_only=True)
        except Exception as e:
            print(f"[TP ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"⚠️ TP placement failed for {sym_ws}")

        self.position = Position(symbol_ws=sym_ws, side=side, qty=qty, entry_price=entry_price,
                                 tp_price=tp_price, sl_price=sl_price, opened_ts=time.time())
        self.last_trade_time = time.time()
        print(f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.6f} TP={tp_price:.6f} SL≈{sl_price:.6f}", flush=True)
        await send_telegram(f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.6f} TP={tp_price:.6f} SL≈{sl_price:.6f}")

    # ---------------- risk watchdog (dynamic SL updates + close) ----------------
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
        if pos.side == "buy":
            profit = (mid - pos.entry_price) / pos.entry_price
        else:
            profit = (pos.entry_price - mid) / pos.entry_price

        updated = False
        # 1) move SL to break-even when profit >= BE_MOVE
        if profit >= BE_MOVE:
            # break-even (entry)
            if pos.side == "buy":
                new_sl = pos.entry_price
            else:
                new_sl = pos.entry_price
            if new_sl > 0 and ((pos.side == "buy" and new_sl > pos.sl_price) or (pos.side == "sell" and new_sl < pos.sl_price)):
                pos.sl_price = new_sl
                updated = True
                print(f"[SL MOVE] {sym}: moved SL to break-even {pos.sl_price:.6f}", flush=True)
                await send_telegram(f"🔧 SL moved to break-even for {sym} @ {pos.sl_price:.6f}")

        # 2) move SL to entry + small profit when profit >= SL_MOVE1
        if profit >= SL_MOVE1:
            if pos.side == "buy":
                new_sl = pos.entry_price * (1.0 + 0.002)  # +0.20%
            else:
                new_sl = pos.entry_price * (1.0 - 0.002)
            # only move if it's better than current SL
            if ((pos.side == "buy" and new_sl > pos.sl_price) or (pos.side == "sell" and new_sl < pos.sl_price)):
                pos.sl_price = new_sl
                updated = True
                print(f"[SL MOVE] {sym}: moved SL to small profit {pos.sl_price:.6f}", flush=True)
                await send_telegram(f"🔧 SL moved to +0.20% for {sym} @ {pos.sl_price:.6f}")

        # 3) trailing enable once profit >= TRAIL_START
        if profit >= TRAIL_START:
            # trailing SL: keep SL at mid - small buffer for buy, mid + small buffer for sell
            if pos.side == "buy":
                new_sl = mid * (1.0 - (SL_PCT / 2))  # tighten trailing distance
                if new_sl > pos.sl_price:
                    pos.sl_price = new_sl
                    updated = True
                    print(f"[TRAIL SL] {sym}: trailing SL updated to {pos.sl_price:.6f}", flush=True)
                    await send_telegram(f"🟢 Trailing SL updated for {sym} @ {pos.sl_price:.6f}")
            else:
                new_sl = mid * (1.0 + (SL_PCT / 2))
                if new_sl < pos.sl_price:
                    pos.sl_price = new_sl
                    updated = True
                    print(f"[TRAIL SL] {sym}: trailing SL updated to {pos.sl_price:.6f}", flush=True)
                    await send_telegram(f"🟢 Trailing SL updated for {sym} @ {pos.sl_price:.6f}")

        # If price crosses SL (client-side close)
        if pos.side == "buy":
            dd = (pos.entry_price - mid) / pos.entry_price  # negative if profit
            # check SL breach: mid <= sl_price
            if mid <= pos.sl_price:
                # close
                await self.exchange.close_position_market(sym)
                print(f"[SL HIT] {sym} BUY entry={pos.entry_price:.6f} now={mid:.6f} SL={pos.sl_price:.6f}", flush=True)
                await send_telegram(f"🛑 SL HIT {sym} BUY entry={pos.entry_price:.6f} now={mid:.6f} SL={pos.sl_price:.6f}")
                self.position = None
                return
        else:
            if mid >= pos.sl_price:
                await self.exchange.close_position_market(sym)
                print(f"[SL HIT] {sym} SELL entry={pos.entry_price:.6f} now={mid:.6f} SL={pos.sl_price:.6f}", flush=True)
                await send_telegram(f"🛑 SL HIT {sym} SELL entry={pos.entry_price:.6f} now={mid:.6f} SL={pos.sl_price:.6f}")
                self.position = None
                return

        # nothing triggered; if we updated SL, keep position
        if updated:
            # also log current TP/SL status
            print(f"[POS] {sym} entry={pos.entry_price:.6f} TP={pos.tp_price:.6f} SL={pos.sl_price:.6f} profit={profit*100:.2f}%", flush=True)

    # ---------------- idle heartbeat ----------------
    async def maybe_heartbeat(self):
        now = time.time()
        if now - self.last_trade_time < HEARTBEAT_IDLE_SEC:
            return
        if now - self.last_heartbeat_ts < HEARTBEAT_IDLE_SEC:
            return
        self.last_heartbeat_ts = now
        eq = await self.exchange.fetch_equity()
        print(f"[HEARTBEAT] idle, equity={eq:.2f}", flush=True)
        await send_telegram(f"💤 Bot idle ~{int(HEARTBEAT_IDLE_SEC/60)} min. Equity={eq:.2f} USDT")


# ---------------- WEBSOCKET LOOP ----------------
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
                            # skip subscription success messages
                            if data.get("success") is True:
                                continue
                            topic = data.get("topic")
                            if not topic:
                                continue

                            # ORDERBOOK
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
                                    mkt.add_trade(sym, {"price": price, "size": qty,
                                                       "side": "buy" if side == "buy" else "sell", "ts": now_ts})
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting", flush=True)
                            break
        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1.0)


# ---------------- DEBUG CONSOLE ----------------
def debug_console(mkt: MarketState, bot: ScalperBot):
    help_text = ("[DEBUG] Commands:\n"
                 "  ws    - show websocket ready flags\n"
                 "  book  - show last orderbook timestamps\n"
                 "  trades- show # of recent trades per symbol\n"
                 "  pos   - show current open position\n"
                 "  help  - show this message\n")
    print(help_text, flush=True)
    while True:
        try:
            cmd = input("").strip().lower()
        except EOFError:
            return
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


# ---------------- MAIN ----------------
async def main():
    print(f"Starting {BOT_VERSION} ...", flush=True)
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

    # start debug console thread
    threading.Thread(target=debug_console, args=(mkt, bot), daemon=True).start()

    await bot.init_equity_and_leverage()

    ws_task = asyncio.create_task(ws_loop(mkt))

    try:
        while True:
            await bot.maybe_kill_switch()
            await bot.eval_symbols_and_maybe_enter()
            await bot.watchdog_position()
            await asyncio.sleep(1.0)
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
