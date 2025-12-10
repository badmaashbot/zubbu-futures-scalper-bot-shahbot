#!/usr/bin/env python3
"""
ZUBBU_REVERSAL_V1.0 – Full bot.py (Safe Reversal + BB assist + Vol Guard)

Core Logic (high level):
------------------------
- Bybit v5 public WS (orderbook.1 + publicTrade) – linear USDT perp
- Robust orderbook parser (new compact format + legacy)
- Orderflow engine:
    * imbalance (top 5 levels)
    * micro burst (last 0.35s trades)
    * accumulation burst (last 5s trades)
- 1m micro-structure (high/low/range/position)
- 1m Bollinger Bands (direction + move estimation, NOT a hard filter)
- Reversal logic:
    * only trade from 1m extremes (top/bottom of 1m range)
    * orderflow must flip in reversal direction (burst + imbalance + micro)
    * expected move (from BB/structure) must be >= 0.35%
    * buffer validation (~0.35s) to confirm move is real, not trap
- SAFE MODE volatility guard:
    * BTC 1m > 0.50% => cooldown (no entries for few seconds)
    * symbol 1m range > 0.80% => skip symbol
    * 1m BB width > 1.20% => skip symbol
- Single open position at a time (best symbol chosen)
- Fixed TP = +0.25%
- Fixed SL = -0.18%
- No dynamic ladder SL
- Cooldown after exits to avoid revenge / chop
- Detailed skip logs (rate-limited) so you can see WHY it skipped

NOTE: This is an experimental strategy. Use TESTNET or very small size first.
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
import ccxt  # sync ccxt, we wrap blocking calls with asyncio.to_thread

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_REVERSAL_V1.0"

# ---------------- ENV / BASIC CONFIG ----------------

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

# WebSocket symbols (Bybit format)
SYMBOLS_WS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]

# WebSocket ready flags (symbol -> bool)
ws_ready: Dict[str, bool] = {s: False for s in SYMBOLS_WS}

# ccxt unified symbols for USDT perpetual
SYMBOL_MAP = {
    "BTCUSDT": "BTC/USDT:USDT",
    "ETHUSDT": "ETH/USDT:USDT",
    "BNBUSDT": "BNB/USDT:USDT",
    "SOLUSDT": "SOL/USDT:USDT",
    "DOGEUSDT": "DOGE/USDT:USDT",
}

# --------------- CORRECT WEBSOCKET URL -----------------
if TESTNET:
    WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"
else:
    WS_URL = "wss://stream.bybit.com/v5/public/linear"

# --------------- TRADING CONFIG -----------------

LEVERAGE = 5
EQUITY_USE_FRACTION = 0.97  # use up to 97% equity * leverage as position notional

# Fixed TP / SL (reversal scalper)
TP_PCT = 0.0025   # 0.25% take profit
SL_PCT = 0.0018   # 0.18% stop loss

# --- Orderflow filters (for reversal confirmation) ---
SCORE_MIN        = 0.60    # softer score than old momentum bot
IMBALANCE_THRESH = 0.05    # 5% imbalance
BURST_MICRO_MIN  = 0.020   # micro burst (0.35s) minimum
BURST_ACCUM_MIN  = 0.050   # accumulation burst (5s) minimum
MAX_SPREAD       = 0.0015  # 0.15% max spread
MIN_RANGE_PCT    = 0.00020 # 0.02% minimum 1m micro-range

# Market data timing
RECENT_TRADE_WINDOW = 0.35  # 350 ms micro burst window
ACCUM_BURST_WINDOW  = 5.0   # 5 second accumulation window
BOOK_STALE_SEC      = 6.0   # ignore orderbook older than 6 seconds

# rate-limit logs
LAST_SKIP_LOG: Dict[str, float] = {}
SKIP_LOG_COOLDOWN = 5.0  # seconds per (symbol,reason)

# Risk & heartbeat
KILL_SWITCH_DD      = 0.05   # 5% equity drawdown -> stop trading
HEARTBEAT_IDLE_SEC  = 1800   # 30 minutes idle heartbeat

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# --------------- REVERSAL / SAFE MODE CONSTANTS -----------------

# Expected move for entry (reversal must have this room)
EXPECTED_MOVE_MIN = 0.0035  # 0.35%

# Buffer validation (delay between signal and entry)
CONFIRMATION_DELAY = 0.35   # 0.35 seconds

# Cooldowns after exits (SAFE mode)
COOLDOWN_SL = 30.0   # 30s cooldown after SL
COOLDOWN_TP = 12.0   # 12s cooldown after TP

# SAFE MODE VOLATILITY GUARDS

# BTC volatility guard: if BTC 1m move > this, skip all entries and set cool-off
BTC_VOL_SPIKE_PCT   = 0.005   # 0.50% in 1 minute
BTC_VOL_COOLDOWN    = 10.0    # 10 seconds after spike

# Symbol-level micro-range guard
SYMBOL_RANGE_MAX_SAFE = 0.008  # 0.80% 1m range => too wild for safe scalper

# Bollinger width guard (1m BB)
BB_WIDTH_MAX_SAFE     = 0.012  # 1.20% BB width => hyper volatile, skip in safe mode

# Volatility / trade density
VEL_MIN_TRADES   = 6       # minimum trades in last 5s

# --------------- TELEGRAM -----------------

_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0   # at most 1 msg every 30s to avoid spam


async def send_telegram(msg: str):
    """Safe, rate-limited Telegram sender."""
    global _last_tg_ts
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    now = time.time()
    if now - _last_tg_ts < TG_MIN_INTERVAL:
        return
    _last_tg_ts = now
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3.0)
        ) as session:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            )
    except Exception:
        # never let TG issues break the bot
        pass


# --------------- UTILS -----------------


def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    """Convert to float or return default if None/invalid."""
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def stddev(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    m = mean(values)
    var = sum((v - m) ** 2 for v in values) / (n - 1)
    return math.sqrt(max(var, 0.0))


# --------------- EXCHANGE CLIENT (ccxt sync wrapped into async) -----------------

class ExchangeClient:
    """
    Thin async wrapper around ccxt.bybit (sync HTTP).
    All heavy calls are done in a thread (asyncio.to_thread) so WS stays smooth.
    """

    def __init__(self):
        cfg = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},  # USDT perpetual
        }
        if TESTNET:
            cfg["urls"] = {
                "api": {
                    "public": "https://api-testnet.bybit.com",
                    "private": "https://api-testnet.bybit.com",
                }
            }
        self.client = ccxt.bybit(cfg)

    # ---------------- LEVERAGE ----------------
    async def set_leverage(self, sym_ws: str, lev: int) -> None:
        symbol = SYMBOL_MAP[sym_ws]

        def _work():
            try:
                self.client.set_leverage(
                    lev, symbol, params={"category": "linear"}
                )
            except Exception:
                pass

        await asyncio.to_thread(_work)

    # ---------------- EQUITY (USDT + PNL) ----------------
    async def fetch_equity(self) -> float:
        def _work():
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

        return await asyncio.to_thread(_work)

    # ---------------- GET POSITION SIZE (contracts) ----------------
    async def get_position_size(self, sym_ws: str) -> float:
        """Return open contracts qty for symbol (0.0 if flat)."""
        symbol = SYMBOL_MAP[sym_ws]

        def _work():
            try:
                pos = self.client.fetch_positions([symbol])
                for p in pos:
                    contracts = safe_float(p.get("contracts"), 0.0) or 0.0
                    return contracts
            except Exception:
                return 0.0
            return 0.0

        return await asyncio.to_thread(_work)

    # ---------------- MARKET CLOSE ----------------
    async def close_position_market(self, sym_ws: str):
        """Close any open position in given symbol at market."""
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
                    self.client.create_order(
                        symbol, "market", side, abs(contracts), None, params
                    )
                except Exception:
                    pass

        await asyncio.to_thread(_work)

    # ---------------- MARKET ORDER ----------------
    async def create_market_order(
        self, sym_ws: str, side: str, qty: float, reduce_only: bool = False
    ):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True

        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)

        return await asyncio.to_thread(_work)

    # ---------------- LIMIT ORDER ----------------
    async def create_limit_order(
        self,
        sym_ws: str,
        side: str,
        qty: float,
        price: float,
        reduce_only: bool = False,
        post_only: bool = False,
    ):
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


# --------------- DATA STRUCTURES -----------------


@dataclass
class Position:
    symbol_ws: str
    side: str          # "buy" or "sell"
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    opened_ts: float


class MarketState:
    """
    Holds orderbook + trades for all symbols, updated from WS.
    """

    def __init__(self):
        self.books: Dict[str, dict] = {
            s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS
        }
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (Bybit V5 + legacy) --------
    def update_book(self, symbol: str, data: dict):
        """
        Correct parser for Bybit V5 "orderbook.1.<symbol>" stream.
        Supports:
          - NEW format: {"b": [...], "a": [...], "ts": ...}
          - DELTA where b/a contain qty 0 (delete) or qty>0 (update)
          - OLD format fallback: {"type": "snapshot"/"delta", "bids": [...], "asks": [...]}
        """
        book = self.books[symbol]

        # --- New V5 compact format ---
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000)
            book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

            # Snapshot: both sides present
            if "b" in data and "a" in data:
                book["bids"].clear()
                book["asks"].clear()
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p and q:
                        book["bids"][p] = q
                for px, qty in data.get("a", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p and q:
                        book["asks"][p] = q
                return

            # Delta: only b or a
            if "b" in data:
                for px, qty in data["b"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if not p:
                        continue
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q

            if "a" in data:
                for px, qty in data["a"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if not p:
                        continue
                    if q == 0:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q

            return

        # --- Legacy format fallback ---
        ts_raw = data.get("ts")
        book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()
            for px, qty in data.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p and q:
                    book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p and q:
                    book["asks"][p] = q
            return

        # delta update for legacy
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p:
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p:
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
        """
        Compute mid, spread, imbalance, micro burst, accumulation burst,
        micro-range, trade_count (velocity), and candles for 1m/BB.
        """
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None

        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC:
            return None

        # Top 5 levels imbalance
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

        # --- Burst engine: micro (0.35s) + accumulation (5s) ---
        cutoff_micro = now - RECENT_TRADE_WINDOW
        cutoff_accum = now - ACCUM_BURST_WINDOW

        micro_burst = 0.0
        accum_burst = 0.0
        recent_prices: List[float] = []
        recent_trades: List[Tuple[float, float]] = []

        for t in reversed(self.trades[symbol]):
            ts = t["ts"]
            if ts < cutoff_accum:
                break
            side_mult = 1.0 if t["side"] == "buy" else -1.0
            sz = t["size"]

            accum_burst += side_mult * sz
            if ts >= cutoff_micro:
                micro_burst += side_mult * sz

            price = t["price"]
            recent_prices.append(price)
            recent_trades.append((ts, side_mult * sz))

        if not recent_prices:
            return None

        # micro-range (last ~ACCUM_BURST_WINDOW seconds)
        high = max(recent_prices)
        low = min(recent_prices)
        rng = (high - low) / mid if mid > 0 else 0.0

        trade_count = len(recent_trades)

        return {
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "burst": accum_burst,
            "burst_micro": micro_burst,
            "range_pct": rng,
            "trade_count": trade_count,
        }


# =====================================================
# MOMENTUM SCORE (used lightly, not as main logic)
# =====================================================

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    if spread <= 0:
        spread = 1e-6
    return abs(imbalance) * abs(burst) / spread


# --------------- CORE BOT LOGIC -----------------


class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt

        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

        # 1m price history buffers (for micro-structure + BB)
        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        # store last BTC vol spike time
        self.last_btc_vol_spike_ts: float = 0.0

        # skip-log cooldown
        self.last_skip_log = LAST_SKIP_LOG

        # pending entry for confirmation delay
        self.pending_entry: Optional[dict] = None

        # exit cooldown state
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False

        # last watchdog timestamp
        self.last_watchdog_ts: float = 0.0

    # -------------------------------------------------
    # helpers: structure, BB, logging
    # -------------------------------------------------
    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        """Keep 1m rolling mid-price history."""
        buf1 = self.price_1m[sym]
        buf1.append((now, mid))
        cutoff1 = now - 60.0  # 1 minute
        while buf1 and buf1[0][0] < cutoff1:
            buf1.popleft()

    def _get_1m_context(self, sym: str) -> Optional[dict]:
        """
        Returns 1m micro-structure:
          - high, low, range
          - position of current price inside that range (0..1)
          - near_support / near_resistance flags
        """
        buf = self.price_1m[sym]
        if len(buf) < 5:
            return None

        prices = [p for _, p in buf]
        high = max(prices)
        low = min(prices)
        rng = high - low
        if rng <= 0:
            return None

        last_price = prices[-1]
        pos_in_range = (last_price - low) / rng  # 0 bottom, 1 top

        return {
            "high": high,
            "low": low,
            "range": rng,
            "pos": pos_in_range,
            "near_support": pos_in_range <= 0.2,
            "near_resistance": pos_in_range >= 0.8,
        }

    def _get_1m_bb(self, sym: str) -> Optional[dict]:
        """
        Simple 1m Bollinger Bands (N=20) using mid prices.
        Used ONLY for direction / expected-move estimation and volatility check.
        """
        buf = self.price_1m[sym]
        if len(buf) < 20:
            return None
        prices = [p for _, p in buf]
        closes = prices[-20:]
        m = mean(closes)
        sd = stddev(closes)
        if m <= 0 or sd <= 0:
            return None
        # Using 3 std (more extreme locations)
        upper = m + 3.0 * sd
        lower = m - 3.0 * sd
        width_pct = (upper - lower) / m if m > 0 else 0.0
        return {
            "mid": m,
            "upper": upper,
            "lower": lower,
            "width_pct": width_pct,
        }

    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < SKIP_LOG_COOLDOWN:
            return

        self.last_skip_log[key] = now
        mid = feat.get("mid", 0.0)
        spread = feat.get("spread", 0.0)
        imb = feat.get("imbalance", 0.0)
        burst = feat.get("burst", 0.0)
        b_micro = feat.get("burst_micro", 0.0)
        rng = feat.get("range_pct", 0.0)
        tcount = feat.get("trade_count", 0)

        msg = (
            f"[SKIP] {sym} {reason} {extra} | "
            f"mid={mid:.4f} spread={spread:.6f} "
            f"imb={imb:.4f} burst={burst:.4f} micro={b_micro:.4f} "
            f"trades={tcount} rng={rng:.6f}"
        )
        print(msg, flush=True)

    # -------------------------------------------------
    # lifecycle
    # -------------------------------------------------
    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(
            f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT. "
            f"Kill at {KILL_SWITCH_DD*100:.1f}% DD."
        )
        # set leverage for all symbols
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.")
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
                await send_telegram(
                    f"🚨 Kill-switch: equity drawdown {dd*100:.2f}%. Position closed."
                )
                self.position = None
            raise SystemExit("Kill-switch triggered")

    # -------------------------------------------------
    # SAFE MODE VOLATILITY GUARD
    # -------------------------------------------------
    def _safe_mode_block_entries(self, now: float) -> bool:
        """
        Returns True if SAFE MODE says "no entries right now".
        """
        # global BTC cool-down after spike
        if now - self.last_btc_vol_spike_ts < BTC_VOL_COOLDOWN:
            return True

        # BTC 1m actual movement check (for fresh spikes)
        buf = self.price_1m.get("BTCUSDT")
        if buf and len(buf) >= 2:
            p0 = buf[0][1]
            p1 = buf[-1][1]
            if p0 > 0:
                chg = abs(p1 - p0) / p0
                if chg > BTC_VOL_SPIKE_PCT:
                    self.last_btc_vol_spike_ts = now
                    print(
                        f"[SAFE] BTC 1m move={chg:.4f} > {BTC_VOL_SPIKE_PCT:.4f} -> cooldown.",
                        flush=True,
                    )
                    return True
        return False

    # -------------------------------------------------
    # main decision loop
    # -------------------------------------------------
    async def eval_symbols_and_maybe_enter(self):
        """
        Reversal logic:
        - Only one position at a time
        - SAFE MODE volatility guard
        - 0.35s buffer confirmation before real entry
        """
        if self.position is not None:
            return

        now = time.time()

        # EXIT COOLDOWN
        if self.last_exit_ts > 0.0:
            cooldown = COOLDOWN_SL if self.last_exit_was_loss else COOLDOWN_TP
            if now - self.last_exit_ts < cooldown:
                return

        # SAFE MODE (BTC global)
        if self._safe_mode_block_entries(now):
            return

        # ---------- PENDING ENTRY CONFIRMATION ----------
        if self.pending_entry:
            pe = self.pending_entry
            if now - pe["ts"] >= CONFIRMATION_DELAY:
                sym = pe["symbol"]
                side = pe["side"]
                feat_new = self.mkt.compute_features(sym)
                if not feat_new:
                    self.pending_entry = None
                else:
                    # re-evaluate expected move & vol safety
                    ctx_1m = self._get_1m_context(sym)
                    bb = self._get_1m_bb(sym)
                    expected_move = self._estimate_expected_move(sym, feat_new, ctx_1m, bb, side)
                    if expected_move < EXPECTED_MOVE_MIN:
                        self._log_skip(sym, "confirm_expected_move", feat_new,
                                       f"{expected_move:.4f} < {EXPECTED_MOVE_MIN:.4f}")
                        self.pending_entry = None
                    else:
                        # vol guard per symbol
                        if not self._symbol_safe(sym, feat_new, ctx_1m, bb):
                            self._log_skip(sym, "confirm_vol_guard", feat_new, "")
                            self.pending_entry = None
                        else:
                            # orderflow direction must still match side
                            if not self._reversal_flow_ok(feat_new, side):
                                self._log_skip(sym, "confirm_dir", feat_new, "reversal flow broke")
                                self.pending_entry = None
                            else:
                                await self.open_position(sym, feat_new, side)
                                self.pending_entry = None
                                return

            # still waiting for confirmation
            if self.pending_entry:
                return

        # ---------- NORMAL SCAN ----------
        best_score = 0.0
        best_sym: Optional[str] = None
        best_feat: Optional[dict] = None
        best_side: Optional[str] = None

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            # update 1m buffer
            mid = feat["mid"]
            self._update_price_buffers(sym, mid, now)

            ctx_1m = self._get_1m_context(sym)
            bb = self._get_1m_bb(sym)

            # symbol-level SAFE MODE guard
            if not self._symbol_safe(sym, feat, ctx_1m, bb):
                continue

            # require enough micro-range to be meaningful
            rng = feat["range_pct"]
            spread = feat["spread"]
            if spread <= 0 or spread > MAX_SPREAD:
                self._log_skip(sym, "spread", feat, f"> MAX_SPREAD({MAX_SPREAD})")
                continue
            if rng < MIN_RANGE_PCT:
                self._log_skip(sym, "range", feat, f"< MIN_RANGE_PCT({MIN_RANGE_PCT})")
                continue

            # we need 1m context for reversal positioning
            if not ctx_1m:
                self._log_skip(sym, "ctx_1m_missing", feat, "")
                continue

            # decide reversal side from 1m position
            pos = ctx_1m["pos"]
            if pos <= 0.25:
                # bottom area => look for long reversals
                side = "buy"
            elif pos >= 0.75:
                # top area => look for short reversals
                side = "sell"
            else:
                # middle of range -> no clear reversal edge
                self._log_skip(sym, "mid_range", feat, f"pos={pos:.2f}")
                continue

            # orderflow conditions (reversal confirmation, not pure momentum)
            if not self._reversal_flow_ok(feat, side):
                self._log_skip(sym, "reversal_flow", feat, f"side={side}")
                continue

            # ensure enough trades for reliability
            if feat["trade_count"] < VEL_MIN_TRADES:
                self._log_skip(sym, "velocity", feat,
                               f"trades={feat['trade_count']} < {VEL_MIN_TRADES}")
                continue

            # estimated expected move (from BB + 1m structure)
            expected_move = self._estimate_expected_move(sym, feat, ctx_1m, bb, side)
            if expected_move < EXPECTED_MOVE_MIN:
                self._log_skip(sym, "expected_move", feat,
                               f"{expected_move:.4f} < {EXPECTED_MOVE_MIN:.4f}")
                continue

            # simple score for choosing best symbol (not for entry validity)
            score = compute_momentum_score(feat["imbalance"], feat["burst"], feat["spread"])
            if score < SCORE_MIN:
                self._log_skip(sym, "score", feat, f"{score:.3f} < {SCORE_MIN}")
                continue

            if score > best_score:
                best_score = score
                best_sym = sym
                best_feat = feat
                best_side = side

        if not best_sym or not best_feat or not best_side:
            return

        # tiny anti-spam: don't double-trigger same symbol in <0.5s
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        # set pending entry for confirmation instead of enter instantly
        self.pending_entry = {
            "symbol": best_sym,
            "side": best_side,
            "ts": now,
        }
        print(
            f"[SIGNAL] {best_sym} {best_side.upper()} score={best_score:.3f} "
            f"waiting {CONFIRMATION_DELAY:.2f}s confirmation",
            flush=True,
        )

    # -------------------------------------------------
    # helpers for entry logic
    # -------------------------------------------------
    def _symbol_safe(self, sym: str, feat: dict,
                     ctx_1m: Optional[dict],
                     bb: Optional[dict]) -> bool:
        """
        SAFE MODE checks per symbol:
        - 1m range not hyper crazy
        - BB width not hyper crazy
        """
        rng = feat.get("range_pct", 0.0)
        if rng > SYMBOL_RANGE_MAX_SAFE:
            self._log_skip(sym, "sym_range_unsafe", feat,
                           f"rng={rng:.4f} > {SYMBOL_RANGE_MAX_SAFE:.4f}")
            return False

        if bb:
            w = bb.get("width_pct", 0.0)
            if w > BB_WIDTH_MAX_SAFE:
                self._log_skip(sym, "bb_width_unsafe", feat,
                               f"bb_width={w:.4f} > {BB_WIDTH_MAX_SAFE:.4f}")
                return False

        # require some 1m context
        if ctx_1m and ctx_1m["range"] <= 0:
            self._log_skip(sym, "ctx_1m_zero_range", feat, "")
            return False

        return True

    def _reversal_flow_ok(self, feat: dict, side: str) -> bool:
        """
        Orderflow conditions for reversal entry:
        - use accumulation + micro burst + imbalance sign
        - not insane aggressive, but must show clear flip
        """
        imb = feat["imbalance"]
        burst = feat["burst"]
        b_micro = feat["burst_micro"]

        if abs(imb) < IMBALANCE_THRESH:
            return False
        if abs(burst) < BURST_ACCUM_MIN:
            return False
        if abs(b_micro) < BURST_MICRO_MIN:
            return False

        if side == "buy":
            return imb > 0 and burst > 0 and b_micro > 0
        else:
            return imb < 0 and burst < 0 and b_micro < 0

    def _estimate_expected_move(
        self,
        sym: str,
        feat: dict,
        ctx_1m: Optional[dict],
        bb: Optional[dict],
        side: str,
    ) -> float:
        """
        Estimate expected move for reversal:
        - use 1m micro-range extremes (high/low)
        - use BB mid & bands as "return to mean" estimate
        - we DO NOT skip solely because of BB; it's just for estimation
        Returns expected move in decimal (0.0035 = 0.35%).
        """
        mid = feat["mid"]
        if mid <= 0:
            return 0.0

        estimates: List[float] = []

        # 1m structure extremes
        if ctx_1m:
            if side == "buy":
                # from bottom towards 1m mid or high
                high = ctx_1m["high"]
                low = ctx_1m["low"]
                # distance to 1m mid
                mid_price = (high + low) / 2.0
                if mid_price > mid:
                    estimates.append((mid_price - mid) / mid)
                # distance to 1m high (more ambitious)
                if high > mid:
                    estimates.append((high - mid) / mid)
            else:
                high = ctx_1m["high"]
                low = ctx_1m["low"]
                mid_price = (high + low) / 2.0
                # from top downwards
                if mid_price < mid:
                    estimates.append((mid - mid_price) / mid)
                if low < mid:
                    estimates.append((mid - low) / mid)

        # Bollinger mean reversion estimate
        if bb:
            bb_mid = bb["mid"]
            upper = bb["upper"]
            lower = bb["lower"]

            if side == "buy":
                # from near lower band up towards BB mid
                if bb_mid > mid:
                    estimates.append((bb_mid - mid) / mid)
                # or up towards upper band (if mid extremely low)
                if upper > mid:
                    estimates.append((upper - mid) / mid)
            else:
                # from near upper band down towards BB mid
                if bb_mid < mid:
                    estimates.append((mid - bb_mid) / mid)
                # or down towards lower band
                if lower < mid:
                    estimates.append((mid - lower) / mid)

        if not estimates:
            return 0.0

        # we care about the minimum decent expectation among our estimates,
        # but to be safer we can take the max to ensure there's enough room
        expected = max(estimates)
        return max(expected, 0.0)

    # -------------------------------------------------
    # order execution
    # -------------------------------------------------
    async def open_position(self, sym_ws: str, feat: dict, side: str):
        """
        Open a new reversal position with FIXED TP, FIXED SL.
        """
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

        # round qty down to 3 decimals
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        # ENTRY: market order
        try:
            order = await self.exchange.create_market_order(
                sym_ws, side, qty, reduce_only=False
            )
            entry_price = safe_float(
                order.get("average") or order.get("price"), mid
            ) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}")
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # recompute TP / SL from actual entry
        if side == "buy":
            tp_price = entry_price * (1.0 + TP_PCT)
            sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - TP_PCT)
            sl_price = entry_price * (1.0 + SL_PCT)

        # TP as limit reduce-only maker
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(
                sym_ws,
                opp_side,
                qty,
                tp_price,
                reduce_only=True,
                post_only=True,
            )
        except Exception as e:
            print(f"[TP ERROR] {sym_ws}: {e}")
            await send_telegram(f"⚠️ TP order placement failed for {sym_ws}")

        self.position = Position(
            symbol_ws=sym_ws,
            side=side,
            qty=qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            opened_ts=time.time(),
        )
        self.last_trade_time = time.time()
        print(
            f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL={sl_price:.4f} (tp={TP_PCT*100:.2f}%, sl={SL_PCT*100:.2f}%)",
            flush=True,
        )
        await send_telegram(
            f"📌 REVERSAL ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f}\n"
            f"TP={tp_price:.4f} ({TP_PCT*100:.2f}%) SL={sl_price:.4f} ({SL_PCT*100:.2f}%)"
        )

    # -------------------------------------------------
    # risk watchdog: detect TP/SL hits + backup SL
    # -------------------------------------------------
    async def watchdog_position(self):
        """
        - Detect if exchange closed position (TP hit or SL hit)
        - If still open, apply backup SL check using mid price
        """
        now = time.time()
        if now - self.last_watchdog_ts < 0.5:
            return
        self.last_watchdog_ts = now

        if not self.position:
            return

        pos = self.position
        sym = pos.symbol_ws

        # 1) CHECK IF POSITION STILL OPEN
        size = await self.exchange.get_position_size(sym)
        if size <= 0:
            # Position closed on exchange (TP/SL hit)
            feat = self.mkt.compute_features(sym)
            reason = "unknown"
            was_loss = True

            if feat:
                mid = feat["mid"]
                if pos.side == "buy":
                    move = (mid - pos.entry_price) / pos.entry_price
                else:
                    move = (pos.entry_price - mid) / pos.entry_price

                if move > 0:
                    reason = "TP filled (profit)"
                    was_loss = False
                else:
                    reason = "SL filled (loss)"
                    was_loss = True

            print(
                f"[EXIT DETECTED] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} "
                f"tp={pos.tp_price:.4f} sl={pos.sl_price:.4f} reason={reason}",
                flush=True,
            )
            await send_telegram(
                f"📤 EXIT — {sym} {pos.side.upper()} entry={pos.entry_price:.4f}\n"
                f"➡️ {reason}"
            )

            self.last_exit_ts = time.time()
            self.last_exit_was_loss = was_loss
            self.position = None
            return

        # 2) BACKUP SL CHECK (if exchange SL fails, we still kill it)
        feat = self.mkt.compute_features(sym)
        if not feat:
            return
        mid = feat["mid"]
        if mid <= 0:
            return

        hit = False
        if pos.side == "buy" and mid <= pos.sl_price:
            hit = True
        if pos.side == "sell" and mid >= pos.sl_price:
            hit = True

        if hit:
            await self.exchange.close_position_market(sym)
            print(
                f"[BACKUP SL] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} "
                f"SL={pos.sl_price:.4f} now={mid:.4f}",
                flush=True,
            )
            await send_telegram(
                f"🛑 BACKUP SL — {sym} {pos.side.upper()}\n"
                f"Entry={pos.entry_price:.4f}  SL={pos.sl_price:.4f}"
            )
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = True
            self.position = None

    # -------------------------------------------------
    # idle heartbeat
    # -------------------------------------------------
    async def maybe_heartbeat(self):
        """
        Idle heartbeat every HEARTBEAT_IDLE_SEC when no trades.
        """
        now = time.time()
        if now - self.last_trade_time < HEARTBEAT_IDLE_SEC:
            return
        if now - self.last_heartbeat_ts < HEARTBEAT_IDLE_SEC:
            return
        self.last_heartbeat_ts = now
        eq = await self.exchange.fetch_equity()
        print(f"[HEARTBEAT] idle, equity={eq:.2f}", flush=True)
        await send_telegram(
            f"💤 Bot idle {HEARTBEAT_IDLE_SEC/60:.0f} min. Equity={eq:.2f} USDT"
        )


# --------------- WEBSOCKET LOOP -----------------


async def ws_loop(mkt: MarketState):
    """
    One WS connection, multiple topics: orderbook.1 + publicTrade for all symbols.
    Updates MarketState in real-time.
    """

    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")

    print("⚡ WS loop started... connecting...")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_URL,
                    receive_timeout=40,
                    heartbeat=20
                ) as ws:

                    print("📡 Connected to WS server, subscribing...")
                    await ws.send_json({
                        "op": "subscribe",
                        "args": topics
                    })

                    # Mark symbols as connected
                    for sym in SYMBOLS_WS:
                        ws_ready[sym] = True

                    await send_telegram("📡 WS Connected: All symbols")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue

                            # Ignore subscription confirmations
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

                                    mkt.add_trade(sym, {
                                        "price": price,
                                        "size": qty,
                                        "side": "buy" if side == "buy" else "sell",
                                        "ts": now_ts
                                    })

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting", flush=True)
                            break

        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)


# --------------- DEBUG CONSOLE (runs in separate thread) -----------------


def debug_console(mkt: MarketState, bot: ScalperBot):
    """
    Simple blocking console running in a daemon thread.
    Commands:
      ws      -> show websocket state
      book    -> show last orderbook timestamps
      trades  -> show # of recent trades per symbol
      pos     -> show current open position
      help    -> show commands
    """
    help_text = (
        "[DEBUG] Commands:\n"
        "  ws      - show websocket ready flags\n"
        "  book    - show last orderbook timestamps\n"
        "  trades  - show count of recent trades per symbol\n"
        "  pos     - show current open position\n"
        "  help    - show this message\n"
    )
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


# --------------- MAIN LOOP -----------------


async def main():
    print(f"Starting {BOT_VERSION} ...", flush=True)
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

    # start debug console thread
    threading.Thread(
        target=debug_console, args=(mkt, bot), daemon=True
    ).start()

    await bot.init_equity_and_leverage()

    ws_task = asyncio.create_task(ws_loop(mkt))

    try:
        while True:
            await bot.maybe_kill_switch()
            await bot.eval_symbols_and_maybe_enter()
            await bot.watchdog_position()
            await bot.maybe_heartbeat()
            await asyncio.sleep(0.20)  # main loop ~5 times per second
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except Exception:
            pass


if __name__ == "__main__":
    try:
        import uvloop  # optional, for speed
        uvloop.install()
    except Exception:
        pass

    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
```0
