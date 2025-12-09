#!/usr/bin/env python3
"""
ZUBBU_SCALPER_V4.5_HTF – Full bot.py
(with CONFIRMATION, TP room, LV filter, Ladder SL, real SL_PCT, 5m + 15m trend,
cooldown after exit, simple footprint spoof filter, 0.25s eval, ~0.5s watchdog)

Core Logic:
-----------
- Bybit v5 public WS (orderbook.1 + publicTrade) – linear USDT perp
- Robust orderbook parser (new compact format + legacy)
- Orderflow engine:
    * imbalance (top 5 levels)
    * micro burst (last 0.35s trades)
    * accumulation burst (last 5s trades)
    * score = |imbalance| * |accum_burst| / spread
- 1m structure filter (support/resistance zones)
- 5m trend filter (up / down / flat)
- 15m higher-timeframe trend filter (strong bias filter, slightly relaxed)
- Single open position at a time (best symbol chosen)
- Adaptive TP (0.40% / 0.60% depending on micro-range)
- STATIC SL based on SL_PCT + dynamic ladder SL
- TP room filter (requires ~0.30% space in 1m range)
- Liquidity Vacuum (LV) filter: opposite wall max 20%
- Simple spoof / absorption footprint filter based on fast liq changes
- Confirmation delay before entry (~0.5s)
- Cooldown after exits (TP / SL) to avoid back-to-back traps
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
BOT_VERSION = "ZUBBU_SCALPER_V4.5_HTF"

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

# Static SL, actually used now
SL_PCT = 0.0030   # 0.30% stop loss

# --- Orderflow filters (balanced-aggressive, with accumulation) ---
SCORE_MIN        = 0.90    # impulse score threshold
IMBALANCE_THRESH = 0.06    # 6% imbalance
BURST_MICRO_MIN  = 0.030   # micro burst (0.35s) minimum
BURST_ACCUM_MIN  = 0.060   # accumulation burst (5s) minimum
MAX_SPREAD       = 0.0012  # 0.12% max spread
MIN_RANGE_PCT    = 0.00020 # 0.012% minimum micro-range

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

# --------------- EXTRA FILTER CONSTANTS -----------------

# BTC volatility guard: if BTC 1m move > this, skip all entries
BTC_VOL_GUARD_PCT = 0.004  # 0.40% in 1 minute

# Velocity / acceleration
VEL_MIN_TRADES   = 8       # minimum trades in last 5s
BURST_SLOPE_MIN  = 0.003   # minimum net acceleration of burst (tune if needed)

# TP room expectation: we want space for ~0.30% move (more realistic)
TARGET_MOVE_PCT   = 0.003  # 0.30% target room

# Liquidity vacuum
LV_MAX_WALL_RATIO = 0.20   # opposite wall max 20% of our side liquidity

# 5m / 15m trend thresholds
TREND_5M_THRESH   = 0.002   # 0.20%
TREND_15M_THRESH  = 0.004   # 0.40%

# --------------- NEW CONTROL CONSTANTS -----------------

# Confirmation delay: wait after first signal before entering
CONFIRMATION_DELAY = 0.50  # seconds

# Cooldowns after exits
COOLDOWN_SL = 25.0   # seconds after loss
COOLDOWN_TP = 10.0   # seconds after TP / positive exit

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
        Compute mid, spread, imbalance, micro burst, accumulation burst, micro-range,
        and extra info for velocity / slope filters.
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

        # micro-range for small volatility check (last ~ACCUM_BURST_WINDOW seconds)
        if len(recent_prices) >= 2:
            high = max(recent_prices)
            low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0
        else:
            rng = 0.0

        # velocity and simple acceleration of burst
        trade_count = len(recent_trades)
        trade_velocity = trade_count / ACCUM_BURST_WINDOW

        burst_slope = 0.0
        if trade_count >= 2:
            mid_split = now - ACCUM_BURST_WINDOW / 2.0
            early = sum(v for ts, v in recent_trades if ts < mid_split)
            late = sum(v for ts, v in recent_trades if ts >= mid_split)
            burst_slope = late - early

        return {
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "burst": accum_burst,          # main burst used in score
            "burst_micro": micro_burst,    # confirmation burst
            "range_pct": rng,
            "burst_slope": burst_slope,
            "trade_count": trade_count,
            "trade_velocity": trade_velocity,
        }


# =====================================================
# MOMENTUM (for logging / score filter)
# =====================================================

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    """
    Simple impulse strength score.
    Higher = stronger push in one direction.
    """
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

        # price history buffers for 1m + 5m + 15m structure
        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_5m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_15m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}

        # skip-log cooldown (local reference)
        self.last_skip_log = LAST_SKIP_LOG

        # BTC volatility guard last log
        self.last_btc_guard_ts: float = 0.0

        # pending entry for confirmation delay
        self.pending_entry: Optional[dict] = None

        # exit cooldown state
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False

        # last watchdog timestamp (for 0.5s-ish frequency)
        self.last_watchdog_ts: float = 0.0

        # simple liquidity footprint memory per symbol
        # to detect sudden spoof walls or disappearing absorption
        self.last_liq_state: Dict[str, Optional[dict]] = {s: None for s in SYMBOLS_WS}

    # -------------------------------------------------
    # helpers: structure + trend + logging
    # -------------------------------------------------
    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        """Keep 1m + 5m + 15m rolling mid-price history."""
        buf1 = self.price_1m[sym]
        buf5 = self.price_5m[sym]
        buf15 = self.price_15m[sym]

        buf1.append((now, mid))
        buf5.append((now, mid))
        buf15.append((now, mid))

        cutoff1 = now - 60.0       # 1 minute
        cutoff5 = now - 300.0      # 5 minutes
        cutoff15 = now - 900.0     # 15 minutes

        while buf1 and buf1[0][0] < cutoff1:
            buf1.popleft()
        while buf5 and buf5[0][0] < cutoff5:
            buf5.popleft()
        while buf15 and buf15[0][0] < cutoff15:
            buf15.popleft()

    def _get_1m_context(self, sym: str) -> Optional[dict]:
        """
        Returns 1m micro structure for symbol:
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

        ctx = {
            "high": high,
            "low": low,
            "range": rng,
            "pos": pos_in_range,
            "near_support": pos_in_range <= 0.2,
            "near_resistance": pos_in_range >= 0.8,
        }
        return ctx

    def _get_5m_trend(self, sym: str) -> Optional[dict]:
        """
        Returns 5m trend context:
          - trend: "up", "down", "flat"
        """
        buf = self.price_5m[sym]
        if len(buf) < 10:
            return None

        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
        if first <= 0:
            return None

        change = (last - first) / first

        if change > TREND_5M_THRESH:
            trend = "up"
        elif change < -TREND_5M_THRESH:
            trend = "down"
        else:
            trend = "flat"

        return {"trend": trend, "change": change}

    def _get_15m_trend(self, sym: str) -> Optional[dict]:
        """
        Returns 15m higher timeframe trend:
          - trend: "up", "down", "flat"
        """
        buf = self.price_15m[sym]
        if len(buf) < 10:
            return None

        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
        if first <= 0:
            return None

        change = (last - first) / first

        if change > TREND_15M_THRESH:
            trend = "up"
        elif change < -TREND_15M_THRESH:
            trend = "down"
        else:
            trend = "flat"

        return {"trend": trend, "change": change}

    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        """
        Rate-limited skip logger so you see WHY the bot is not trading.
        """
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < SKIP_LOG_COOLDOWN:
            return  # only log same reason every few seconds per symbol

        self.last_skip_log[key] = now
        mid = feat.get("mid", 0.0)
        spread = feat.get("spread", 0.0)
        imb = feat.get("imbalance", 0.0)
        burst = feat.get("burst", 0.0)
        b_micro = feat.get("burst_micro", 0.0)
        rng = feat.get("range_pct", 0.0)
        slope = feat.get("burst_slope", 0.0)
        tcount = feat.get("trade_count", 0)
        tvel = feat.get("trade_velocity", 0.0)

        msg = (
            f"[SKIP] {sym} {reason} {extra} | "
            f"mid={mid:.4f} spread={spread:.6f} "
            f"imb={imb:.4f} burst={burst:.4f} micro={b_micro:.4f} "
            f"slope={slope:.4f} trades={tcount} vel={tvel:.2f} rng={rng:.6f}"
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
    # main decision loop
    # -------------------------------------------------
    async def eval_symbols_and_maybe_enter(self):
        """
        Scan all symbols, compute features, pick the best one,
        set a pending signal, and after CONFIRMATION_DELAY open position.
        Only runs when there is NO open position and cooldown allows it.
        """
        if self.position is not None:
            return

        now = time.time()

        # ----- EXIT COOLDOWN -----
        if self.last_exit_ts > 0.0:
            cooldown = COOLDOWN_SL if self.last_exit_was_loss else COOLDOWN_TP
            if now - self.last_exit_ts < cooldown:
                # cooling after recent exit
                await self.maybe_heartbeat()
                return

        # ======== BTC BUFFER REFRESH FIX ========
        btc_buf = self.price_1m.get("BTCUSDT")
        if btc_buf and len(btc_buf) > 2:
            # If buffer has very old values, reset it so guard turns OFF properly
            if now - btc_buf[0][0] > 75:
                print("[GUARD RESET] BTC history stale — refreshing buffer")
                btc_buf.clear()

        # ---------- BTC Volatility Guard ----------
        btc_buf = self.price_1m.get("BTCUSDT")
        if btc_buf and len(btc_buf) >= 2:
            p_first = btc_buf[0][1]
            p_last = btc_buf[-1][1]
            if p_first > 0:
                btc_change = abs(p_last - p_first) / p_first
                if btc_change > BTC_VOL_GUARD_PCT:
                    if now - self.last_btc_guard_ts > 10.0:
                        self.last_btc_guard_ts = now
                        print(
                            f"[GUARD] BTC 1m move={btc_change:.4f} > "
                            f"{BTC_VOL_GUARD_PCT:.4f} -> skip entries",
                            flush=True
                        )
                    await self.maybe_heartbeat()
                    return

        # ---------- PENDING ENTRY CONFIRMATION ----------
        if self.pending_entry:
            pe = self.pending_entry
            if now - pe["ts"] >= CONFIRMATION_DELAY:
                sym = pe["symbol"]
                side = pe["side"]
                feat_new = self.mkt.compute_features(sym)
                if not feat_new:
                    # data stale or missing, drop signal
                    self.pending_entry = None
                else:
                    imb = feat_new["imbalance"]
                    burst = feat_new["burst"]
                    micro = feat_new["burst_micro"]
                    slope = feat_new.get("burst_slope", 0.0)
                    spread = feat_new["spread"]

                    # direction must still align
                    if side == "buy":
                        if not (imb > 0 and burst > 0 and micro > 0 and slope > 0):
                            self._log_skip(
                                sym, "confirm_dir",
                                feat_new,
                                "direction flipped before confirm (LONG)"
                            )
                            self.pending_entry = None
                        else:
                            score = compute_momentum_score(imb, burst, spread)
                            if score < SCORE_MIN:
                                self._log_skip(
                                    sym, "confirm_score",
                                    feat_new,
                                    f"{score:.3f} < {SCORE_MIN}"
                                )
                                self.pending_entry = None
                            else:
                                await self.open_position(sym, feat_new, side)
                                self.pending_entry = None
                                return
                    else:  # side == "sell"
                        if not (imb < 0 and burst < 0 and micro < 0 and slope < 0):
                            self._log_skip(
                                sym, "confirm_dir",
                                feat_new,
                                "direction flipped before confirm (SHORT)"
                            )
                            self.pending_entry = None
                        else:
                            score = compute_momentum_score(imb, burst, spread)
                            if score < SCORE_MIN:
                                self._log_skip(
                                    sym, "confirm_score",
                                    feat_new,
                                    f"{score:.3f} < {SCORE_MIN}"
                                )
                                self.pending_entry = None
                            else:
                                await self.open_position(sym, feat_new, side)
                                self.pending_entry = None
                                return

            # If still waiting for confirmation time, don't search new signals
            if self.pending_entry:
                await self.maybe_heartbeat()
                return

        # ---------- NORMAL SCAN: find best new signal ----------
        best_score = 0.0
        best_sym: Optional[str] = None
        best_feat: Optional[dict] = None
        best_side: Optional[str] = None

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            imb = feat["imbalance"]
            burst = feat["burst"]          # accumulation burst
            b_micro = feat["burst_micro"]  # micro burst
            spread = feat["spread"]
            rng = feat["range_pct"]
            mid = feat["mid"]
            burst_slope = feat.get("burst_slope", 0.0)
            trade_count = feat.get("trade_count", 0)
            trade_velocity = feat.get("trade_velocity", 0.0)  # currently just for logs

            # update 1m/5m/15m buffers
            self._update_price_buffers(sym, mid, now)
            ctx_1m = self._get_1m_context(sym)
            ctx_5m = self._get_5m_trend(sym)
            ctx_15m = self._get_15m_trend(sym)

            # ---------------- BASIC FILTERS ----------------
            if spread <= 0 or spread > MAX_SPREAD:
                self._log_skip(sym, "spread", feat, f"> MAX_SPREAD({MAX_SPREAD})")
                continue
            if rng < MIN_RANGE_PCT:
                self._log_skip(sym, "range", feat, f"< MIN_RANGE_PCT({MIN_RANGE_PCT})")
                continue

            # imbalance strength
            if abs(imb) < IMBALANCE_THRESH:
                self._log_skip(sym, "imbalance", feat, f"< {IMBALANCE_THRESH}")
                continue

            # burst confirmation: accumulation + micro
            if abs(burst) < BURST_ACCUM_MIN:
                self._log_skip(sym, "burst_accum", feat, f"< {BURST_ACCUM_MIN}")
                continue
            if abs(b_micro) < BURST_MICRO_MIN:
                self._log_skip(sym, "burst_micro", feat, f"< {BURST_MICRO_MIN}")
                continue

            # ---------- Velocity confirmation ----------
            if trade_count < VEL_MIN_TRADES:
                self._log_skip(
                    sym,
                    "velocity",
                    feat,
                    f"trades={trade_count} < {VEL_MIN_TRADES}"
                )
                continue

            # acceleration filter
            if abs(burst_slope) < BURST_SLOPE_MIN:
                self._log_skip(sym, "burst_slope", feat, f"< {BURST_SLOPE_MIN}")
                continue

            # ---------------- DIRECTION FROM ORDERFLOW ----------------
            if imb > 0 and burst > 0 and b_micro > 0 and burst_slope > 0:
                side = "buy"
            elif imb < 0 and burst < 0 and b_micro < 0 and burst_slope < 0:
                side = "sell"
            else:
                self._log_skip(sym, "direction", feat, "imb/burst/micro/slope disagree")
                continue

            # ---------- Continuation filter (short-term price move) ----------
            buf1 = self.price_1m[sym]
            if len(buf1) >= 3:
                p_old = buf1[-3][1]
                p_new = buf1[-1][1]
                if p_old > 0:
                    price_chg = (p_new - p_old) / p_old
                    if side == "buy" and price_chg <= 0:
                        self._log_skip(sym, "continuation", feat, "price not moving up")
                        continue
                    if side == "sell" and price_chg >= 0:
                        self._log_skip(sym, "continuation", feat, "price not moving down")
                        continue

            # ---------------- IMPULSE SCORE ----------------
            score = compute_momentum_score(imb, burst, spread)
            if score < SCORE_MIN:
                self._log_skip(sym, "score", feat, f"{score:.3f} < {SCORE_MIN}")
                continue

            # ---------------- STRUCTURE FILTER (1m S/R) ----------------
            if ctx_1m:
                if side == "buy" and ctx_1m["near_resistance"]:
                    self._log_skip(sym, "structure", feat, "long at 1m resistance")
                    continue
                if side == "sell" and ctx_1m["near_support"]:
                    self._log_skip(sym, "structure", feat, "short at 1m support")
                    continue

            # ---------------- TREND FILTER (5m bias) ----------------
            if ctx_5m:
                trend = ctx_5m["trend"]
                if trend == "up" and side == "sell":
                    # allow only very strong counter-trend shorts
                    if score < SCORE_MIN * 1.8:
                        self._log_skip(sym, "trend_5m", feat, "uptrend, short too weak")
                        continue
                if trend == "down" and side == "buy":
                    # allow only very strong counter-trend longs
                    if score < SCORE_MIN * 1.8:
                        self._log_skip(sym, "trend_5m", feat, "downtrend, long too weak")
                        continue

            # ---------------- HIGHER TIMEFRAME FILTER (15m bias) ----------------
            if ctx_15m:
                t15 = ctx_15m["trend"]
                if t15 == "up" and side == "sell":
                    # require strong signal to go against 15m uptrend (relaxed vs old 2.2x)
                    if score < SCORE_MIN * 1.7:
                        self._log_skip(sym, "trend_15m", feat, "15m uptrend, short too weak")
                        continue
                if t15 == "down" and side == "buy":
                    if score < SCORE_MIN * 1.7:
                        self._log_skip(sym, "trend_15m", feat, "15m downtrend, long too weak")
                        continue

            # ====================================================
            # TP ROOM CHECK — we require room for at least ~0.30% move
            # ====================================================
            if ctx_1m:
                if side == "buy":
                    distance_to_res = (ctx_1m["high"] - mid) / mid
                    if distance_to_res < TARGET_MOVE_PCT:
                        self._log_skip(
                            sym,
                            "no_tp_room_long",
                            feat,
                            f"room={distance_to_res:.4f} < target={TARGET_MOVE_PCT:.4f}"
                        )
                        continue
                if side == "sell":
                    distance_to_sup = (mid - ctx_1m["low"]) / mid
                    if distance_to_sup < TARGET_MOVE_PCT:
                        self._log_skip(
                            sym,
                            "no_tp_room_short",
                            feat,
                            f"room={distance_to_sup:.4f} < target={TARGET_MOVE_PCT:.4f}"
                        )
                        continue

            # ====================================================
            # LIQUIDITY VACUUM + SIMPLE FOOTPRINT / SPOOF CHECK
            # Uses top 5 bids/asks from current book
            # ====================================================
            book = self.mkt.books.get(sym, {"bids": {}, "asks": {}})
            bids_sorted = sorted(book["bids"].items(), key=lambda x: -x[0])[:5]
            asks_sorted = sorted(book["asks"].items(), key=lambda x: x[0])[:5]
            bid_vol = sum(q for _, q in bids_sorted)
            ask_vol = sum(q for _, q in asks_sorted)

            now_ts = now
            prev_liq = self.last_liq_state.get(sym)
            if prev_liq is not None:
                dt = now_ts - prev_liq["ts"]
                if 0 < dt < 0.35:
                    prev_bid = prev_liq["bid_vol"]
                    prev_ask = prev_liq["ask_vol"]
                    prev_mid = prev_liq["mid"]
                    price_change = 0.0
                    if mid > 0:
                        price_change = abs(mid - prev_mid) / mid

                    # if opposite wall suddenly spikes by >60% with almost no price move in <0.35s -> spoof
                    if price_change < 0.0005:
                        if side == "buy":
                            if prev_ask > 0 and ask_vol > prev_ask * 1.6:
                                self._log_skip(sym, "spoof_ask_wall", feat, "")
                                self.last_liq_state[sym] = {
                                    "bid_vol": bid_vol,
                                    "ask_vol": ask_vol,
                                    "mid": mid,
                                    "ts": now_ts,
                                }
                                continue
                            # lack of bid absorption (iceberg vanished)
                            if prev_bid > 0 and bid_vol < prev_bid * 0.5:
                                self._log_skip(sym, "no_bid_absorption", feat, "")
                                self.last_liq_state[sym] = {
                                    "bid_vol": bid_vol,
                                    "ask_vol": ask_vol,
                                    "mid": mid,
                                    "ts": now_ts,
                                }
                                continue
                        else:  # side == "sell"
                            if prev_bid > 0 and bid_vol > prev_bid * 1.6:
                                self._log_skip(sym, "spoof_bid_wall", feat, "")
                                self.last_liq_state[sym] = {
                                    "bid_vol": bid_vol,
                                    "ask_vol": ask_vol,
                                    "mid": mid,
                                    "ts": now_ts,
                                }
                                continue
                            # lack of ask absorption (iceberg vanished)
                            if prev_ask > 0 and ask_vol < prev_ask * 0.5:
                                self._log_skip(sym, "no_ask_absorption", feat, "")
                                self.last_liq_state[sym] = {
                                    "bid_vol": bid_vol,
                                    "ask_vol": ask_vol,
                                    "mid": mid,
                                    "ts": now_ts,
                                }
                                continue

            # update footprint memory
            self.last_liq_state[sym] = {
                "bid_vol": bid_vol,
                "ask_vol": ask_vol,
                "mid": mid,
                "ts": now_ts,
            }

            # -------- LV filter --------
            if side == "buy":
                if bid_vol <= 0:
                    self._log_skip(sym, "lv_no_bid_liq", feat, "")
                    continue
                wall_ratio = ask_vol / (bid_vol + 1e-9)
                if wall_ratio > LV_MAX_WALL_RATIO:
                    self._log_skip(
                        sym,
                        "lv_sell_wall",
                        feat,
                        f"ratio={wall_ratio:.2f} > {LV_MAX_WALL_RATIO:.2f}"
                    )
                    continue

            if side == "sell":
                if ask_vol <= 0:
                    self._log_skip(sym, "lv_no_ask_liq", feat, "")
                    continue
                wall_ratio = bid_vol / (ask_vol + 1e-9)
                if wall_ratio > LV_MAX_WALL_RATIO:
                    self._log_skip(
                        sym,
                        "lv_buy_wall",
                        feat,
                        f"ratio={wall_ratio:.2f} > {LV_MAX_WALL_RATIO:.2f}"
                    )
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

        # tiny anti-spam: don't double-trigger same symbol in <0.5s
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        # set pending entry for confirmation instead of entering instantly
        self.pending_entry = {
            "symbol": best_sym,
            "feat": best_feat,
            "side": best_side,
            "ts": now,
        }
        print(
            f"[SIGNAL] {best_sym} {best_side.upper()} score={best_score:.3f} "
            f"waiting {CONFIRMATION_DELAY:.2f}s confirmation"
        )

    # -------------------------------------------------
    # order execution
    # -------------------------------------------------
    async def open_position(self, sym_ws: str, feat: dict, side: str):
        """
        Open a new position with ADAPTIVE TP, STATIC SL and dynamic ladder SL.
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

        # determine adaptive TP based on micro range (volatility)
        rng = feat.get("range_pct", 0.0)
        if rng < 0.0045:         # <0.45% micro range
            tp_pct = 0.0040      # target ~0.40%
        else:
            tp_pct = 0.0060      # target ~0.60%

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
            tp_price = entry_price * (1.0 + tp_pct)
            sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - tp_pct)
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
            f"TP={tp_price:.4f} SL={sl_price:.4f} (tp_pct={tp_pct*100:.2f}%)"
        )
        await send_telegram(
            f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL={sl_price:.4f} (tp={tp_pct*100:.2f}%, ladder enabled)"
        )

    # -------------------------------------------------
    # risk watchdog: detect TP/SL hits + backup SL + ladder trail
    # -------------------------------------------------
    async def watchdog_position(self):
        """
        - Detect if exchange closed position (TP hit or SL hit)
        - If still open, apply ladder dynamic SL + backup SL check
        """
        now = time.time()
        # throttle watchdog to ~0.5s
        if now - self.last_watchdog_ts < 0.5:
            return
        self.last_watchdog_ts = now

        if not self.position:
            return

        pos = self.position
        sym = pos.symbol_ws

        # ========== 1) CHECK IF POSITION SIZE OPEN OR CLOSED ==========
        size = await self.exchange.get_position_size(sym)

        # If exchange shows no open position -> TP or SL executed
        if size <= 0:
            # Recompute last move for reason calculation
            feat = self.mkt.compute_features(sym)
            reason = "unknown"
            was_loss = True

            if feat:
                mid = feat["mid"]
                if pos.side == "buy":
                    move = (mid - pos.entry_price) / pos.entry_price
                else:
                    move = (pos.entry_price - mid) / pos.entry_price

                # Profit or loss detection
                if move > 0:
                    reason = "TP filled (profit/ladder)"
                    was_loss = False
                else:
                    reason = "SL filled (loss/ladder)"
                    was_loss = True

            # -------- LOG & TELEGRAM --------
            print(
                f"[EXIT DETECTED] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} "
                f"tp={pos.tp_price:.4f} sl={pos.sl_price:.4f} reason={reason}"
            )
            await send_telegram(
                f"📤 EXIT — {sym} {pos.side.upper()} entry={pos.entry_price:.4f}\n"
                f"➡️ {reason}"
            )

            # set exit cooldown
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = was_loss

            # CLEAR POSITION
            self.position = None
            return

        # ========== 2) IF STILL OPEN, GET CURRENT PRICE ==========
        feat = self.mkt.compute_features(sym)
        if not feat:
            return
        mid = feat["mid"]
        if mid <= 0:
            return

        # ========== 3) DYNAMIC LADDER SL UPDATE ==========
        entry = pos.entry_price

        # profit calculation
        if pos.side == "buy":
            profit_pct = (mid - entry) / entry
        else:
            profit_pct = (entry - mid) / entry

        new_sl = pos.sl_price
        reason = None

        # ----- Level 1: +0.20% -> SL to entry -----
        if profit_pct >= 0.0020:
            if pos.side == "buy":
                target_sl = entry
                if target_sl > new_sl:
                    new_sl = target_sl
                    reason = "L1: SL → breakeven"
            else:
                target_sl = entry
                if target_sl < new_sl:
                    new_sl = target_sl
                    reason = "L1: SL → breakeven"

        # ----- Level 2: +0.40% -> SL to +0.20% -----
        if profit_pct >= 0.0040:
            if pos.side == "buy":
                target_sl = entry * 1.0020  # +0.20%
                if target_sl > new_sl:
                    new_sl = target_sl
                    reason = "L2: SL → +0.20%"
            else:
                target_sl = entry * 0.9980  # -0.20%
                if target_sl < new_sl:
                    new_sl = target_sl
                    reason = "L2: SL → +0.20%"

        # ----- Level 3: +0.60% -> SL to +0.40% -----
        if profit_pct >= 0.0060:
            if pos.side == "buy":
                target_sl = entry * 1.0040  # +0.40%
                if target_sl > new_sl:
                    new_sl = target_sl
                    reason = "L3: SL → +0.40%"
            else:
                target_sl = entry * 0.9960  # -0.40%
                if target_sl < new_sl:
                    new_sl = target_sl
                    reason = "L3: SL → +0.40%"

        # ----- If SL updated -> apply -----
        if new_sl != pos.sl_price:
            pos.sl_price = new_sl
            print(f"[LADDER SL] {sym} {reason}, new SL={new_sl:.4f}")
            await send_telegram(
                f"🔁 {sym} {reason} — SL updated to {new_sl:.4f}"
            )

        # ========== 4) BACKUP SL CHECK ==========
        hit = False

        # long backup SL
        if pos.side == "buy" and mid <= pos.sl_price:
            hit = True

        # short backup SL
        if pos.side == "sell" and mid >= pos.sl_price:
            hit = True

        if hit:
            # Force close market
            await self.exchange.close_position_market(sym)

            print(
                f"[BACKUP SL] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} "
                f"SL={pos.sl_price:.4f} now={mid:.4f}"
            )
            await send_telegram(
                f"🛑 BACKUP SL — {sym} {pos.side.upper()}\n"
                f"Entry={pos.entry_price:.4f}  SL={pos.sl_price:.4f}"
            )

            # set exit cooldown as loss
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = True

            self.position = None
            return

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
        print(f"[HEARTBEAT] idle, equity={eq:.2f}")
        await send_telegram(
            f"💤 Bot idle {HEARTBEAT_IDLE_SEC/60:.0f} min. Equity={eq:.2f} USDT"
        )


# --------------- WEBSOCKET LOOP -----------------


async def ws_loop(mkt: MarketState):
    """
    One WS connection, multiple topics: orderbook.1 + publicTrade for all symbols.
    Updates MarketState in real-time.
    """

    # Correct topics
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

                            # Ignore subscription confirmation
                            if data.get("success") is True:
                                continue

                            topic = data.get("topic")
                            if not topic:
                                continue

                            # -------- ORDERBOOK ----------
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                # Bybit sends array sometimes
                                if isinstance(payload, list):
                                    payload = payload[0]
                                if isinstance(payload, dict):
                                    mkt.update_book(sym, payload)

                            # -------- TRADES ----------
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
                            print("⚠ WS ERROR — reconnecting")
                            break

        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}")
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)


# --------------- DEBUG CONSOLE (runs in separate thread) -----------------


def debug_console(mkt: MarketState, bot: ScalperBot):
    """
    Simple blocking console running in a daemon thread.
    You can type commands directly in tmux where bot is running.

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
            # stdin closed, just stop thread
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
    print(f"Starting {BOT_VERSION} ...")
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
            # watchdog is self-throttled to ~0.5s
            await bot.watchdog_position()
            await asyncio.sleep(0.25)  # main scan loop at 0.25s
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
