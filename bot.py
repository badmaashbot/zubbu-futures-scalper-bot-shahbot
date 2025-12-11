#!/usr/bin/env python3
"""
ZUBBU_REVERSAL_V3_OPTION_A — Full bot.py (Pure Reversal, Option A)
- Keeps original parser, WS, orderbook handling, Telegram, debug console, skip logs.
- Replaces only decision logic with Option A: Bollinger touch + wick rejection + RSI curl + microflow fade + confirmation delay.
- TP = 0.25% fixed, SL = 0.18% fixed (change constants below if desired).
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
import statistics

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_REVERSAL_V3_OPTION_A"

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

# Pure Reversal fixed TP / SL for Option A
TP_PCT = 0.0025   # 0.25% take profit
SL_PCT = 0.0018   # 0.18% stop loss

# --- Orderflow filters (kept but less strict for Option A) ---
SCORE_MIN        = 0.90    # impulse score threshold
IMBALANCE_THRESH = 0.06    # 6% imbalance
BURST_MICRO_MIN  = 0.030   # micro burst (0.35s) minimum
BURST_ACCUM_MIN  = 0.060   # accumulation burst (5s) minimum
MAX_SPREAD       = 0.0012  # 0.12% max spread
MIN_RANGE_PCT    = 0.00020 # 0.012% minimum micro-range

# Market data timing
RECENT_TRADE_WINDOW = 0.35  # 350 ms micro burst window (used for microflow)
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

# --------------- PURE REVERSAL PARAMETERS (OPTION A) -----------------
BB_PERIOD = 20
BB_STD = 2.0
RSI_PERIOD = 14
WICK_MIN_RATIO = 0.40      # wick >= 40% of body
CONFIRMATION_DELAY = 0.35  # seconds to re-check before entry
MICROFLOW_WINDOW = 0.35    # seconds for microflow fade check
VOLATILITY_THRESHOLD = 0.006  # 0.6% 1-minute range skip (optional guard)

# --------------- TELEGRAM -----------------

_last_tg_ts = 0.0
TG_MIN_INTERVAL = 10.0   # at most 1 msg every 10s to avoid spam


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
            timeout=aiohttp.ClientTimeout(total=4.0)
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


# ---------------- Indicators helpers for Option A -----------------
def bollinger_from_prices(prices: List[float], period=BB_PERIOD, std=BB_STD):
    if len(prices) < period:
        return None
    window = prices[-period:]
    mean = statistics.mean(window)
    variance = statistics.pvariance(window)
    dev = math.sqrt(variance)
    upper = mean + std * dev
    lower = mean - std * dev
    return upper, mean, lower


def rsi_from_prices(prices: List[float], period=RSI_PERIOD) -> Optional[float]:
    """
    Simple RSI based on price closes (Wilder's smoothing not applied for speed).
    It is acceptable for quick confirmation in this bot.
    """
    if len(prices) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(-period, 0):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains.append(diff)
        else:
            losses.append(-diff)
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


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


# --------------- CORE BOT LOGIC (Option A) -----------------


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

        # BTC volatility guard last log (legacy quick guard)
        self.last_btc_guard_ts: float = 0.0

        # ONE-SHOT guard state (new)
        self.guard_active: bool = False
        self.guard_start_ts: float = 0.0

        # pending entry for confirmation delay (Option A)
        self.pending_entry: Optional[dict] = None

        # exit cooldown state
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False

        # last watchdog timestamp (for 0.5s-ish frequency)
        self.last_watchdog_ts: float = 0.0

        # simple liquidity footprint memory per symbol (kept for compatibility)
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
            "open": prices[0],
            "close": prices[-1],
            "prices": prices
        }
        return ctx

    def _get_5m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_5m[sym]
        if len(buf) < 10:
            return None
        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
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

    def _get_15m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_15m[sym]
        if len(buf) < 10:
            return None
        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
        if first <= 0:
            return None
        change = (last - first) / first
        if change > 0.004:
            trend = "up"
        elif change < -0.004:
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
    # Option A: main decision loop (pure reversal)
    # -------------------------------------------------
    async def eval_symbols_and_maybe_enter(self):
        """
        Option A scanning:
        - For each symbol compute features (orderbook-based features kept)
        - Use 1m price buffer to compute BB & RSI
        - Candidate when price touches BB extremes + wick rejection + RSI curl + microflow fade
        - Set pending_entry and confirm after CONFIRMATION_DELAY
        """
        if self.position is not None:
            return

        now = time.time()

        # ---------------- BTC Volatility One-Shot Guard (60s) ----------------
        # Uses 1m buffer for BTC. If a large move happens, block new entries for 60s.
        btc_buf = self.price_1m.get("BTCUSDT")
        if btc_buf and len(btc_buf) >= 2:
            p_first = btc_buf[0][1]
            p_last = btc_buf[-1][1]
            if p_first > 0:
                btc_change = abs(p_last - p_first) / p_first

                # Trigger guard if BTC spikes and guard currently OFF
                if btc_change > BTC_VOL_GUARD_PCT and not self.guard_active:
                    self.guard_active = True
                    self.guard_start_ts = now
                    print(f"[GUARD] BTC spike {btc_change:.4f} > {BTC_VOL_GUARD_PCT:.4f} -> blocking 60s", flush=True)
                    await send_telegram("⚠️ GUARD TRIGGERED — BTC volatility high, pausing 60s")

                # If guard active, block entries for 60 seconds then reset
                if self.guard_active:
                    if now - self.guard_start_ts < 60.0:
                        # still cooling
                        await self.maybe_heartbeat()
                        return
                    else:
                        self.guard_active = False
                        print("[GUARD] Reset — entries re-enabled", flush=True)
                        await send_telegram("🟢 GUARD RESET — entries re-enabled")

        # legacy BTC fast guard/log (keeps the previous behavior log cadence)
        if btc_buf and len(btc_buf) >= 2:
            p_first = btc_buf[0][1]
            p_last = btc_buf[-1][1]
            if p_first > 0:
                btc_change = abs(p_last - p_first) / p_first
                if btc_change > 0.004:
                    if now - self.last_btc_guard_ts > 10.0:
                        self.last_btc_guard_ts = now
                        print(
                            f"[GUARD] BTC 1m move={btc_change:.4f} > 0.004 -> skip entries",
                            flush=True
                        )
                    await self.maybe_heartbeat()
                    return

        # If we have a pending entry — check confirmation
        if self.pending_entry:
            pe = self.pending_entry
            if now - pe["ts"] >= CONFIRMATION_DELAY:
                sym = pe["symbol"]
                side = pe["side"]
                feat_new = self.mkt.compute_features(sym)
                if not feat_new:
                    self.pending_entry = None
                else:
                    # Re-evaluate the Option A conditions quickly
                    ok = self._check_option_a_conditions(sym, side, feat_new)
                    if ok:
                        await self._open_entry_from_pending(sym, feat_new, side)
                        self.pending_entry = None
                        return
                    else:
                        self._log_skip(sym, "confirm_failed", feat_new, "confirmation failed")
                        await send_telegram(f"❌ PENDING DROPPED {sym} {side.upper()}")
                        self.pending_entry = None
            else:
                # still waiting for confirm time
                await self.maybe_heartbeat()
                return

        # Scan for new signal
        best_sym = None
        best_feat = None
        best_side = None

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            mid = feat["mid"]

            # Update price buffers
            self._update_price_buffers(sym, mid, now)
            ctx_1m = self._get_1m_context(sym)
            if not ctx_1m:
                continue

            prices_1m = ctx_1m["prices"]
            # Need enough samples for indicators
            if len(prices_1m) < max(BB_PERIOD, RSI_PERIOD) + 2:
                continue

            # Compute bollinger
            bb = bollinger_from_prices(prices_1m, BB_PERIOD, BB_STD)
            if not bb:
                self._log_skip(sym, "bb_not_ready", feat, "")
                continue
            upper, midbb, lower = bb

            last_price = prices_1m[-1]

            # Check touch extremes
            touched_upper = last_price >= upper * 0.995
            touched_lower = last_price <= lower * 1.005
            if not (touched_upper or touched_lower):
                self._log_skip(sym, "bb_not_touch", feat, "")
                continue

            # Build minute candle (open/close/high/low)
            open_p = ctx_1m["open"]
            close_p = ctx_1m["close"]
            high_p = ctx_1m["high"]
            low_p = ctx_1m["low"]
            body = abs(close_p - open_p)
            if body <= 1e-9:
                self._log_skip(sym, "tiny_body", feat, "")
                continue
            upper_wick = max(0.0, high_p - max(open_p, close_p))
            lower_wick = max(0.0, min(open_p, close_p) - low_p)

            # Determine side candidate
            side = "sell" if touched_upper else "buy"

            # Wick strength
            wick_ok = False
            if side == "sell":
                if upper_wick / body >= WICK_MIN_RATIO:
                    wick_ok = True
            else:
                if lower_wick / body >= WICK_MIN_RATIO:
                    wick_ok = True
            if not wick_ok:
                self._log_skip(sym, "weak_wick", feat, f"up={upper_wick:.6f} low={lower_wick:.6f}")
                continue

            # RSI curl confirmation
            r_now = rsi_from_prices(prices_1m, RSI_PERIOD)
            r_prev = rsi_from_prices(prices_1m[:-1], RSI_PERIOD)
            if r_now is None or r_prev is None:
                self._log_skip(sym, "rsi_not_ready", feat, "")
                continue
            if side == "sell" and not (r_now < r_prev):
                self._log_skip(sym, "rsi_no_curl_down", feat, f"rnow={r_now:.2f} rprev={r_prev:.2f}")
                continue
            if side == "buy" and not (r_now > r_prev):
                self._log_skip(sym, "rsi_no_curl_up", feat, f"rnow={r_now:.2f} rprev={r_prev:.2f}")
                continue

            # Microflow fade (use recent trades)
            now_ts = time.time()
            buys = 0.0
            sells = 0.0
            for t in reversed(self.mkt.trades[sym]):
                if now_ts - t["ts"] > MICROFLOW_WINDOW:
                    break
                if t["side"] == "buy":
                    buys += t["size"]
                else:
                    sells += t["size"]
            microflow = buys - sells
            # For a short candidate (touched upper), microflow should be negative (selling pressure)
            if side == "sell" and microflow >= 0:
                self._log_skip(sym, "microflow_not_fade_short", feat, f"mf={microflow:.3f}")
                continue
            if side == "buy" and microflow <= 0:
                self._log_skip(sym, "microflow_not_fade_long", feat, f"mf={microflow:.3f}")
                continue

            # Volatility guard (skip if massive 1m range)
            one_min_range_pct = (high_p - low_p) / ((high_p + low_p) / 2.0) if (high_p + low_p) > 0 else 0.0
            if one_min_range_pct > VOLATILITY_THRESHOLD:
                self._log_skip(sym, "vol_spike", feat, f"rng={one_min_range_pct:.4f}")
                continue

            # All Option A filters passed — mark as candidate
            # Score by absolute microflow and imbalance (simple)
            score = abs(feat.get("imbalance", 0.0)) + abs(microflow)
            if score > 0 and score > (best_feat["score"] if best_feat else 0):
                best_sym = sym
                best_feat = {"feat": feat, "score": score, "last_price": last_price}
                best_side = side

        if not best_sym:
            await self.maybe_heartbeat()
            return

        # Anti-spam: don't trigger same symbol too fast
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        # Set pending entry for confirmation
        self.pending_entry = {
            "symbol": best_sym,
            "side": best_side,
            "ts": now
        }
        print(f"[SIGNAL] {best_sym} {best_side.upper()} — pending {CONFIRMATION_DELAY}s confirmation")
        await send_telegram(f"⏳ PENDING {best_sym} {best_side.upper()} — confirm {CONFIRMATION_DELAY}s")

    # Helper: re-evaluate Option A conditions (used for confirmation)
    def _check_option_a_conditions(self, sym: str, side: str, feat: dict) -> bool:
        """
        Re-check the same Option A filters quickly before entry (used during confirmation).
        """
        now = time.time()
        mid = feat["mid"]
        self._update_price_buffers(sym, mid, now)
        ctx = self._get_1m_context(sym)
        if not ctx:
            return False
        prices = ctx["prices"]
        if len(prices) < max(BB_PERIOD, RSI_PERIOD) + 2:
            return False
        bb = bollinger_from_prices(prices, BB_PERIOD, BB_STD)
        if not bb:
            return False
        upper, midbb, lower = bb
        last_price = prices[-1]
        touched_upper = last_price >= upper * 0.995
        touched_lower = last_price <= lower * 1.005
        if side == "sell" and not touched_upper:
            return False
        if side == "buy" and not touched_lower:
            return False
        open_p = ctx["open"]
        close_p = ctx["close"]
        high_p = ctx["high"]
        low_p = ctx["low"]
        body = abs(close_p - open_p)
        if body <= 1e-9:
            return False
        upper_wick = max(0.0, high_p - max(open_p, close_p))
        lower_wick = max(0.0, min(open_p, close_p) - low_p)
        if side == "sell":
            if upper_wick / body < WICK_MIN_RATIO:
                return False
        else:
            if lower_wick / body < WICK_MIN_RATIO:
                return False
        r_now = rsi_from_prices(prices, RSI_PERIOD)
        r_prev = rsi_from_prices(prices[:-1], RSI_PERIOD)
        if r_now is None or r_prev is None:
            return False
        if side == "sell" and not (r_now < r_prev):
            return False
        if side == "buy" and not (r_now > r_prev):
            return False
        # microflow
        buys = sells = 0.0
        now_ts = time.time()
        for t in reversed(self.mkt.trades[sym]):
            if now_ts - t["ts"] > MICROFLOW_WINDOW:
                break
            if t["side"] == "buy":
                buys += t["size"]
            else:
                sells += t["size"]
        microflow = buys - sells
        if side == "sell" and microflow >= 0:
            return False
        if side == "buy" and microflow <= 0:
            return False
        # passed all
        return True

    # Open position based on Option A fixed TP/SL
    async def _open_entry_from_pending(self, sym: str, feat: dict, side: str):
        """
        Execute market entry with fixed TP and SL (Option A).
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
        min_qty = MIN_QTY_MAP.get(sym, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym} qty {qty:.6f} < min {min_qty}", flush=True)
            await send_telegram(f"❌ ENTRY SKIPPED {sym} qty too small")
            return
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return
        # ENTRY: market order
        try:
            order = await self.exchange.create_market_order(sym, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym}: {e}")
            await send_telegram(f"❌ Entry failed for {sym}")
            return
        # compute TP/SL fixed
        if side == "buy":
            tp_price = entry_price * (1.0 + TP_PCT)
            sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - TP_PCT)
            sl_price = entry_price * (1.0 + SL_PCT)
        # place TP as limit reduce-only maker
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(
                sym,
                opp_side,
                qty,
                tp_price,
                reduce_only=True,
                post_only=True,
            )
        except Exception as e:
            print(f"[TP ERROR] {sym}: {e}")
            await send_telegram(f"⚠️ TP order placement failed for {sym}")
        self.position = Position(
            symbol_ws=sym,
            side=side,
            qty=qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            opened_ts=time.time(),
        )
        self.last_trade_time = time.time()
        print(
            f"[ENTRY] {sym} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL={sl_price:.4f}"
        )
        await send_telegram(
            f"📌 ENTRY {sym} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL={sl_price:.4f}"
        )

    # -------------------------------------------------
    # risk watchdog: detect TP/SL hits + backup SL + ladder trail
    # (kept simple for Option A)
    # -------------------------------------------------
    async def watchdog_position(self):
        """
        - Detect if exchange closed position (TP hit or SL hit)
        - If still open, apply backup SL check
        """
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
                    reason = "TP filled (profit)"
                    was_loss = False
                else:
                    reason = "SL filled (loss)"
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

        # ========== 3) BACKUP SL CHECK ==========
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
        )# --------------- CORE BOT LOGIC (Option A) -----------------


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

        # BTC volatility guard last log (legacy quick guard)
        self.last_btc_guard_ts: float = 0.0

        # ONE-SHOT guard state (new)
        self.guard_active: bool = False
        self.guard_start_ts: float = 0.0

        # pending entry for confirmation delay (Option A)
        self.pending_entry: Optional[dict] = None

        # exit cooldown state
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False

        # last watchdog timestamp (for 0.5s-ish frequency)
        self.last_watchdog_ts: float = 0.0

        # simple liquidity footprint memory per symbol (kept for compatibility)
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
            "open": prices[0],
            "close": prices[-1],
            "prices": prices
        }
        return ctx

    def _get_5m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_5m[sym]
        if len(buf) < 10:
            return None
        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
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

    def _get_15m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_15m[sym]
        if len(buf) < 10:
            return None
        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
        if first <= 0:
            return None
        change = (last - first) / first
        if change > 0.004:
            trend = "up"
        elif change < -0.004:
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
    # Option A: main decision loop (pure reversal)
    # -------------------------------------------------
    async def eval_symbols_and_maybe_enter(self):
        """
        Option A scanning:
        - For each symbol compute features (orderbook-based features kept)
        - Use 1m price buffer to compute BB & RSI
        - Candidate when price touches BB extremes + wick rejection + RSI curl + microflow fade
        - Set pending_entry and confirm after CONFIRMATION_DELAY
        """
        if self.position is not None:
            return

        now = time.time()

        # ---------------- BTC Volatility One-Shot Guard (60s) ----------------
        # Uses 1m buffer for BTC. If a large move happens, block new entries for 60s.
        btc_buf = self.price_1m.get("BTCUSDT")
        if btc_buf and len(btc_buf) >= 2:
            p_first = btc_buf[0][1]
            p_last = btc_buf[-1][1]
            if p_first > 0:
                btc_change = abs(p_last - p_first) / p_first

                # Trigger guard if BTC spikes and guard currently OFF
                if btc_change > BTC_VOL_GUARD_PCT and not self.guard_active:
                    self.guard_active = True
                    self.guard_start_ts = now
                    print(f"[GUARD] BTC spike {btc_change:.4f} > {BTC_VOL_GUARD_PCT:.4f} -> blocking 60s", flush=True)
                    await send_telegram("⚠️ GUARD TRIGGERED — BTC volatility high, pausing 60s")

                # If guard active, block entries for 60 seconds then reset
                if self.guard_active:
                    if now - self.guard_start_ts < 60.0:
                        # still cooling
                        await self.maybe_heartbeat()
                        return
                    else:
                        self.guard_active = False
                        print("[GUARD] Reset — entries re-enabled", flush=True)
                        await send_telegram("🟢 GUARD RESET — entries re-enabled")

        # legacy BTC fast guard/log (keeps the previous behavior log cadence)
        if btc_buf and len(btc_buf) >= 2:
            p_first = btc_buf[0][1]
            p_last = btc_buf[-1][1]
            if p_first > 0:
                btc_change = abs(p_last - p_first) / p_first
                if btc_change > 0.004:
                    if now - self.last_btc_guard_ts > 10.0:
                        self.last_btc_guard_ts = now
                        print(
                            f"[GUARD] BTC 1m move={btc_change:.4f} > 0.004 -> skip entries",
                            flush=True
                        )
                    await self.maybe_heartbeat()
                    return

        # If we have a pending entry — check confirmation
        if self.pending_entry:
            pe = self.pending_entry
            if now - pe["ts"] >= CONFIRMATION_DELAY:
                sym = pe["symbol"]
                side = pe["side"]
                feat_new = self.mkt.compute_features(sym)
                if not feat_new:
                    self.pending_entry = None
                else:
                    # Re-evaluate the Option A conditions quickly
                    ok = self._check_option_a_conditions(sym, side, feat_new)
                    if ok:
                        await self._open_entry_from_pending(sym, feat_new, side)
                        self.pending_entry = None
                        return
                    else:
                        self._log_skip(sym, "confirm_failed", feat_new, "confirmation failed")
                        await send_telegram(f"❌ PENDING DROPPED {sym} {side.upper()}")
                        self.pending_entry = None
            else:
                # still waiting for confirm time
                await self.maybe_heartbeat()
                return

        # Scan for new signal
        best_sym = None
        best_feat = None
        best_side = None

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            mid = feat["mid"]

            # Update price buffers
            self._update_price_buffers(sym, mid, now)
            ctx_1m = self._get_1m_context(sym)
            if not ctx_1m:
                continue

            prices_1m = ctx_1m["prices"]
            # Need enough samples for indicators
            if len(prices_1m) < max(BB_PERIOD, RSI_PERIOD) + 2:
                continue

            # Compute bollinger
            bb = bollinger_from_prices(prices_1m, BB_PERIOD, BB_STD)
            if not bb:
                self._log_skip(sym, "bb_not_ready", feat, "")
                continue
            upper, midbb, lower = bb

            last_price = prices_1m[-1]

            # Check touch extremes
            touched_upper = last_price >= upper * 0.995
            touched_lower = last_price <= lower * 1.005
            if not (touched_upper or touched_lower):
                self._log_skip(sym, "bb_not_touch", feat, "")
                continue

            # Build minute candle (open/close/high/low)
            open_p = ctx_1m["open"]
            close_p = ctx_1m["close"]
            high_p = ctx_1m["high"]
            low_p = ctx_1m["low"]
            body = abs(close_p - open_p)
            if body <= 1e-9:
                self._log_skip(sym, "tiny_body", feat, "")
                continue
            upper_wick = max(0.0, high_p - max(open_p, close_p))
            lower_wick = max(0.0, min(open_p, close_p) - low_p)

            # Determine side candidate
            side = "sell" if touched_upper else "buy"

            # Wick strength
            wick_ok = False
            if side == "sell":
                if upper_wick / body >= WICK_MIN_RATIO:
                    wick_ok = True
            else:
                if lower_wick / body >= WICK_MIN_RATIO:
                    wick_ok = True
            if not wick_ok:
                self._log_skip(sym, "weak_wick", feat, f"up={upper_wick:.6f} low={lower_wick:.6f}")
                continue

            # RSI curl confirmation
            r_now = rsi_from_prices(prices_1m, RSI_PERIOD)
            r_prev = rsi_from_prices(prices_1m[:-1], RSI_PERIOD)
            if r_now is None or r_prev is None:
                self._log_skip(sym, "rsi_not_ready", feat, "")
                continue
            if side == "sell" and not (r_now < r_prev):
                self._log_skip(sym, "rsi_no_curl_down", feat, f"rnow={r_now:.2f} rprev={r_prev:.2f}")
                continue
            if side == "buy" and not (r_now > r_prev):
                self._log_skip(sym, "rsi_no_curl_up", feat, f"rnow={r_now:.2f} rprev={r_prev:.2f}")
                continue

            # Microflow fade (use recent trades)
            now_ts = time.time()
            buys = 0.0
            sells = 0.0
            for t in reversed(self.mkt.trades[sym]):
                if now_ts - t["ts"] > MICROFLOW_WINDOW:
                    break
                if t["side"] == "buy":
                    buys += t["size"]
                else:
                    sells += t["size"]
            microflow = buys - sells
            # For a short candidate (touched upper), microflow should be negative (selling pressure)
            if side == "sell" and microflow >= 0:
                self._log_skip(sym, "microflow_not_fade_short", feat, f"mf={microflow:.3f}")
                continue
            if side == "buy" and microflow <= 0:
                self._log_skip(sym, "microflow_not_fade_long", feat, f"mf={microflow:.3f}")
                continue

            # Volatility guard (skip if massive 1m range)
            one_min_range_pct = (high_p - low_p) / ((high_p + low_p) / 2.0) if (high_p + low_p) > 0 else 0.0
            if one_min_range_pct > VOLATILITY_THRESHOLD:
                self._log_skip(sym, "vol_spike", feat, f"rng={one_min_range_pct:.4f}")
                continue

            # All Option A filters passed — mark as candidate
            # Score by absolute microflow and imbalance (simple)
            score = abs(feat.get("imbalance", 0.0)) + abs(microflow)
            if score > 0 and score > (best_feat["score"] if best_feat else 0):
                best_sym = sym
                best_feat = {"feat": feat, "score": score, "last_price": last_price}
                best_side = side

        if not best_sym:
            await self.maybe_heartbeat()
            return

        # Anti-spam: don't trigger same symbol too fast
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        # Set pending entry for confirmation
        self.pending_entry = {
            "symbol": best_sym,
            "side": best_side,
            "ts": now
        }
        print(f"[SIGNAL] {best_sym} {best_side.upper()} — pending {CONFIRMATION_DELAY}s confirmation")
        await send_telegram(f"⏳ PENDING {best_sym} {best_side.upper()} — confirm {CONFIRMATION_DELAY}s")

    # Helper: re-evaluate Option A conditions (used for confirmation)
    def _check_option_a_conditions(self, sym: str, side: str, feat: dict) -> bool:
        """
        Re-check the same Option A filters quickly before entry (used during confirmation).
        """
        now = time.time()
        mid = feat["mid"]
        self._update_price_buffers(sym, mid, now)
        ctx = self._get_1m_context(sym)
        if not ctx:
            return False
        prices = ctx["prices"]
        if len(prices) < max(BB_PERIOD, RSI_PERIOD) + 2:
            return False
        bb = bollinger_from_prices(prices, BB_PERIOD, BB_STD)
        if not bb:
            return False
        upper, midbb, lower = bb
        last_price = prices[-1]
        touched_upper = last_price >= upper * 0.995
        touched_lower = last_price <= lower * 1.005
        if side == "sell" and not touched_upper:
            return False
        if side == "buy" and not touched_lower:
            return False
        open_p = ctx["open"]
        close_p = ctx["close"]
        high_p = ctx["high"]
        low_p = ctx["low"]
        body = abs(close_p - open_p)
        if body <= 1e-9:
            return False
        upper_wick = max(0.0, high_p - max(open_p, close_p))
        lower_wick = max(0.0, min(open_p, close_p) - low_p)
        if side == "sell":
            if upper_wick / body < WICK_MIN_RATIO:
                return False
        else:
            if lower_wick / body < WICK_MIN_RATIO:
                return False
        r_now = rsi_from_prices(prices, RSI_PERIOD)
        r_prev = rsi_from_prices(prices[:-1], RSI_PERIOD)
        if r_now is None or r_prev is None:
            return False
        if side == "sell" and not (r_now < r_prev):
            return False
        if side == "buy" and not (r_now > r_prev):
            return False
        # microflow
        buys = sells = 0.0
        now_ts = time.time()
        for t in reversed(self.mkt.trades[sym]):
            if now_ts - t["ts"] > MICROFLOW_WINDOW:
                break
            if t["side"] == "buy":
                buys += t["size"]
            else:
                sells += t["size"]
        microflow = buys - sells
        if side == "sell" and microflow >= 0:
            return False
        if side == "buy" and microflow <= 0:
            return False
        # passed all
        return True

    # Open position based on Option A fixed TP/SL
    async def _open_entry_from_pending(self, sym: str, feat: dict, side: str):
        """
        Execute market entry with fixed TP and SL (Option A).
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
        min_qty = MIN_QTY_MAP.get(sym, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym} qty {qty:.6f} < min {min_qty}", flush=True)
            await send_telegram(f"❌ ENTRY SKIPPED {sym} qty too small")
            return
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return
        # ENTRY: market order
        try:
            order = await self.exchange.create_market_order(sym, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym}: {e}")
            await send_telegram(f"❌ Entry failed for {sym}")
            return
        # compute TP/SL fixed
        if side == "buy":
            tp_price = entry_price * (1.0 + TP_PCT)
            sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - TP_PCT)
            sl_price = entry_price * (1.0 + SL_PCT)
        # place TP as limit reduce-only maker
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(
                sym,
                opp_side,
                qty,
                tp_price,
                reduce_only=True,
                post_only=True,
            )
        except Exception as e:
            print(f"[TP ERROR] {sym}: {e}")
            await send_telegram(f"⚠️ TP order placement failed for {sym}")
        self.position = Position(
            symbol_ws=sym,
            side=side,
            qty=qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            opened_ts=time.time(),
        )
        self.last_trade_time = time.time()
        print(
            f"[ENTRY] {sym} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL={sl_price:.4f}"
        )
        await send_telegram(
            f"📌 ENTRY {sym} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL={sl_price:.4f}"
        )

    # -------------------------------------------------
    # risk watchdog: detect TP/SL hits + backup SL + ladder trail
    # (kept simple for Option A)
    # -------------------------------------------------
    async def watchdog_position(self):
        """
        - Detect if exchange closed position (TP hit or SL hit)
        - If still open, apply backup SL check
        """
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
                    reason = "TP filled (profit)"
                    was_loss = False
                else:
                    reason = "SL filled (loss)"
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

        # ========== 3) BACKUP SL CHECK ==========
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


# --------------- WEBSOCKET LOOP (kept original trade+orderbook subscriptions) -----------------


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
            try:
                await bot.maybe_kill_switch()
            except SystemExit:
                raise
            except Exception as e:
                print("[ERR] maybe_kill_switch", e)

            try:
                await bot.eval_symbols_and_maybe_enter()
            except Exception as e:
                print("[ERR] eval loop", e)

            try:
                await bot.watchdog_position()
            except Exception as e:
                print("[ERR] watchdog", e)

            await asyncio.sleep(0.12)  # main scan loop
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
