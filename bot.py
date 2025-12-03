#!/usr/bin/env python3
"""
Bybit USDT Perp Micro-Scalper V4.1 (Option X – Flow Scalper)

- One WS connection, 5 symbols (BTC, ETH, BNB, SOL, DOGE)
- Orderbook + recent trade burst signal engine
- Dynamic TP based on impulse strength + trend
- Fixed, tight SL (client-side watchdog)
- Only 1 open position at a time (best symbol chosen)
- Built-in debug console (type commands in tmux: ws, book, trades, pos, help)
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
import ccxt  # sync ccxt, wrapped with asyncio.to_thread for non-blocking IO

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_SCALPER_V4_1_FLOW"


# --------- MOVE ANALYZER (for Dynamic TP learning) ---------

class MoveAnalyzer:
    def __init__(self, max_len: int = 500):
        self.moves = deque(maxlen=max_len)

    def add_move(self, pct: float) -> None:
        self.moves.append(pct)

    def avg_move(self) -> float:
        if not self.moves:
            return 0.006  # default 0.6%
        return sum(self.moves) / len(self.moves)


# Global analyzer
move_analyzer = MoveAnalyzer()


# --------------- ENV / BASIC CONFIG -----------------

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

LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95  # use up to 95% equity * leverage as position notional

SL_PCT = 0.0035  # 0.35% SL

# --- Orderflow filters (balanced–aggressive, NOT strict) ---
SCORE_MIN = 0.60          # impulse score threshold (reduced from 0.75 -> more trades)
IMBALANCE_THRESH = 0.015  # 1.5% imbalance
BURST_THRESH = 0.020      # 2.0% burst threshold (relative size)
MAX_SPREAD = 0.0015       # 0.15% max spread
MIN_RANGE_PCT = 0.00020   # 0.020% micro-range

# Market data timing
RECENT_TRADE_WINDOW = 0.35     # 350 ms for burst window
BOOK_STALE_SEC = 6.0           # ignore orderbook older than 6 seconds

# Risk & heartbeat
KILL_SWITCH_DD = 0.05          # 5% equity drawdown -> stop trading
HEARTBEAT_IDLE_SEC = 1800      # 30 minutes idle heartbeat

# "Real push" threshold: want ~0.40% potential
PUSH_THRESHOLD = 0.004         # 0.40% move potential

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

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

    async def fetch_equity(self) -> float:
        """Wallet balance + unrealized PnL."""
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

    async def fetch_upnl(self) -> float:
        def _work():
            try:
                positions = self.client.fetch_positions()
                return sum(
                    safe_float(p.get("unrealizedPnl"), 0.0) or 0.0
                    for p in positions
                )
            except Exception:
                return 0.0

        return await asyncio.to_thread(_work)

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

    async def fetch_equity(self) -> float:
        """Wallet balance + unrealized PnL."""
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

    async def fetch_upnl(self) -> float:
        def _work():
            try:
                positions = self.client.fetch_positions()
                return sum(
                    safe_float(p.get("unrealizedPnl"), 0.0) or 0.0
                    for p in positions
                )
            except Exception:
                return 0.0

        return await asyncio.to_thread(_work)

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
        self.trades: Dict[str, deque] = {s: deque(maxlen=1000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (supports new Bybit format) --------
    def update_book(self, symbol: str, data: dict):
        """
        Update local L2 book from WS message.
        Supports:
          - Legacy format: {"type": "snapshot"/"delta", "bids": [...], "asks": [...]}
          - New format:    {"b": [...], "a": [...], "ts": ...}
        """
        book = self.books[symbol]

        # New compact snapshot format: "b" / "a"
        if isinstance(data, dict) and "b" in data and "a" in data:
            # full refresh of top levels
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

            ts_raw = data.get("ts", time.time() * 1000.0)
            book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0
            return

        # ---- Legacy format below ----
        ts_raw = data.get("ts")
        ts = safe_float(ts_raw, time.time() * 1000.0) / 1000.0
        book["ts"] = ts

        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()
            for px, qty in data.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p is None or q is None:
                    continue
                book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p is None or q is None:
                    continue
                book["asks"][p] = q
            return

        # incremental updates
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p is None or q is None:
                    continue
                if q == 0:
                    book["bids"].pop(p, None)
                else:
                    book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p is None or q is None:
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
        """
        Compute mid, spread, imbalance, burst, micro-range for decision engine.
        Returns None if book/trades are not usable.
        """
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

        # recent burst from trades (last RECENT_TRADE_WINDOW seconds)
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

        # micro-range for small volatility check
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
        self.trades: Dict[str, deque] = {s: deque(maxlen=1000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (100% correct Bybit V5 + legacy support) --------
    def update_book(self, symbol: str, data: dict):
        """
        Correct parser for Bybit V5 "orderbook.1.<symbol>" stream.
        Supports:
          - NEW format: {"b": [...], "a": [...], "ts": ...}
          - DELTA updates (b/a with qty=0 or qty>0)
          - OLD format fallback: {"type": "snapshot"/"delta", "bids": [...], "asks": [...]}
        """

        book = self.books[symbol]

        # ---------------------------
        # NEW BYBIT V5 FORMAT (b / a)
        # ---------------------------
        if isinstance(data, dict) and ("b" in data or "a" in data):

            # Timestamp update
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000)
            book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

            # If BOTH b and a → FULL SNAPSHOT REFRESH
            if "b" in data and "a" in data:
                book["bids"].clear()
                book["asks"].clear()

                # snapshot bids
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p and q and q > 0:
                        book["bids"][p] = q

                # snapshot asks
                for px, qty in data.get("a", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p and q and q > 0:
                        book["asks"][p] = q

                return  # finished snapshot

            # DELTA UPDATE (only b or only a)
            if "b" in data:
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if not p:
                        continue
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q

            if "a" in data:
                for px, qty in data.get("a", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if not p:
                        continue
                    if q == 0:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q

            return  # finished delta update

        # ---------------------------
        # LEGACY FORMAT (FALLBACK)
        # ---------------------------
        ts_raw = data.get("ts", time.time() * 1000)
        book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

        typ = data.get("type")

        # Legacy snapshot
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()

            for px, qty in data.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p and q:
                    book["bids"][p] = q

            for px, qty in data.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p and q:
                    book["asks"][p] = q

            return

        # Legacy delta (delete/update/insert)
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p:
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q

            for px, qty in part.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p:
                    if q == 0:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q

    # -------------------------------------------------------------------
    #                           TRADE STORAGE
    # -------------------------------------------------------------------
    def add_trade(self, symbol: str, trade: dict):
        """Store trade: {price, size, side, ts}"""
        self.trades[symbol].append(trade)

    # -------------------------------------------------------------------
    #                       GET BEST BID / ASK
    # -------------------------------------------------------------------
    def get_best_bid_ask(self, symbol: str):
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        best_bid = max(book["bids"].keys())
        best_ask = min(book["asks"].keys())
        return best_bid, best_ask

    # -------------------------------------------------------------------
    #                     COMPUTE FEATURES (imbalance, burst, etc.)
    # -------------------------------------------------------------------
    def compute_features(self, symbol: str):
        """
        Compute mid, spread, imbalance, burst, micro-range.
        Returns None if unusable.
        """
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None

        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC:
            return None

        # Top 5 levels
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

        # Burst calc from trades
        cutoff = now - RECENT_TRADE_WINDOW
        burst = 0.0
        recent_prices = []
        last_price = None

        for t in reversed(self.trades[symbol]):
            if t["ts"] < cutoff:
                break
            if t["side"] == "buy":
                burst += t["size"]
            else:
                burst -= t["size"]
            last_price = t["price"]
            recent_prices.append(t["price"])

        if last_price is None:
            return None

        # micro range
        if len(recent_prices) >= 2:
            high = max(recent_prices)
            low = min(recent_prices)
            rng = (high - low) / mid
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
# MOMENTUM & DYNAMIC TP HELPERS
# =====================================================

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    """
    Simple impulse strength score.
    Higher = stronger push in one direction.
    """
    if spread <= 0:
        spread = 1e-6
    return abs(imbalance) * abs(burst) / spread


def choose_dynamic_tp(
    imbalance: float,
    burst: float,
    spread: float,
    range_pct: float,
) -> float:
    """
    Select TP % based on impulse strength.
    Uses orderflow only (no heavy history), so it stays fast.
    """
    score = compute_momentum_score(imbalance, burst, spread)

    # Weak impulse → small TP
    if score < 2.0:
        return 0.004   # 0.4 %

    # Normal impulse
    if score < 4.0:
        return 0.006   # 0.6 %

    # Strong impulse
    if score < 8.0:
        return 0.009   # 0.9 %

    # Very strong impulse (rare but powerful)
    return 0.012       # 1.2 %


# --------------- CORE BOT LOGIC -----------------


class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt

        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

        # price history buffers for 1m + 5m structure
        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_5m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}

        # skip-log cooldown (to avoid spam)
        self.last_skip_log: Dict[str, float] = {}

    # -------------------------------------------------
    # helpers: structure + trend + logging
    # -------------------------------------------------
    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        """Keep 1m + 5m rolling mid-price history."""
        buf1 = self.price_1m[sym]
        buf5 = self.price_5m[sym]

        buf1.append((now, mid))
        buf5.append((now, mid))

        cutoff1 = now - 60.0       # 1 minute
        cutoff5 = now - 300.0      # 5 minutes

        while buf1 and buf1[0][0] < cutoff1:
            buf1.popleft()
        while buf5 and buf5[0][0] < cutoff5:
            buf5.popleft()

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
            "near_support": pos_in_range <= 0.15,
            "near_resistance": pos_in_range >= 0.85,
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

        if change > 0.0018:      # +0.18% or more = uptrend
            trend = "up"
        elif change < -0.0018:   # -0.18% or more = downtrend
            trend = "down"
        else:
            trend = "flat"

        return {"trend": trend, "change": change}

    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        """
        Rate-limited skip logger so you see WHY the bot is not trading.
        Shows roughly every 3s per (symbol+reason).
        """
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < 3.0:
            return  # only log same reason every 3s per symbol

        self.last_skip_log[key] = now
        mid = feat.get("mid", 0.0)
        spread = feat.get("spread", 0.0)
        imb = feat.get("imbalance", 0.0)
        burst = feat.get("burst", 0.0)
        rng = feat.get("range_pct", 0.0)

        msg = (
            f"[SKIP] {sym} {reason} {extra} | "
            f"mid={mid:.4f} spread={spread:.6f} "
            f"imb={imb:.4f} burst={burst:.4f} rng={rng:.6f}"
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
            await bot.watchdog_position()
            await asyncio.sleep(1.0)  # 1-second scan loop
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
    asyncio.run(main())
