#!/usr/bin/env python3
# ZUBBU_SWEEP_V4.5_COMBINED.py
# Combined: V4.5 framework (WS/parser/TG/exchange/watchdog) + Sweep entry logic + 1m candle system

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
import ccxt  # sync ccxt; heavy calls wrapped with asyncio.to_thread

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_SWEEP_V4.5_COMBINED"

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

LEVERAGE = 3
EQUITY_USE_FRACTION = 0.97  # use up to 97% equity * leverage as position notional

# Static SL (will be used by ladder system)
SL_PCT = 0.0060   # 0.60% stop loss

# Sweep-specific constants (from your sweep bot)
CAPITAL_RISK_PCT = 1.0
TP_PCT = 0.30
SWEEP_WICK_MIN = 3.0
RSI_OB = 80
RSI_OS = 20
VOL_SPIKE = 2.0
CONFIRM_BODY_PCT = 0.05
MAX_SPREAD_PCT = 0.04
BTC_VOL_MAX = 0.004
SIGNAL_COOLDOWN = 180

# Other V4.5 constants that remain in file (not used for sweep other than reuse)
SCORE_MIN = 0.90
IMBALANCE_THRESH = 0.06
BURST_MICRO_MIN = 0.030
BURST_ACCUM_MIN = 0.060
MAX_SPREAD = 0.0012
MIN_RANGE_PCT = 0.00020

# Market data timing (for feature staleness)
BOOK_STALE_SEC = 6.0

# Risk & heartbeat
KILL_SWITCH_DD = 0.05
HEARTBEAT_IDLE_SEC = 1800

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# ----------------- TELEGRAM -----------------

_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0  # at most 1 msg every 30s to avoid spam


async def send_telegram(msg: str):
    """Safe, rate-limited Telegram sender (V4.5 style)."""
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


# alias compatibility
async def send_tg(msg: str):
    return await send_telegram(msg)


# --------------- UTILS -----------------


def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# --------------- EXCHANGE CLIENT (ccxt sync wrapped into async) -----------------


class ExchangeClient:
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
                self.client.set_leverage(lev, symbol, params={"category": "linear"})
            except Exception:
                pass

        await asyncio.to_thread(_work)

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

    async def get_position_size(self, sym_ws: str) -> float:
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

    async def create_market_order(self, sym_ws: str, side: str, qty: float, reduce_only: bool = False):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True

        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)

        return await asyncio.to_thread(_work)

    async def create_limit_order(
        self, sym_ws: str, side: str, qty: float, price: float, reduce_only: bool = False, post_only: bool = False
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
    side: str
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    opened_ts: float


class MarketState:
    """
    Holds orderbook + trades for all symbols, updated from WS.
    Also includes the 1-minute candle builder and RSI/SMA used by sweep logic.
    """

    def __init__(self):
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=3000) for s in SYMBOLS_WS}
        self.candles: Dict[str, deque] = {s: deque(maxlen=150) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}
        self.last_skip_log: Dict[str, float] = {}

    # Orderbook parser (kept from V4.5)
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]

        # handle array-wrapped payloads
        if isinstance(data, list) and data:
            data = data[0]

        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000)
            book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

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

        # legacy fallback
        ts_raw = data.get("ts") or (time.time() * 1000)
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

        # incremental updates legacy
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
        self.trades[symbol].append(trade)

    # ----------------- Candle helpers (added from sweep bot) -----------------
    def build_1m_candles(self, sym: str):
        now = int(time.time())
        trades = [t for t in self.trades[sym] if t["ts"] >= now - 60]
        if not trades:
            return
        prices = [t["price"] for t in trades]
        candle = {"open": prices[0], "high": max(prices), "low": min(prices),
                  "close": prices[-1], "volume": sum(t["size"] for t in trades), "ts": now}
        if not self.candles[sym] or candle["ts"] // 60 != self.candles[sym][-1]["ts"] // 60:
            self.candles[sym].append(candle)

    def last_candle(self, sym: str):
        return self.candles[sym][-2] if len(self.candles[sym]) > 1 else None

    def current_candle(self, sym: str):
        return self.candles[sym][-1] if self.candles[sym] else None

    def volume_sma(self, sym: str, n: int = 20):
        closed = list(self.candles[sym])[:-1] if len(self.candles[sym]) > 1 else list(self.candles[sym])
        if len(closed) < n:
            return None
        return sum(c["volume"] for c in closed[-n:]) / n

    def rsi(self, sym: str, n: int = 14):
        closed = list(self.candles[sym])[:-1] if len(self.candles[sym]) > 1 else list(self.candles[sym])
        closes = [c["close"] for c in closed]
        if len(closes) < n + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i - 1]
            gains.append(max(ch, 0)); losses.append(max(-ch, 0))
        avg_gain = sum(gains[-n:]) / n
        avg_loss = sum(losses[-n:]) / n
        if avg_loss == 0:
            return 100
        return 100 - 100 / (1 + avg_gain / avg_loss)

    # helper to get best bid/ask
    def get_best_bid_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        best_bid = max(book["bids"].keys())
        best_ask = min(book["asks"].keys())
        return best_bid, best_ask


# =====================================================
# Momentum helpers (kept but not used by sweep entry itself)
# =====================================================

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    if spread <= 0:
        spread = 1e-6
    return abs(imbalance) * abs(burst) / spread


# --------------- CORE BOT (V4.5) but with sweep entry -----------------


class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt

        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

        # price history buffers for 1m + 5m + 15m structure (used by guards)
        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_5m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_15m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}

        # skip-log cooldown (local reference)
        self.last_skip_log: Dict[str, float] = {}

        # pending entry not used (sweep enters immediately)
        self.pending_entry: Optional[dict] = None

        # exit cooldown state for sweep
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False

        # last watchdog timestamp
        self.last_watchdog_ts: float = 0.0

        # counters (simple)
        self.counters = {"executed": 0, "skipped": 0, "tg_sent": 0, "errors": 0}

    # ---------------- helpers for price buffers (kept)
    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        buf1 = self.price_1m[sym]
        buf5 = self.price_5m[sym]
        buf15 = self.price_15m[sym]

        buf1.append((now, mid)); buf5.append((now, mid)); buf15.append((now, mid))

        cutoff1 = now - 60.0; cutoff5 = now - 300.0; cutoff15 = now - 900.0
        while buf1 and buf1[0][0] < cutoff1: buf1.popleft()
        while buf5 and buf5[0][0] < cutoff5: buf5.popleft()
        while buf15 and buf15[0][0] < cutoff15: buf15.popleft()

    def _get_1m_context(self, sym: str) -> Optional[dict]:
        buf = self.price_1m[sym]
        if len(buf) < 5: return None
        prices = [p for _, p in buf]
        high = max(prices); low = min(prices); rng = high - low
        if rng <= 0: return None
        last_price = prices[-1]; pos_in_range = (last_price - low) / rng
        return {"high": high, "low": low, "range": rng, "pos": pos_in_range,
                "near_support": pos_in_range <= 0.2, "near_resistance": pos_in_range >= 0.8}

    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT.")
        # set leverage for all symbols
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.")
        await send_telegram("⚙️ Leverage set for all symbols.")

    async def maybe_kill_switch(self):
        if self.start_equity is None: return
        eq = await self.exchange.fetch_equity()
        if self.start_equity <= 0: return
        dd = (self.start_equity - eq) / self.start_equity
        if dd >= KILL_SWITCH_DD:
            if self.position:
                await self.exchange.close_position_market(self.position.symbol_ws)
                await send_telegram(f"🚨 Kill-switch: equity drawdown {dd*100:.2f}%. Position closed.")
                self.position = None
            raise SystemExit("Kill-switch triggered")

    # ----------------- SWEEP LOGIC (entry) -----------------
    def _log_skip(self, sym: str, reason: str, extra: str = ""):
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < 5.0:
            return
        self.last_skip_log[key] = now
        print(f"[SKIP] {sym} {reason} {extra}", flush=True)
        self.counters["skipped"] += 1

    def btc_vol_high(self):
        btc = self.mkt.last_candle("BTCUSDT")
        if not btc: return False
        try:
            return (btc["high"] - btc["low"]) / btc["open"] > BTC_VOL_MAX
        except Exception:
            return False

    def spread_ok(self, sym: str) -> bool:
        book = self.mkt.books[sym]
        if not book["bids"] or not book["asks"]:
            return False
        best_bid = max(book["bids"]); best_ask = min(book["asks"])
        return (best_ask - best_bid) / best_ask <= MAX_SPREAD_PCT

    def detect_sweep(self, sym: str) -> (bool, Optional[str]):
        # Build candle from recent trades
        self.mkt.build_1m_candles(sym)
        last = self.mkt.last_candle(sym)
        if not last: return False, None
        body = abs(last["close"] - last["open"])
        if body == 0: return False, None
        uwick = last["high"] - max(last["open"], last["close"])
        lwick = min(last["open"], last["close"]) - last["low"]
        if not (uwick >= SWEEP_WICK_MIN * body or lwick >= SWEEP_WICK_MIN * body): return False, None

        vol_sma = self.mkt.volume_sma(sym)
        if not vol_sma or last["volume"] < VOL_SPIKE * vol_sma: return False, None

        rsi = self.mkt.rsi(sym)
        if rsi is None: return False, None

        side = None
        if uwick >= SWEEP_WICK_MIN * body and rsi >= RSI_OB:
            side = "sell"
        if lwick >= SWEEP_WICK_MIN * body and rsi <= RSI_OS:
            side = "buy"
        if not side: return False, None

        curr = self.mkt.current_candle(sym)
        if not curr: return False, None
        if abs(curr["close"] - curr["open"]) < CONFIRM_BODY_PCT * curr["open"]:
            return False, None

        # confirm direction vs previous candle
        if side == "sell" and curr["close"] < last["close"]:
            return True, "sell"
        if side == "buy" and curr["close"] > last["close"]:
            return True, "buy"
        return False, None

    async def eval_symbols_and_maybe_enter(self):
        # Only sweep entry logic — no scalper/burst logic
        # Only one position at a time
        if self.position is not None:
            return

        if self.btc_vol_high():
            self._log_skip("BTCUSDT", "btc_vol_high", f"BTC_VOL_MAX={BTC_VOL_MAX}")
            return

        now = time.time()
        for sym in SYMBOLS_WS:
            # spread check
            if not self.spread_ok(sym):
                self._log_skip(sym, "spread", f"> MAX_SPREAD_PCT({MAX_SPREAD_PCT})")
                continue

            # cooldown per symbol
            if now - self.mkt.last_signal_ts.get(sym, 0.0) < SIGNAL_COOLDOWN:
                self._log_skip(sym, "cooldown", f"cooldown={SIGNAL_COOLDOWN}s")
                continue

            sweep, side = self.detect_sweep(sym)
            if not sweep:
                self._log_skip(sym, "no_sweep", "")
                continue

            # fetch equity and compute qty using same method as sweep file
            try:
                bal = await self.exchange.fetch_equity()
            except Exception as e:
                self._log_skip(sym, "balance_fetch_err", str(e))
                continue
            equity = float(bal or 0.0)
            # compute mid
            best = self.mkt.get_best_bid_ask(sym)
            if not best:
                self._log_skip(sym, "no_book_levels", "")
                continue
            mid = (best[0] + best[1]) / 2.0
            if mid <= 0:
                self._log_skip(sym, "mid_zero", "")
                continue

            qty = round(equity * CAPITAL_RISK_PCT / 100 / mid, 3)
            if qty <= 0:
                self._log_skip(sym, "qty_zero", f"equity={equity}")
                continue

            # Place market entry using existing open_position code (reuse open_position)
            try:
                # call open_position which handles order placement, TP placement, and position object
                await self.open_position(sym, {"mid": mid}, side)
                self.mkt.last_signal_ts[sym] = time.time()
                self.counters["executed"] += 1
                # send_tg done inside open_position
            except Exception as e:
                self.counters["errors"] += 1
                print(f"[ENTRY ERROR] {sym}: {e}", flush=True)
                await send_tg(f"❌ Entry failed for {sym}: {e}")

    # ---------------- Existing V4.5 order execution (kept) -----------------
    async def open_position(self, sym_ws: str, feat: dict, side: str):
        """
        Reuse V4.5 open_position logic but accept feat minimally (we compute qty here using equity+mid).
        """
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0:
            return

        # compute mid from feat if present or from book
        mid = feat.get("mid") if feat.get("mid") else None
        if mid is None:
            best = self.mkt.get_best_bid_ask(sym_ws)
            if not best:
                return
            mid = (best[0] + best[1]) / 2.0

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

        # Determine TP % using simple rule (we keep this conservative)
        tp_pct = 0.0040  # default 0.4% (you can tune)
        # ENTRY: market order
        try:
            order = await self.exchange.create_market_order(sym_ws, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}", flush=True)
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
            await self.exchange.create_limit_order(sym_ws, opp_side, qty, tp_price, reduce_only=True, post_only=True)
        except Exception as e:
            print(f"[TP ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"⚠️ TP order placement failed for {sym_ws}")

        self.position = Position(symbol_ws=sym_ws, side=side, qty=qty, entry_price=entry_price,
                                 tp_price=tp_price, sl_price=sl_price, opened_ts=time.time())
        self.last_trade_time = time.time()
        print(f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} TP={tp_price:.4f} SL={sl_price:.4f}")
        await send_telegram(f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} TP={tp_price:.4f} SL={sl_price:.4f}")

    # ----------------- risk watchdog (kept from V4.5) -----------------
    async def watchdog_position(self):
        now = time.time()
        if now - self.last_watchdog_ts < 0.5:
            return
        self.last_watchdog_ts = now

        if not self.position:
            return

        pos = self.position
        sym = pos.symbol_ws

        # 1) check if exchange shows open position
        size = await self.exchange.get_position_size(sym)
        if size <= 0:
            feat = None
            try:
                feat = self.mkt.compute_features(sym) if hasattr(self.mkt, "compute_features") else None
            except Exception:
                feat = None
            reason = "unknown"
            was_loss = True
            if feat:
                mid = feat.get("mid", 0.0)
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
            print(f"[EXIT DETECTED] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} tp={pos.tp_price:.4f} sl={pos.sl_price:.4f} reason={reason}")
            await send_telegram(f"📤 EXIT — {sym} {pos.side.upper()} entry={pos.entry_price:.4f}\n➡️ {reason}")
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = was_loss
            self.position = None
            return

        # 2) if still open, get mid
        feat = None
        try:
            # Use compute_features if present, else compute mid from book
            if hasattr(self.mkt, "compute_features"):
                feat = self.mkt.compute_features(sym)
            if not feat:
                b = self.mkt.get_best_bid_ask(sym)
                if not b:
                    return
                mid = (b[0] + b[1]) / 2.0
            else:
                mid = feat["mid"]
        except Exception:
            return

        # dynamic ladder SL implementation (simplified / kept)
        entry = pos.entry_price
        if pos.side == "buy":
            profit_pct = (mid - entry) / entry
        else:
            profit_pct = (entry - mid) / entry

        new_sl = pos.sl_price
        reason = None

        if profit_pct >= 0.0020:
            target_sl = entry
            if (pos.side == "buy" and target_sl > new_sl) or (pos.side == "sell" and target_sl < new_sl):
                new_sl = target_sl; reason = "L1: breakeven"

        if profit_pct >= 0.0040:
            target_sl = entry * 1.0020 if pos.side == "buy" else entry * 0.9980
            if (pos.side == "buy" and target_sl > new_sl) or (pos.side == "sell" and target_sl < new_sl):
                new_sl = target_sl; reason = "L2: +0.20%"

        if profit_pct >= 0.0060:
            target_sl = entry * 1.0040 if pos.side == "buy" else entry * 0.9960
            if (pos.side == "buy" and target_sl > new_sl) or (pos.side == "sell" and target_sl < new_sl):
                new_sl = target_sl; reason = "L3: +0.40%"

        if new_sl != pos.sl_price:
            pos.sl_price = new_sl
            print(f"[LADDER SL] {sym} {reason}, new SL={new_sl:.4f}", flush=True)
            await send_telegram(f"🔁 {sym} {reason} — SL updated to {new_sl:.4f}")

        # backup SL check
        hit = False
        if pos.side == "buy" and mid <= pos.sl_price:
            hit = True
        if pos.side == "sell" and mid >= pos.sl_price:
            hit = True

        if hit:
            await self.exchange.close_position_market(sym)
            print(f"[BACKUP SL] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} SL={pos.sl_price:.4f} now={mid:.4f}", flush=True)
            await send_telegram(f"🛑 BACKUP SL — {sym} {pos.side.upper()} Entry={pos.entry_price:.4f}  SL={pos.sl_price:.4f}")
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = True
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


# --------------- WEBSOCKET LOOP (V4.5 parser kept) -----------------


async def ws_loop(mkt: MarketState):
    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")

    print("⚡ WS loop started... connecting...")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, receive_timeout=40, heartbeat=20) as ws:
                    print("📡 Connected to WS server, subscribing...")
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
                                    mkt.update_book(sym, payload)

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
                                    mkt.add_trade(sym, {"price": price, "size": qty, "side": "buy" if side == "buy" else "sell", "ts": now_ts})
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting", flush=True)
                            break

        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)


# --------------- DEBUG CONSOLE -----------------


def debug_console(mkt: MarketState, bot: ScalperBot):
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
            print("[DEBUG] position =", getattr(bot, "position", None), flush=True)
        elif cmd == "help":
            print(help_text, flush=True)
        elif cmd == "":
            continue
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)


# --------------- MAIN -----------------


async def main():
    print(f"Starting {BOT_VERSION} ...")
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

    # debug console thread
    threading.Thread(target=debug_console, args=(mkt, bot), daemon=True).start()

    await bot.init_equity_and_leverage()

    ws_task = asyncio.create_task(ws_loop(mkt))
    try:
        while True:
            await bot.maybe_kill_switch()
            await bot.eval_symbols_and_maybe_enter()
            await bot.watchdog_position()
            await asyncio.sleep(0.25)
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
