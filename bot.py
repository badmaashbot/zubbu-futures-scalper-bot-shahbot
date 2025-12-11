#!/usr/bin/env python3
"""
ZUBBU_REVERSAL_V4 — Pure Reversal (Option B: High Safety)
- Full bot file with fast/slow market detector tuned for HIGH SAFETY (Option B).
- Pauses aggressively in volatile/trending conditions and only trades in calm/ranging periods.
- Keeps original WS/orderbook parser, trade feed, Telegram, debug console, and execution wrappers.
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
BOT_VERSION = "ZUBBU_REVERSAL_V4_OPTION_B"

# ---------------- ENV / BASIC CONFIG ----------------
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

# Symbols
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

# Trading config
LEVERAGE = 5
EQUITY_USE_FRACTION = 0.97
TP_PCT = 0.0025
SL_PCT = 0.0018

# Timing and data windows
RECENT_TRADE_WINDOW = 0.35
ACCUM_BURST_WINDOW = 5.0
BOOK_STALE_SEC = 6.0

# Safety / thresholds (Option B - High Safety)
BB_PERIOD = 20
BB_STD = 2.0
RSI_PERIOD = 14
WICK_MIN_RATIO = 0.45      # stricter wick requirement (45% of body)
CONFIRMATION_DELAY = 1.5   # seconds
MICROFLOW_WINDOW = 0.35
VOLATILITY_THRESHOLD = 0.0035  # be stricter for high safety
BTC_VOL_GUARD_PCT = 0.004

# Fast/Slow detector thresholds (strict)
FAST_RANGE_PCT = 0.0025    # 0.25% 1-minute range considered FAST
FAST_TRADE_VEL = 3.0      # trades/sec in 5s window
FAST_BURST_SLOPE = 0.7
FAST_SPREAD = 0.0009

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

# Telegram
_last_tg_ts = 0.0
TG_MIN_INTERVAL = 10.0

async def send_telegram(msg: str):
    global _last_tg_ts
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    now = time.time()
    if now - _last_tg_ts < TG_MIN_INTERVAL:
        return
    _last_tg_ts = now
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4.0)) as session:
            await session.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                                data={"chat_id": TG_CHAT_ID, "text": msg})
    except Exception:
        pass

def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

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

    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000)
            book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0
            if "b" in data and "a" in data:
                book["bids"].clear(); book["asks"].clear()
                for px, qty in data.get("b", []):
                    p = safe_float(px); q = safe_float(qty)
                    if p and q: book["bids"][p] = q
                for px, qty in data.get("a", []):
                    p = safe_float(px); q = safe_float(qty)
                    if p and q: book["asks"][p] = q
                return
            if "b" in data:
                for px, qty in data["b"]:
                    p = safe_float(px); q = safe_float(qty)
                    if not p: continue
                    if q == 0: book["bids"].pop(p, None)
                    else: book["bids"][p] = q
            if "a" in data:
                for px, qty in data["a"]:
                    p = safe_float(px); q = safe_float(qty)
                    if not p: continue
                    if q == 0: book["asks"].pop(p, None)
                    else: book["asks"][p] = q
            return
        ts_raw = data.get("ts")
        book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0
        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear(); book["asks"].clear()
            for px, qty in data.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p and q: book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p and q: book["asks"][p] = q
            return
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px); q = safe_float(qty)
                if p:
                    if q == 0: book["bids"].pop(p, None)
                    else: book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px); q = safe_float(qty)
                if p:
                    if q == 0: book["asks"].pop(p, None)
                    else: book["asks"][p] = q

    def add_trade(self, symbol: str, trade: dict):
        self.trades[symbol].append(trade)

    def get_best_bid_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]: return None
        best_bid = max(book["bids"].keys()); best_ask = min(book["asks"].keys())
        return best_bid, best_ask

    def compute_features(self, symbol: str) -> Optional[dict]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]: return None
        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC: return None
        bids_sorted = sorted(book["bids"].items(), key=lambda x: -x[0])[:5]
        asks_sorted = sorted(book["asks"].items(), key=lambda x: x[0])[:5]
        bid_vol = sum(q for _, q in bids_sorted); ask_vol = sum(q for _, q in asks_sorted)
        if bid_vol + ask_vol == 0: return None
        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)
        best_bid = bids_sorted[0][0]; best_ask = asks_sorted[0][0]
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0: return None
        spread = (best_ask - best_bid) / mid
        cutoff_micro = now - RECENT_TRADE_WINDOW; cutoff_accum = now - ACCUM_BURST_WINDOW
        micro_burst = 0.0; accum_burst = 0.0
        recent_prices: List[float] = []
        recent_trades: List[Tuple[float, float]] = []
        for t in reversed(self.trades[symbol]):
            ts = t["ts"]
            if ts < cutoff_accum: break
            side_mult = 1.0 if t["side"] == "buy" else -1.0
            sz = t["size"]
            accum_burst += side_mult * sz
            if ts >= cutoff_micro: micro_burst += side_mult * sz
            price = t["price"]
            recent_prices.append(price)
            recent_trades.append((ts, side_mult * sz))
        if not recent_prices: return None
        if len(recent_prices) >= 2:
            high = max(recent_prices); low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0
        else:
            rng = 0.0
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
            "burst": accum_burst,
            "burst_micro": micro_burst,
            "range_pct": rng,
            "burst_slope": burst_slope,
            "trade_count": trade_count,
            "trade_velocity": trade_velocity,
        }

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    if spread <= 0: spread = 1e-6
    return abs(imbalance) * abs(burst) / spread

class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0
        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_5m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_15m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.last_skip_log = LAST_SKIP_LOG
        self.last_btc_guard_ts: float = 0.0
        self.guard_active: bool = False
        self.guard_start_ts: float = 0.0
        self.pending_entry: Optional[dict] = None
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False
        self.last_watchdog_ts: float = 0.0
        self.last_liq_state: Dict[str, Optional[dict]] = {s: None for s in SYMBOLS_WS}

    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        buf1 = self.price_1m[sym]; buf5 = self.price_5m[sym]; buf15 = self.price_15m[sym]
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
                "near_support": pos_in_range <= 0.2, "near_resistance": pos_in_range >= 0.8,
                "open": prices[0], "close": prices[-1], "prices": prices}

    def _get_5m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_5m[sym]
        if len(buf) < 10: return None
        prices = [p for _, p in buf]; first = prices[0]; last = prices[-1]
        if first <= 0: return None
        change = (last - first) / first
        if change > 0.002: trend = "up"
        elif change < -0.002: trend = "down"
        else: trend = "flat"
        return {"trend": trend, "change": change}

    def _get_15m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_15m[sym]
        if len(buf) < 10: return None
        prices = [p for _, p in buf]; first = prices[0]; last = prices[-1]
        if first <= 0: return None
        change = (last - first) / first
        if change > 0.004: trend = "up"
        elif change < -0.004: trend = "down"
        else: trend = "flat"
        return {"trend": trend, "change": change}

    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        now = time.time(); key = f"{sym}:{reason}"; last = self.last_skip_log.get(key, 0.0)
        if now - last < SKIP_LOG_COOLDOWN: return
        self.last_skip_log[key] = now
        mid = feat.get("mid", 0.0); spread = feat.get("spread", 0.0); imb = feat.get("imbalance", 0.0)
        burst = feat.get("burst", 0.0); b_micro = feat.get("burst_micro", 0.0); rng = feat.get("range_pct", 0.0)
        slope = feat.get("burst_slope", 0.0); tcount = feat.get("trade_count", 0); tvel = feat.get("trade_velocity", 0.0)
        msg = (f"[SKIP] {sym} {reason} {extra} | mid={mid:.4f} spread={spread:.6f} "
               f"imb={imb:.4f} burst={burst:.4f} micro={b_micro:.4f} slope={slope:.4f} trades={tcount} vel={tvel:.2f} rng={rng:.6f}")
        print(msg, flush=True)

    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity(); self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT. Kill at {KILL_SWITCH_DD*100:.1f}% DD.")
        for s in SYMBOLS_WS: await self.exchange.set_leverage(s, LEVERAGE)
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

    # ----- FAST/SLOW DETECTOR -----
    def is_fast_market(self, sym: str, feat: dict) -> bool:
        """
        Determine if market is FAST for the symbol. Uses stricter Option-B thresholds.
        Returns True if market is considered fast/unfavorable for reversal entries.
        """
        # 1) spread
        spread = feat.get("spread", 0.0)
        if spread >= FAST_SPREAD:
            return True
        # 2) 1m range (we keep recent 1m buffer)
        ctx = self._get_1m_context(sym)
        if ctx:
            high = ctx["high"]; low = ctx["low"]
            mid = (high + low) / 2.0 if (high + low) > 0 else 0.0
            if mid > 0:
                one_min_range_pct = (high - low) / mid
                if one_min_range_pct >= FAST_RANGE_PCT:
                    return True
        # 3) trade velocity
        tvel = feat.get("trade_velocity", 0.0)
        if tvel >= FAST_TRADE_VEL:
            return True
        # 4) burst slope strong
        if abs(feat.get("burst_slope", 0.0)) >= FAST_BURST_SLOPE:
            return True
        # 5) BTC one-shot guard (global)
        btc_ctx = self.price_1m.get("BTCUSDT")
        if btc_ctx and len(btc_ctx) >= 2:
            p_first = btc_ctx[0][1]; p_last = btc_ctx[-1][1]
            if p_first > 0 and abs(p_last - p_first) / p_first >= BTC_VOL_GUARD_PCT:
                return True
        return False

    async def eval_symbols_and_maybe_enter(self):
        if self.position is not None: return
        now = time.time()
        # If pending entry - check confirmation
        if self.pending_entry:
            pe = self.pending_entry
            if now - pe["ts"] >= CONFIRMATION_DELAY:
                sym = pe["symbol"]; side = pe["side"]
                feat_new = self.mkt.compute_features(sym)
                if not feat_new:
                    self.pending_entry = None
                else:
                    if self.is_fast_market(sym, feat_new):
                        self._log_skip(sym, "fast_on_confirm", feat_new, "market turned fast during confirm")
                        await send_telegram(f"❌ Confirm dropped {sym} - market fast")
                        self.pending_entry = None
                    else:
                        ok = self._check_option_b_conditions(sym, side, feat_new)
                        if ok:
                            await self._open_entry_from_pending(sym, feat_new, side)
                            self.pending_entry = None
                            return
                        else:
                            self._log_skip(sym, "confirm_failed", feat_new, "confirm filters failed")
                            await send_telegram(f"❌ PENDING DROPPED {sym} {side.upper()}")
                            self.pending_entry = None
            else:
                await self.maybe_heartbeat()
                return

        best_sym = None; best_feat = None; best_side = None
        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat: continue
            mid = feat["mid"]
            self._update_price_buffers(sym, mid, now)
            ctx_1m = self._get_1m_context(sym)
            if not ctx_1m: continue
            prices_1m = ctx_1m["prices"]
            if len(prices_1m) < max(BB_PERIOD, RSI_PERIOD) + 2: continue
            bb = bollinger_from_prices(prices_1m, BB_PERIOD, BB_STD)
            if not bb: continue
            upper, midbb, lower = bb
            last_price = prices_1m[-1]
            touched_upper = last_price >= upper * 0.995
            touched_lower = last_price <= lower * 1.005
            if not (touched_upper or touched_lower):
                self._log_skip(sym, "bb_not_touch", feat, "")
                continue
            open_p = ctx_1m["open"]; close_p = ctx_1m["close"]
            high_p = ctx_1m["high"]; low_p = ctx_1m["low"]
            body = abs(close_p - open_p)
            if body <= 1e-9:
                self._log_skip(sym, "tiny_body", feat, "")
                continue
            upper_wick = max(0.0, high_p - max(open_p, close_p))
            lower_wick = max(0.0, min(open_p, close_p) - low_p)
            side = "sell" if touched_upper else "buy"
            wick_ok = False
            if side == "sell":
                if upper_wick / body >= WICK_MIN_RATIO: wick_ok = True
            else:
                if lower_wick / body >= WICK_MIN_RATIO: wick_ok = True
            if not wick_ok:
                self._log_skip(sym, "weak_wick", feat, f"up={upper_wick:.6f} low={lower_wick:.6f}")
                continue
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
            now_ts = time.time(); buys = 0.0; sells = 0.0
            for t in reversed(self.mkt.trades[sym]):
                if now_ts - t["ts"] > MICROFLOW_WINDOW: break
                if t["side"] == "buy": buys += t["size"]
                else: sells += t["size"]
            microflow = buys - sells
            # Strict microflow: require clear fade
            if side == "sell" and microflow >= -0.5 * abs(feat.get("burst_micro", 0.0)):
                self._log_skip(sym, "microflow_not_fade_short", feat, f"mf={microflow:.3f}")
                continue
            if side == "buy" and microflow <= 0.5 * abs(feat.get("burst_micro", 0.0)):
                self._log_skip(sym, "microflow_not_fade_long", feat, f"mf={microflow:.3f}")
                continue
            one_min_range_pct = (high_p - low_p) / ((high_p + low_p) / 2.0) if (high_p + low_p) > 0 else 0.0
            if one_min_range_pct > VOLATILITY_THRESHOLD:
                self._log_skip(sym, "vol_spike", feat, f"rng={one_min_range_pct:.4f}")
                continue
            # Fast market detector
            if self.is_fast_market(sym, feat):
                self._log_skip(sym, "fast_market", feat, "detected fast market - skip")
                continue
            score = abs(feat.get("imbalance", 0.0)) + abs(microflow)
            if score > 0 and score > (best_feat["score"] if best_feat else 0):
                best_sym = sym; best_feat = {"feat": feat, "score": score, "last_price": last_price}; best_side = side

        if not best_sym:
            await self.maybe_heartbeat(); return
        if now - self.mkt.last_signal_ts[best_sym] < 0.5: return
        self.mkt.last_signal_ts[best_sym] = now
        self.pending_entry = {"symbol": best_sym, "side": best_side, "ts": now}
        print(f"[SIGNAL] {best_sym} {best_side.upper()} — pending {CONFIRMATION_DELAY}s confirmation")
        await send_telegram(f"⏳ PENDING {best_sym} {best_side.upper()} — confirm {CONFIRMATION_DELAY}s")

    def _check_option_b_conditions(self, sym: str, side: str, feat: dict) -> bool:
        now = time.time(); mid = feat["mid"]
        self._update_price_buffers(sym, mid, now)
        ctx = self._get_1m_context(sym)
        if not ctx: return False
        prices = ctx["prices"]
        if len(prices) < max(BB_PERIOD, RSI_PERIOD) + 2: return False
        bb = bollinger_from_prices(prices, BB_PERIOD, BB_STD)
        if not bb: return False
        upper, midbb, lower = bb
        last_price = prices[-1]
        touched_upper = last_price >= upper * 0.995
        touched_lower = last_price <= lower * 1.005
        if side == "sell" and not touched_upper: return False
        if side == "buy" and not touched_lower: return False
        open_p = ctx["open"]; close_p = ctx["close"]; high_p = ctx["high"]; low_p = ctx["low"]
        body = abs(close_p - open_p)
        if body <= 1e-9: return False
        upper_wick = max(0.0, high_p - max(open_p, close_p))
        lower_wick = max(0.0, min(open_p, close_p) - low_p)
        if side == "sell" and upper_wick / body < WICK_MIN_RATIO: return False
        if side == "buy" and lower_wick / body < WICK_MIN_RATIO: return False
        r_now = rsi_from_prices(prices, RSI_PERIOD); r_prev = rsi_from_prices(prices[:-1], RSI_PERIOD)
        if r_now is None or r_prev is None: return False
        if side == "sell" and not (r_now < r_prev): return False
        if side == "buy" and not (r_now > r_prev): return False
        buys = sells = 0.0; now_ts = time.time()
        for t in reversed(self.mkt.trades[sym]):
            if now_ts - t["ts"] > MICROFLOW_WINDOW: break
            if t["side"] == "buy": buys += t["size"]
            else: sells += t["size"]
        microflow = buys - sells
        if side == "sell" and microflow >= -0.5 * abs(feat.get("burst_micro", 0.0)): return False
        if side == "buy" and microflow <= 0.5 * abs(feat.get("burst_micro", 0.0)): return False
        if self.is_fast_market(sym, feat): return False
        return True

    async def _open_entry_from_pending(self, sym: str, feat: dict, side: str):
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0: return
        mid = feat["mid"]
        notional = equity * EQUITY_USE_FRACTION * LEVERAGE
        if mid <= 0: return
        qty = notional / mid
        min_qty = MIN_QTY_MAP.get(sym, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym} qty {qty:.6f} < min {min_qty}", flush=True)
            await send_telegram(f"❌ ENTRY SKIPPED {sym} qty too small")
            return
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0: return
        try:
            order = await self.exchange.create_market_order(sym, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym}: {e}")
            await send_telegram(f"❌ Entry failed for {sym}")
            return
        if side == "buy":
            tp_price = entry_price * (1.0 + TP_PCT); sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - TP_PCT); sl_price = entry_price * (1.0 + SL_PCT)
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(sym, opp_side, qty, tp_price, reduce_only=True, post_only=True)
        except Exception as e:
            print(f"[TP ERROR] {sym}: {e}")
            await send_telegram(f"⚠️ TP order placement failed for {sym}")
        self.position = Position(symbol_ws=sym, side=side, qty=qty, entry_price=entry_price, tp_price=tp_price, sl_price=sl_price, opened_ts=time.time())
        self.last_trade_time = time.time()
        print(f"[ENTRY] {sym} {side.upper()} qty={qty} entry={entry_price:.4f} TP={tp_price:.4f} SL={sl_price:.4f}")
        await send_telegram(f"📌 ENTRY {sym} {side.upper()} qty={qty} entry={entry_price:.4f} TP={tp_price:.4f} SL={sl_price:.4f}")

    async def watchdog_position(self):
        if not self.position: return
        pos = self.position; sym = pos.symbol_ws
        size = await self.exchange.get_position_size(sym)
        if size <= 0:
            feat = self.mkt.compute_features(sym); reason = "unknown"; was_loss = True
            if feat:
                mid = feat["mid"]
                if pos.side == "buy": move = (mid - pos.entry_price) / pos.entry_price
                else: move = (pos.entry_price - mid) / pos.entry_price
                if move > 0: reason = "TP filled (profit)"; was_loss = False
                else: reason = "SL filled (loss)"; was_loss = True
            print(f"[EXIT DETECTED] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} tp={pos.tp_price:.4f} sl={pos.sl_price:.4f} reason={reason}")
            await send_telegram(f"📤 EXIT — {sym} {pos.side.upper()} entry={pos.entry_price:.4f}\n➡️ {reason}")
            self.last_exit_ts = time.time(); self.last_exit_was_loss = was_loss; self.position = None; return
        feat = self.mkt.compute_features(sym)
        if not feat: return
        mid = feat["mid"]
        if mid <= 0: return
        hit = False
        if pos.side == "buy" and mid <= pos.sl_price: hit = True
        if pos.side == "sell" and mid >= pos.sl_price: hit = True
        if hit:
            await self.exchange.close_position_market(sym)
            print(f"[BACKUP SL] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} SL={pos.sl_price:.4f} now={mid:.4f}")
            await send_telegram(f"🛑 BACKUP SL — {sym} {pos.side.upper()}\nEntry={pos.entry_price:.4f}  SL={pos.sl_price:.4f}")
            self.last_exit_ts = time.time(); self.last_exit_was_loss = True; self.position = None; return

    async def maybe_heartbeat(self):
        now = time.time()
        if now - self.last_trade_time < HEARTBEAT_IDLE_SEC: return
        if now - self.last_heartbeat_ts < HEARTBEAT_IDLE_SEC: return
        self.last_heartbeat_ts = now
        eq = await self.exchange.fetch_equity()
        print(f"[HEARTBEAT] idle, equity={eq:.2f}")
        await send_telegram(f"💤 Bot idle {HEARTBEAT_IDLE_SEC/60:.0f} min. Equity={eq:.2f} USDT")

async def ws_loop(mkt: MarketState):
    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}"); topics.append(f"publicTrade.{s}")
    print("⚡ WS loop started... connecting...")
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, receive_timeout=40, heartbeat=20) as ws:
                    print("📡 Connected to WS server, subscribing...")
                    await ws.send_json({"op": "subscribe", "args": topics})
                    for sym in SYMBOLS_WS: ws_ready[sym] = True
                    await send_telegram("📡 WS Connected: All symbols")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue
                            if data.get("success") is True: continue
                            topic = data.get("topic")
                            if not topic: continue
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload: continue
                                if isinstance(payload, list): payload = payload[0]
                                if isinstance(payload, dict): mkt.update_book(sym, payload)
                            elif topic.startswith("publicTrade"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload: continue
                                trades = payload if isinstance(payload, list) else [payload]
                                now_ts = time.time()
                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(t.get("v") or t.get("q") or t.get("size"))
                                    side = (t.get("S") or t.get("side") or "Buy").lower()
                                    if price is None or qty is None: continue
                                    mkt.add_trade(sym, {"price": price, "size": qty, "side": "buy" if side == "buy" else "sell", "ts": now_ts})
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting")
                            break
        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}")
            for s in SYMBOLS_WS: ws_ready[s] = False
            await asyncio.sleep(1)

def debug_console(mkt: MarketState, bot: ScalperBot):
    help_text = ("[DEBUG] Commands:\n  ws      - show websocket ready flags\n  book    - show last orderbook timestamps\n  trades  - show count of recent trades per symbol\n  pos     - show current open position\n  help    - show this message\n")
    print(help_text, flush=True)
    while True:
        try:
            cmd = input("")
        except EOFError:
            return
        cmd = cmd.strip().lower()
        if cmd == "ws": print("[DEBUG] ws_ready =", ws_ready, flush=True)
        elif cmd == "book": ts_map = {s: mkt.books[s]["ts"] for s in SYMBOLS_WS}; print("[DEBUG] book ts =", ts_map, flush=True)
        elif cmd == "trades": counts = {s: len(mkt.trades[s]) for s in SYMBOLS_WS}; print("[DEBUG] trades len =", counts, flush=True)
        elif cmd == "pos": print("[DEBUG] position =", bot.position, flush=True)
        elif cmd == "help": print(help_text, flush=True)
        elif cmd == "": continue
        else: print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)

async def main():
    print(f"Starting {BOT_VERSION} ...")
    exchange = ExchangeClient(); mkt = MarketState(); bot = ScalperBot(exchange, mkt)
    threading.Thread(target=debug_console, args=(mkt, bot), daemon=True).start()
    await bot.init_equity_and_leverage()
    ws_task = asyncio.create_task(ws_loop(mkt))
    try:
        while True:
            try: await bot.maybe_kill_switch()
            except SystemExit: raise
            except Exception as e: print("[ERR] maybe_kill_switch", e)
            try: await bot.eval_symbols_and_maybe_enter()
            except Exception as e: print("[ERR] eval loop", e)
            try: await bot.watchdog_position()
            except Exception as e: print("[ERR] watchdog", e)
            await asyncio.sleep(0.12)
    finally:
        ws_task.cancel()
        try: await ws_task
        except Exception: pass

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except Exception:
        pass
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
