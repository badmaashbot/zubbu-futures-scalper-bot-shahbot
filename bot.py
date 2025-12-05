#!/usr/bin/env python3
"""
Bybit USDT Perp Micro-Scalper V5
- Single position
- Orderbook + trade burst signal engine
- Limit entries (no blind market entries)
- Dynamic TP + Dynamic SL watchdog
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
import ccxt  # sync ccxt, we will wrap blocking calls with asyncio.to_thread

BOT_VERSION = "ZUBBU_SCALPER_V5"

# ================= MOVE ANALYZER (for future TP tuning) =================

class MoveAnalyzer:
    def __init__(self, max_len: int = 500):
        self.moves = deque(maxlen=max_len)

    def add_move(self, pct: float) -> None:
        self.moves.append(pct)

    def avg_move(self) -> float:
        if not self.moves:
            return 0.006  # 0.6%
        return sum(self.moves) / len(self.moves)


move_analyzer = MoveAnalyzer()

# ================= ENV / BASIC CONFIG =================

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

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

LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95

# --- Risk ---
SL_PCT = 0.0035          # 0.35% hard stop distance
TP_MIN = 0.0040          # 0.40% minimum take profit
TP_MAX = 0.0120          # 1.20% max take profit
BE_TRIGGER = 0.0040      # move SL to breakeven after +0.40%
BE_BUFFER = 0.0002       # small buffer when moved to BE (0.02%)

# --- Orderflow filters (simple but strong) ---
SCORE_MIN        = 0.50       # impulse score threshold
IMBALANCE_THRESH = 0.015      # 1.5% imbalance
BURST_ACCUM_MIN  = 1.20       # sustained burst accumulation
BURST_MICRO_MIN  = 0.20       # micro burst strength
MAX_SPREAD       = 0.0015     # 0.15% max spread
MIN_RANGE_PCT    = 0.00008    # 0.008% minimum micro range

# --- Market data timing ---
RECENT_TRADE_WINDOW_MICRO = 0.40   # 0.40s micro burst
RECENT_TRADE_WINDOW_ACCUM = 3.00   # 3s accumulation window
BOOK_STALE_SEC            = 6.0    # ignore orderbook older than 6 seconds

KILL_SWITCH_DD     = 0.05
HEARTBEAT_IDLE_SEC = 1800

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# ================= TELEGRAM =================

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
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3.0)
        ) as session:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            )
    except Exception:
        pass


# ================= UTILS =================

def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# ================= EXCHANGE CLIENT (ccxt) =================

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

    async def create_limit_order(
        self,
        sym_ws: str,
        side: str,
        qty: float,
        price: float,
        reduce_only: bool = False,
        post_only: bool = False,
        tif: str = "IOC",
    ):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear", "timeInForce": tif}
        if reduce_only:
            params["reduceOnly"] = True
        if post_only:
            params["postOnly"] = True

        def _work():
            return self.client.create_order(symbol, "limit", side, qty, price, params)

        return await asyncio.to_thread(_work)

    async def create_market_order(
        self,
        sym_ws: str,
        side: str,
        qty: float,
        reduce_only: bool = False,
    ):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True

        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)

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
                    self.client.create_order(
                        symbol, "market", side, abs(contracts), None, params
                    )
                except Exception:
                    pass

        await asyncio.to_thread(_work)

# ================= DATA STRUCTURES =================

@dataclass
class Position:
    symbol_ws: str
    side: str          # "buy" or "sell"
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    opened_ts: float
    moved_to_be: bool = False  # did we move SL to breakeven


class MarketState:
    def __init__(self):
        self.books: Dict[str, dict] = {
            s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS
        }
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (Bybit v5 + legacy) --------
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]

        # New compact format with "b" / "a"
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000.0)
            book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0

            # If both sides present -> snapshot
            if "b" in data and "a" in data:
                book["bids"].clear()
                book["asks"].clear()
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None or q is None or q == 0:
                        continue
                    book["bids"][p] = q
                for px, qty in data.get("a", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None or q is None or q == 0:
                        continue
                    book["asks"][p] = q
                return

            # Delta on one side
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

        # Legacy fallback
        ts_raw = data.get("ts")
        book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0

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

        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p is None:
                    continue
                if q == 0:
                    book["bids"].pop(p, None)
                else:
                    book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p is None:
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

        # bursts from trades
        cutoff_micro = now - RECENT_TRADE_WINDOW_MICRO
        cutoff_accum = now - RECENT_TRADE_WINDOW_ACCUM

        burst_micro = 0.0
        burst_accum = 0.0
        recent_prices: List[float] = []

        for t in reversed(self.trades[symbol]):
            ts = t["ts"]
            if ts < cutoff_accum:
                break
            side = t["side"]
            sz = t["size"]
            signed = sz if side == "buy" else -sz

            if ts >= cutoff_micro:
                burst_micro += signed

            burst_accum += signed
            recent_prices.append(t["price"])

        if not recent_prices:
            rng = 0.0
        else:
            high = max(recent_prices)
            low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0

        return {
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "burst_micro": burst_micro,
            "burst_accum": burst_accum,
            "range_pct": rng,
        }

# ================= MOMENTUM / TP HELPERS =================

def compute_momentum_score(imbalance: float, burst_accum: float, spread: float) -> float:
    if spread <= 0:
        spread = 1e-6
    return abs(imbalance) * abs(burst_accum) / spread


def choose_dynamic_tp(imbalance: float, burst_accum: float, burst_micro: float, spread: float) -> float:
    score = compute_momentum_score(imbalance, burst_accum, spread)

    # base from score
    if score < 1.0:
        base = 0.004   # 0.4%
    elif score < 2.0:
        base = 0.006   # 0.6%
    elif score < 4.0:
        base = 0.008   # 0.8%
    else:
        base = 0.012   # 1.2%

    # slight boost if micro and accum both strong
    if abs(burst_micro) > BURST_MICRO_MIN * 2 and abs(burst_accum) > BURST_ACCUM_MIN * 1.2:
        base *= 1.1

    return max(TP_MIN, min(base, TP_MAX))

# ================= CORE BOT LOGIC =================

class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt

        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

        self.last_skip_log: Dict[str, float] = {}

    # ------- skip logger -------
    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < 3.0:
            return
        self.last_skip_log[key] = now

        mid = feat.get("mid", 0.0)
        spread = feat.get("spread", 0.0)
        imb = feat.get("imbalance", 0.0)
        b_micro = feat.get("burst_micro", 0.0)
        b_accum = feat.get("burst_accum", 0.0)
        rng = feat.get("range_pct", 0.0)

        msg = (
            f"[SKIP] {sym} {reason} {extra} | "
            f"mid={mid:.4f} spread={spread:.6f} "
            f"imb={imb:.4f} b_micro={b_micro:.4f} "
            f"b_accum={b_accum:.4f} rng={rng:.6f}"
        )
        print(msg, flush=True)

    # ------- lifecycle -------
    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(
            f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT. "
            f"Kill at {KILL_SWITCH_DD*100:.1f}% DD."
        )
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

    # ------- main decision loop -------
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
            b_micro = feat["burst_micro"]
            b_accum = feat["burst_accum"]
            spread = feat["spread"]
            rng = feat["range_pct"]
            mid = feat["mid"]

            # ---- basic filters ----
            if spread <= 0 or spread > MAX_SPREAD:
                self._log_skip(sym, "spread", feat, f"> MAX_SPREAD({MAX_SPREAD})")
                continue
            if rng < MIN_RANGE_PCT:
                self._log_skip(sym, "range", feat, f"< MIN_RANGE_PCT({MIN_RANGE_PCT})")
                continue
            if abs(imb) < IMBALANCE_THRESH:
                self._log_skip(sym, "imbalance", feat, f"< {IMBALANCE_THRESH}")
                continue
            if abs(b_accum) < BURST_ACCUM_MIN:
                self._log_skip(sym, "burst_accum", feat, f"< {BURST_ACCUM_MIN}")
                continue
            if abs(b_micro) < BURST_MICRO_MIN:
                self._log_skip(sym, "burst_micro", feat, f"< {BURST_MICRO_MIN}")
                continue

            # ---- direction agreement (orderflow) ----
            if imb > 0 and b_micro > 0 and b_accum > 0:
                side = "buy"
            elif imb < 0 and b_micro < 0 and b_accum < 0:
                side = "sell"
            else:
                self._log_skip(sym, "direction", feat, "imb, micro, accum disagree")
                continue

            # ---- impulse score ----
            score = compute_momentum_score(imb, b_accum, spread)
            if score < SCORE_MIN:
                self._log_skip(sym, "score", feat, f"{score:.3f} < {SCORE_MIN}")
                continue

            # choose best by score
            if score > best_score:
                best_score = score
                best_sym = sym
                best_feat = feat
                best_side = side

        if not best_sym or not best_feat or not best_side:
            await self.maybe_heartbeat()
            return

        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        await self.open_position(best_sym, best_feat, best_side)

    # ------- order execution (LIMIT ENTRY with IOC) -------
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

        # choose limit price at best bid/ask
        best = self.mkt.get_best_bid_ask(sym_ws)
        if not best:
            return
        best_bid, best_ask = best
        if side == "buy":
            limit_price = best_bid
        else:
            limit_price = best_ask

        # dynamic TP selection from orderflow
        tp_pct = choose_dynamic_tp(
            feat["imbalance"], feat["burst_accum"], feat["burst_micro"], feat["spread"]
        )

        # place LIMIT IOC entry (no chasing)
        try:
            order = await self.exchange.create_limit_order(
                sym_ws,
                side,
                qty,
                limit_price,
                reduce_only=False,
                post_only=False,
                tif="IOC",   # Immediate or cancel
            )
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}")
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # If IOC didn't fill, there will be no average/price
        entry_price = safe_float(order.get("average") or order.get("price"))
        if entry_price is None:
            print(f"[ENTRY CANCELLED] {sym_ws} IOC not filled at {limit_price}", flush=True)
            return

        # compute TP / SL from actual entry
        if side == "buy":
            tp_price = entry_price * (1.0 + tp_pct)
            sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - tp_pct)
            sl_price = entry_price * (1.0 + SL_PCT)

        # place TP as reduce-only limit
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(
                sym_ws,
                opp_side,
                qty,
                tp_price,
                reduce_only=True,
                post_only=True,
                tif="GTC",
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
            f"TP={tp_price:.4f} SL≈{sl_price:.4f}"
        )
        await send_telegram(
            f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL≈{sl_price:.4f}"
        )

    # ------- watchdog (hard SL + dynamic BE move) -------
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
            move = (mid - pos.entry_price) / pos.entry_price
            down = (pos.entry_price - mid) / pos.entry_price
        else:
            move = (pos.entry_price - mid) / pos.entry_price
            down = (mid - pos.entry_price) / pos.entry_price

        # move SL to breakeven once profit >= BE_TRIGGER
        if (not pos.moved_to_be) and move >= BE_TRIGGER:
            old_sl = pos.sl_price
            if pos.side == "buy":
                pos.sl_price = pos.entry_price * (1.0 - BE_BUFFER)
            else:
                pos.sl_price = pos.entry_price * (1.0 + BE_BUFFER)
            pos.moved_to_be = True
            print(
                f"[SL MOVE] {sym} to BE. old_SL={old_sl:.4f} new_SL={pos.sl_price:.4f} "
                f"move={move*100:.2f}%",
                flush=True,
            )
            await send_telegram(
                f"🔐 SL moved to BE for {sym} (side={pos.side.upper()}). "
                f"Unrealized move={move*100:.2f}%"
            )

        # hard stop: price crosses SL
        if pos.side == "buy" and mid <= pos.sl_price:
            await self.exchange.close_position_market(sym)
            print(
                f"[SL HIT] {sym} LONG entry={pos.entry_price:.4f} now={mid:.4f} SL={pos.sl_price:.4f}"
            )
            await send_telegram(
                f"🛑 SL HIT {sym} LONG entry={pos.entry_price:.4f} now={mid:.4f}"
            )
            move_analyzer.add_move(abs(down))
            self.position = None
            return

        if pos.side == "sell" and mid >= pos.sl_price:
            await self.exchange.close_position_market(sym)
            print(
                f"[SL HIT] {sym} SHORT entry={pos.entry_price:.4f} now={mid:.4f} SL={pos.sl_price:.4f}"
            )
            await send_telegram(
                f"🛑 SL HIT {sym} SHORT entry={pos.entry_price:.4f} now={mid:.4f}"
            )
            move_analyzer.add_move(abs(down))
            self.position = None
            return

    # ------- idle heartbeat -------
    async def maybe_heartbeat(self):
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

# ================= WEBSOCKET LOOP =================

async def ws_loop(mkt: MarketState):
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
                    heartbeat=20,
                ) as ws:
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
                                    mkt.add_trade(sym, {
                                        "price": price,
                                        "size": qty,
                                        "side": "buy" if side == "buy" else "sell",
                                        "ts": now_ts,
                                    })

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting")
                            break

        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}")
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)

# ================= DEBUG CONSOLE =================

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
            print("[DEBUG] position =", bot.position, flush=True)
        elif cmd == "help":
            print(help_text, flush=True)
        elif cmd == "":
            continue
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)

# ================= MAIN LOOP =================

async def main():
    print(f"Starting {BOT_VERSION} ...")
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

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
