#!/usr/bin/env python3
"""
Bybit USDT Perp Micro-Scalper (Aggressive 0.4% Burst Mode, Single Position)

- One WS connection, 5 symbols (BTC, ETH, BNB, SOL, DOGE)
- Orderbook + trade burst signal engine
- Pure momentum scalper: enters ONLY when there is a realistic 0.4% continuation chance
- Dynamic TP/SL handled client-side via watchdog:
    - Start: TP target 0.4%, SL 0.25%
    - After +0.4%: lock small profit, extend target to 0.8%
    - After +0.8%: lock more profit, extend target to 1.0%
    - After +1.0%: trail SL around +0.7% and let profit run
- Only 1 open position at a time (best symbol chosen)
- Built-in debug console (type commands in tmux: ws, book, trades, pos, help)
"""

import logging, traceback, sys
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(message)s")
sys.excepthook = lambda t, v, tb: traceback.print_exception(t, v, tb)

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

# --------- MOVE ANALYZER (only for stats / debugging) ---------

class MoveAnalyzer:
    def __init__(self, max_len=500):
        self.moves = deque(maxlen=max_len)

    def add_move(self, pct: float):
        self.moves.append(pct)

    def avg_move(self) -> float:
        if not self.moves:
            return 0.006  # default 0.6%
        return sum(self.moves) / len(self.moves)

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

SL_PCT = 0.0025   # initial hard SL distance (0.25%)

RECENT_TRADE_WINDOW = 0.15   # 150 ms window to measure trade burst
BOOK_STALE_SEC      = 6.0    # ignore orderbook older than 6 seconds

KILL_SWITCH_DD = 0.05        # kill bot if equity down 5%
HEARTBEAT_IDLE_SEC = 1800    # 30 min

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

def log_skip(sym: str, reason: str, feat: dict):
    print(
        f"[SKIP] {sym}: {reason} | "
        f"spread={feat.get('spread',0):.6f}, "
        f"imb={feat.get('imbalance',0):.4f}, "
        f"burst={feat.get('burst',0):.4f}, "
        f"range={feat.get('range_pct',0):.6f}",
        flush=True
    )

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
    step: int = 0      # 0 -> aiming for 0.4, 1 -> 0.8, 2 -> 1.0+, trailing after

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

    def update_book(self, symbol: str, data: dict):
        """
        Update local L2 book from WS message.
        Handles snapshot & delta safely.
        """
        book = self.books[symbol]
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

        # recent burst from trades (last 150 ms)
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

        # micro-range for info (not heavily used)
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
    """
    Simple 0.4%-chance momentum score.
    Higher score = stronger continuation force relative to cost (spread).
    """
    if spread <= 0:
        spread = 1e-6
    return (abs(imbalance) + abs(burst)) / spread

def choose_dynamic_tp(
    imbalance: float,
    burst: float,
    spread: float,
    range_pct: float,
) -> float:
    """
    For this aggressive bot we always *start* with 0.4% TP.
    Dynamic extension (0.8 / 1.0 / trailing) is handled in watchdog_position.
    """
    return 0.004   # 0.4 %

# --------------- CORE BOT LOGIC -----------------

class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

    # ======== INIT EQUITY & LEVERAGE ========
    async def init_equity_and_leverage(self):
        try:
            eq = await self.exchange.fetch_equity()
        except Exception:
            logging.exception(">>> FIRST EQUITY CALL REJECTED <<<")
            raise
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT", flush=True)
        await send_telegram(
            f"🟢 Bot started. Equity: {eq:.2f} USDT. Kill at {KILL_SWITCH_DD*100:.1f}% DD."
        )
        # set leverage for all symbols
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.", flush=True)
        await send_telegram("⚙️ Leverage set for all symbols.")

    # ======== KILL SWITCH ========
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

    # ======== ENTRY ENGINE (0.4% OPPORTUNITY) ========
    async def eval_symbols_and_maybe_enter(self):
        """
        Scan all symbols, compute features, pick the best one and open position.

        New logic:
        - Only requirement: clear, same-direction momentum & safe spread.
        - We treat any strong short-term push as a 0.4% continuation opportunity.
        """
        if self.position is not None:
            return

        best_sym: Optional[str] = None
        best_feat: Optional[dict] = None
        best_strength = 0.0
        now = time.time()

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            spread = feat["spread"]
            imb = feat["imbalance"]
            burst = feat["burst"]

            # must have usable spread
            if spread <= 0 or spread > 0.0007:  # 0.07% max
                continue

            # direction: both imbalance and burst in SAME direction
            if imb > 0 and burst > 0:
                dir_strength = abs(imb) + abs(burst)
                direction_ok = True
            elif imb < 0 and burst < 0:
                dir_strength = abs(imb) + abs(burst)
                direction_ok = True
            else:
                direction_ok = False
                dir_strength = 0.0

            if not direction_ok:
                continue

            # tiny floor to avoid micro-noise
            if dir_strength < 0.02:
                continue

            # choose symbol with strongest directional impulse
            if dir_strength > best_strength:
                best_strength = dir_strength
                best_sym = sym
                best_feat = feat

        if not best_sym or not best_feat:
            await self.maybe_heartbeat()
            return

        # anti-spam per symbol
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        await self.open_position(best_sym, best_feat)

    # ======== OPEN POSITION WITH STARTING 0.4% TP & 0.25% SL ========
    async def open_position(self, sym_ws: str, feat: dict):
        """
        Open a new position when a 0.4% continuation chance is detected.
        - Start with: TP target = +0.4%, SL distance = 0.25%.
        - All exits are handled client-side in watchdog_position().
        """
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0:
            return

        mid = feat["mid"]
        imb = feat["imbalance"]
        burst = feat["burst"]

        # direction from orderflow
        if imb > 0 and burst > 0:
            side = "buy"
        elif imb < 0 and burst < 0:
            side = "sell"
        else:
            log_skip(sym_ws, "no clear direction (imb*burst <= 0)", feat)
            return

        notional = equity * EQUITY_USE_FRACTION * LEVERAGE
        if mid <= 0:
            return
        qty = notional / mid

        min_qty = MIN_QTY_MAP.get(sym_ws, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym_ws}: qty {qty} < min_qty {min_qty}", flush=True)
            return

        # round qty down to 3 decimals
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        tp0_pct = 0.004    # +0.4%
        sl0_pct = 0.0025   # -0.25%

        if side == "buy":
            tp_price = mid * (1.0 + tp0_pct)
            sl_price = mid * (1.0 - sl0_pct)
        else:
            tp_price = mid * (1.0 - tp0_pct)
            sl_price = mid * (1.0 + sl0_pct)

        # ENTRY: market order
        try:
            order = await self.exchange.create_market_order(
                sym_ws, side, qty, reduce_only=False
            )
            entry_price = safe_float(
                order.get("average") or order.get("price"), mid
            ) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # No exchange TP order; handled client-side via dynamic logic
        self.position = Position(
            symbol_ws=sym_ws,
            side=side,
            qty=qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            opened_ts=time.time(),
            step=0,
        )
        self.last_trade_time = time.time()
        print(
            f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP0={tp_price:.4f} SL0={sl_price:.4f}",
            flush=True,
        )
        await send_telegram(
            f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP0={tp_price:.4f} SL0={sl_price:.4f}"
        )

    # ======== WATCHDOG: DYNAMIC TP/SL + HARD EXIT ========
    async def watchdog_position(self):
        """
        Dynamic TP/SL logic:

        - Start: TP0=0.4%, SL0=0.25%
        - When PnL >= 0.4%: step=1, SL->+0.05%, TP->0.8%
        - When PnL >= 0.8%: step=2, SL->+0.40%, TP->1.0%
        - When PnL >= 1.0%: trail SL around +0.7% (step>=2)
        """
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

        # Compute PnL%
        if pos.side == "buy":
            pnl_pct = (mid - pos.entry_price) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - mid) / pos.entry_price

        # STEP 0 -> STEP 1 (TP0 hit: 0.4%)
        if pos.step == 0 and pnl_pct >= 0.004:
            if pos.side == "buy":
                pos.sl_price = pos.entry_price * 1.0005   # lock +0.05%
                pos.tp_price = pos.entry_price * 1.008    # target +0.8%
            else:
                pos.sl_price = pos.entry_price * 0.9995
                pos.tp_price = pos.entry_price * 0.992
            pos.step = 1
            print(
                f"[DYNAMIC] {sym} step0→1 hit +0.4%: "
                f"SL={pos.sl_price:.4f}, TP={pos.tp_price:.4f}",
                flush=True,
            )
            await send_telegram(
                f"🎯 {sym} TP0 (+0.4%) hit, SL locked, TP→0.8%"
            )

        # STEP 1 -> STEP 2 (TP1 hit: 0.8%)
        if pos.step == 1 and pnl_pct >= 0.008:
            if pos.side == "buy":
                pos.sl_price = pos.entry_price * 1.004   # lock +0.4%
                pos.tp_price = pos.entry_price * 1.01    # aim +1.0%
            else:
                pos.sl_price = pos.entry_price * 0.996
                pos.tp_price = pos.entry_price * 0.99
            pos.step = 2
            print(
                f"[DYNAMIC] {sym} step1→2 hit +0.8%: "
                f"SL={pos.sl_price:.4f}, TP={pos.tp_price:.4f}",
                flush=True,
            )
            await send_telegram(
                f"🎯 {sym} TP1 (+0.8%) hit, SL locked more, TP→1.0%"
            )

        # STEP 2+: price beyond +1.0% → trail SL around +0.7%
        if pos.step >= 2 and pnl_pct >= 0.01:
            if pos.side == "buy":
                trail_sl = pos.entry_price * 1.007  # +0.7%
                if trail_sl > pos.sl_price:
                    pos.sl_price = trail_sl
                    print(f"[DYNAMIC] {sym} trailing LONG SL→{pos.sl_price:.4f}", flush=True)
            else:
                trail_sl = pos.entry_price * 0.993
                if trail_sl < pos.sl_price:
                    pos.sl_price = trail_sl
                    print(f"[DYNAMIC] {sym} trailing SHORT SL→{pos.sl_price:.4f}", flush=True)

        # HARD STOP-LOSS / TRAIL EXIT
        if pos.side == "buy":
            if mid <= pos.sl_price:
                await self.exchange.close_position_market(sym)
                move_analyzer.add_move(abs(pnl_pct))
                print(
                    f"[EXIT SL] {sym} BUY entry={pos.entry_price:.4f} now={mid:.4f} "
                    f"pnl={pnl_pct*100:.2f}%",
                    flush=True,
                )
                await send_telegram(
                    f"🛑 EXIT {sym} BUY at SL. PnL={pnl_pct*100:.2f}%"
                )
                self.position = None
                return
        else:
            if mid >= pos.sl_price:
                await self.exchange.close_position_market(sym)
                move_analyzer.add_move(abs(pnl_pct))
                print(
                    f"[EXIT SL] {sym} SELL entry={pos.entry_price:.4f} now={mid:.4f} "
                    f"pnl={pnl_pct*100:.2f}%",
                    flush=True,
                )
                await send_telegram(
                    f"🛑 EXIT {sym} SELL at SL. PnL={pnl_pct*100:.2f}%"
                )
                self.position = None
                return

    # ======== HEARTBEAT ========
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

                    for sym in SYMBOLS_WS:
                        ws_ready[sym] = True

                    await send_telegram("📡 WS Connected: All symbols")

                    async for msg in ws:

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except:
                                continue

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
                                now = time.time()

                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(t.get("v") or t.get("q") or t.get("size"))
                                    side  = (t.get("S") or t.get("side") or "Buy").lower()

                                    if price is None or qty is None:
                                        continue

                                    mkt.add_trade(sym, {
                                        "price": price,
                                        "size": qty,
                                        "side": "buy" if side == "buy" else "sell",
                                        "ts": now
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
      book    -> show last book timestamps
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
    except Exception:
        logging.exception(">>> FIRST REAL CRASH <<<")
        raise
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
