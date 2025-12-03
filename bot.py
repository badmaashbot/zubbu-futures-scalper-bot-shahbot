#!/usr/bin/env python3
"""
Bybit USDT Perp Micro-Scalper (Aggressive Impulse V2 + Private WS)

- PUBLIC WS: orderbook.1 + publicTrade for 5 symbols (BTC, ETH, BNB, SOL, DOGE)
- PRIVATE WS: order, execution, position, wallet (for strict TP/SL & position sync)
- Dynamic TP ladder: 0.4% → 0.8% → 1.0% → trailing ~0.7%
- SL starts ~0.25%, tightens as TP levels hit
- Only 1 open position at a time (best symbol chosen)
- Kill-switch on equity drawdown
- Debug console in tmux: commands ws, book, trades, pos, help
"""

import os
import sys
import time
import math
import hmac
import json
import asyncio
import logging
import traceback
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Deque, List, Optional, Tuple

import aiohttp
import ccxt  # sync, wrapped with asyncio.to_thread

# --------------------------------------------------------------------
# Logging & global exception hook
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
sys.excepthook = lambda t, v, tb: traceback.print_exception(t, v, tb)

# --------------------------------------------------------------------
# ENVIRONMENT
# --------------------------------------------------------------------
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

# --------------------------------------------------------------------
# SYMBOLS / URLS
# --------------------------------------------------------------------
SYMBOLS_WS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]

SYMBOL_MAP = {
    "BTCUSDT": "BTC/USDT:USDT",
    "ETHUSDT": "ETH/USDT:USDT",
    "BNBUSDT": "BNB/USDT:USDT",
    "SOLUSDT": "SOL/USDT:USDT",
    "DOGEUSDT": "DOGE/USDT:USDT",
}

if TESTNET:
    PUBLIC_WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"
    PRIVATE_WS_URL = "wss://stream-testnet.bybit.com/v5/private"
else:
    PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear"
    PRIVATE_WS_URL = "wss://stream.bybit.com/v5/private"

# WS readiness map
ws_ready: Dict[str, bool] = {s: False for s in SYMBOLS_WS}

# --------------------------------------------------------------------
# TRADING CONFIG
# --------------------------------------------------------------------
LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95   # use up to 95% of (equity * leverage)

# Dynamic TP ladder
TP0_PCT = 0.004     # +0.4%
SL0_PCT = 0.0025    # -0.25%

TP1_PCT = 0.008     # +0.8%
LOCK1_PCT = 0.0005  # +0.05% locked when TP0 reached

TP2_PCT = 0.010     # +1.0%
LOCK2_PCT = 0.004   # +0.4% locked when TP1 reached

TRAIL_START_PCT = 0.010   # start trailing once +1% reached
TRAIL_LOCK_PCT = 0.007    # keep SL around +0.7% while price higher

# Aggressive impulse filters (NOT ultra-strict)
MAX_SPREAD = 0.0007        # 0.07%
IMBALANCE_THRESH = 0.04    # ~52/48+
BURST_THRESH = 0.02        # small but real aggression
SCORE_MIN = 2.0
VOL_FILTER = 0.0002        # 0.02% micro-range

RECENT_TRADE_WINDOW = 0.15  # seconds, for burst
BOOK_STALE_SEC = 6.0

KILL_SWITCH_DD = 0.05       # 5% equity drawdown
HEARTBEAT_IDLE_SEC = 1800   # 30 minutes

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# --------------------------------------------------------------------
# TELEGRAM
# --------------------------------------------------------------------
_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0  # at most 1 msg every 30s


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
        pass  # never break bot from TG issues

# --------------------------------------------------------------------
# UTILS
# --------------------------------------------------------------------
def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def log_skip(sym: str, reason: str, feat: dict):
    print(
        f"[SKIP] {sym}: {reason} | "
        f"spread={feat.get('spread', 0):.6f}, "
        f"imb={feat.get('imbalance', 0):.4f}, "
        f"burst={feat.get('burst', 0):.4f}, "
        f"range={feat.get('range_pct', 0):.6f}",
        flush=True,
    )


# --------------------------------------------------------------------
# DATA CLASSES
# --------------------------------------------------------------------
@dataclass
class Position:
    symbol_ws: str
    side: str            # "buy" or "sell"
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    step: int = 0        # 0,1,2,3...
    opened_ts: float = field(default_factory=time.time)


@dataclass
class PrivateState:
    """
    Holds private account data streamed from private WS.
    We mainly use it for debug & safety checks (order/position sync).
    """
    positions: Dict[str, dict] = field(default_factory=dict)
    wallet: Dict[str, dict] = field(default_factory=dict)
    orders: Dict[str, dict] = field(default_factory=dict)
    last_exec_ts: float = 0.0


class MoveAnalyzer:
    """
    Tiny helper to log realized move sizes (for future tuning).
    """
    def __init__(self, max_len=500):
        self.moves: Deque[float] = deque(maxlen=max_len)

    def add_move(self, pct: float):
        self.moves.append(pct)

    def avg_move(self) -> float:
        if not self.moves:
            return 0.006
        return sum(self.moves) / len(self.moves)


move_analyzer = MoveAnalyzer()

# --------------------------------------------------------------------
# EXCHANGE CLIENT (ccxt sync wrapped into async)
# --------------------------------------------------------------------
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


# --------------------------------------------------------------------
# MARKET STATE (PUBLIC WS)
# --------------------------------------------------------------------
class MarketState:
    def __init__(self):
        self.books: Dict[str, dict] = {
            s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS
        }
        self.trades: Dict[str, Deque[dict]] = {
            s: deque(maxlen=1000) for s in SYMBOLS_WS
        }
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

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

        # incremental
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

        # trades window
        cutoff = now - RECENT_TRADE_WINDOW
        burst = 0.0
        recent_prices: List[float] = []
        for t in reversed(self.trades[symbol]):
            if t["ts"] < cutoff:
                break
            sz = t["size"]
            if t["side"] == "buy":
                burst += sz
            else:
                burst -= sz
            recent_prices.append(t["price"])

        if not recent_prices:
            return None

        high = max(recent_prices)
        low = min(recent_prices)
        rng = (high - low) / mid if mid > 0 else 0.0

        return {
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "burst": burst,
            "range_pct": rng,
        }

# --------------------------------------------------------------------
# SCALPER BOT CORE
# --------------------------------------------------------------------
class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState, priv: PrivateState):
        self.exchange = exchange
        self.mkt = mkt
        self.priv = priv

        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT", flush=True)
        await send_telegram(
            f"🟢 Bot started. Equity: {eq:.2f} USDT. Kill at {KILL_SWITCH_DD*100:.1f}% DD."
        )
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.", flush=True)

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

    async def eval_symbols_and_maybe_enter(self):
        if self.position is not None:
            return

        best_sym: Optional[str] = None
        best_feat: Optional[dict] = None
        best_score: float = 0.0
        now = time.time()

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            mid = feat["mid"]
            spread = feat["spread"]
            imb = feat["imbalance"]
            burst = feat["burst"]
            rng = feat["range_pct"]

            # basic checks
            if spread <= 0 or spread > MAX_SPREAD:
                log_skip(sym, f"spread {spread:.6f} > MAX_SPREAD {MAX_SPREAD}", feat)
                continue

            if abs(imb) < IMBALANCE_THRESH:
                log_skip(sym, f"imb {imb:.4f} < IMBALANCE_THRESH {IMBALANCE_THRESH}", feat)
                continue

            if abs(burst) < BURST_THRESH:
                log_skip(sym, f"burst {burst:.4f} < BURST_THRESH {BURST_THRESH}", feat)
                continue

            if rng < VOL_FILTER:
                log_skip(sym, f"range {rng:.6f} < VOL_FILTER {VOL_FILTER}", feat)
                continue

            # direction consistency
            if (imb > 0 and burst > 0):
                dir_ok = True
            elif (imb < 0 and burst < 0):
                dir_ok = True
            else:
                dir_ok = False

            if not dir_ok:
                log_skip(sym, "direction mismatch (imb*burst <= 0)", feat)
                continue

            # simple impulse score
            score = (abs(imb) + abs(burst)) / max(spread, 1e-6)
            if score < SCORE_MIN:
                log_skip(sym, f"score {score:.3f} < SCORE_MIN {SCORE_MIN}", feat)
                continue

            if score > best_score:
                best_score = score
                best_sym = sym
                best_feat = feat

        if not best_sym or not best_feat:
            await self.maybe_heartbeat()
            return

        # anti-spam
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            return
        self.mkt.last_signal_ts[best_sym] = now

        await self.open_position(best_sym, best_feat)

    async def open_position(self, sym_ws: str, feat: dict):
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0:
            return

        mid = feat["mid"]
        imb = feat["imbalance"]
        burst = feat["burst"]

        # direction
        if imb > 0 and burst > 0:
            side = "buy"
        elif imb < 0 and burst < 0:
            side = "sell"
        else:
            log_skip(sym_ws, "no clear direction on open()", feat)
            return

        notional = equity * EQUITY_USE_FRACTION * LEVERAGE
        if mid <= 0:
            return
        qty = notional / mid

        min_qty = MIN_QTY_MAP.get(sym_ws, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym_ws}: qty {qty} < min_qty {min_qty}", flush=True)
            return

        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        # compute initial TP/SL
        if side == "buy":
            tp_price = mid * (1.0 + TP0_PCT)
            sl_price = mid * (1.0 - SL0_PCT)
        else:
            tp_price = mid * (1.0 - TP0_PCT)
            sl_price = mid * (1.0 + SL0_PCT)

        try:
            order = await self.exchange.create_market_order(
                sym_ws, side, qty, reduce_only=False
            )
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # place TP limit (reduce-only maker)
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
            print(f"[TP ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"⚠️ TP order placement failed for {sym_ws}")

        self.position = Position(
            symbol_ws=sym_ws,
            side=side,
            qty=qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            step=0,
        )
        self.last_trade_time = time.time()
        print(
            f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP0={tp_price:.4f} SL0≈{sl_price:.4f}",
            flush=True,
        )
        await send_telegram(
            f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP0={tp_price:.4f} SL0≈{sl_price:.4f}"
        )

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

        # current PnL in %
        if pos.side == "buy":
            pnl_pct = (mid - pos.entry_price) / pos.entry_price
            dd_pct = (pos.entry_price - mid) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - mid) / pos.entry_price
            dd_pct = (mid - pos.entry_price) / pos.entry_price

        # HARD SL: if price beyond SL line by tiny buffer, close
        sl_line = pos.sl_price
        if pos.side == "buy" and mid <= sl_line:
            await self._do_stop_out(mid, dd_pct)
            return
        if pos.side == "sell" and mid >= sl_line:
            await self._do_stop_out(mid, dd_pct)
            return

        # TP / dynamic ladder logic (works OFF price, not WS order execution)
        # Step 0 -> 1 (TP0 hit)
        if pos.step == 0 and pnl_pct >= TP0_PCT:
            if pos.side == "buy":
                pos.sl_price = pos.entry_price * (1.0 + LOCK1_PCT)
                pos.tp_price = pos.entry_price * (1.0 + TP1_PCT)
            else:
                pos.sl_price = pos.entry_price * (1.0 - LOCK1_PCT)
                pos.tp_price = pos.entry_price * (1.0 - TP1_PCT)
            pos.step = 1
            print(
                f"[TP STEP 1] {sym} pnl={pnl_pct*100:.2f}% "
                f"new SL={pos.sl_price:.4f} TP={pos.tp_price:.4f}",
                flush=True,
            )
            await send_telegram(
                f"🔁 TP STEP1 {sym}: pnl={pnl_pct*100:.2f}%, "
                f"SL lock≈{LOCK1_PCT*100:.2f}%, TP→{TP1_PCT*100:.2f}%"
            )

        # Step 1 -> 2 (TP1 hit)
        elif pos.step == 1 and pnl_pct >= TP1_PCT:
            if pos.side == "buy":
                pos.sl_price = pos.entry_price * (1.0 + LOCK2_PCT)
                pos.tp_price = pos.entry_price * (1.0 + TP2_PCT)
            else:
                pos.sl_price = pos.entry_price * (1.0 - LOCK2_PCT)
                pos.tp_price = pos.entry_price * (1.0 - TP2_PCT)
            pos.step = 2
            print(
                f"[TP STEP 2] {sym} pnl={pnl_pct*100:.2f}% "
                f"new SL={pos.sl_price:.4f} TP={pos.tp_price:.4f}",
                flush=True,
            )
            await send_telegram(
                f"🔁 TP STEP2 {sym}: pnl={pnl_pct*100:.2f}%, "
                f"SL lock≈{LOCK2_PCT*100:.2f}%, TP→{TP2_PCT*100:.2f}%"
            )

        # Step >=2 and pnl >= 1% → trailing around 0.7%
        elif pos.step >= 2 and pnl_pct >= TRAIL_START_PCT:
            if pos.side == "buy":
                trail_sl = pos.entry_price * (1.0 + TRAIL_LOCK_PCT)
                if trail_sl > pos.sl_price:
                    pos.sl_price = trail_sl
            else:
                trail_sl = pos.entry_price * (1.0 - TRAIL_LOCK_PCT)
                if trail_sl < pos.sl_price:
                    pos.sl_price = trail_sl
            pos.step = 3

        # Optional: if price crosses our theoretical TP line, we can close aggressively
        # to avoid limit TP being missed in real orderbook.
        if pos.side == "buy" and mid >= pos.tp_price:
            await self._do_take_profit(mid, pnl_pct)
        elif pos.side == "sell" and mid <= pos.tp_price:
            await self._do_take_profit(mid, pnl_pct)

    async def _do_stop_out(self, mid: float, dd_pct: float):
        """Market close position as SL."""
        if not self.position:
            return
        sym = self.position.symbol_ws
        side = self.position.side
        await self.exchange.close_position_market(sym)

        move_analyzer.add_move(abs(dd_pct))
        print(
            f"[SL] {sym} {side.upper()} entry={self.position.entry_price:.4f} "
            f"now={mid:.4f} DD={dd_pct*100:.2f}%",
            flush=True,
        )
        await send_telegram(
            f"🛑 SL {sym} {side.upper()} entry={self.position.entry_price:.4f} "
            f"now={mid:.4f} DD={dd_pct*100:.2f}%"
        )
        self.position = None

    async def _do_take_profit(self, mid: float, pnl_pct: float):
        """Market close position as TP fallback."""
        if not self.position:
            return
        sym = self.position.symbol_ws
        side = self.position.side
        await self.exchange.close_position_market(sym)

        move_analyzer.add_move(abs(pnl_pct))
        print(
            f"[TP] {sym} {side.upper()} entry={self.position.entry_price:.4f} "
            f"now={mid:.4f} PNL={pnl_pct*100:.2f}%",
            flush=True,
        )
        await send_telegram(
            f"✅ TP {sym} {side.upper()} entry={self.position.entry_price:.4f} "
            f"now={mid:.4f} PNL={pnl_pct*100:.2f}%"
        )
        self.position = None

    async def maybe_heartbeat(self):
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

# --------------------------------------------------------------------
# PUBLIC WS LOOP
# --------------------------------------------------------------------
async def public_ws_loop(mkt: MarketState):
    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")

    print("⚡ PUBLIC WS loop starting...")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    PUBLIC_WS_URL,
                    receive_timeout=40,
                    heartbeat=20,
                ) as ws:
                    print("📡 Connected to PUBLIC WS, subscribing...")
                    await ws.send_json({"op": "subscribe", "args": topics})

                    for sym in SYMBOLS_WS:
                        ws_ready[sym] = True

                    await send_telegram("📡 PUBLIC WS Connected.")

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
                                now = time.time()
                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(
                                        t.get("v") or t.get("q") or t.get("size")
                                    )
                                    side = (t.get("S") or t.get("side") or "Buy").lower()
                                    if price is None or qty is None:
                                        continue
                                    mkt.add_trade(
                                        sym,
                                        {
                                            "price": price,
                                            "size": qty,
                                            "side": "buy" if side == "buy" else "sell",
                                            "ts": now,
                                        },
                                    )

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ PUBLIC WS ERROR — reconnecting")
                            break
        except Exception as e:
            print(f"❌ PUBLIC WS crashed: {e}", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)


# --------------------------------------------------------------------
# PRIVATE WS LOOP
# --------------------------------------------------------------------
async def private_ws_loop(priv: PrivateState):
    """
    Connects to private WS, authenticates, subscribes to:
      - order
      - execution
      - position
      - wallet
    We mainly log + keep last known state for safety / debugging.
    """
    print("🔒 PRIVATE WS loop starting...")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    PRIVATE_WS_URL,
                    receive_timeout=40,
                    heartbeat=20,
                ) as ws:
                    # auth
                    expires = int((time.time() + 60) * 1000)
                    sign_payload = f"GET/realtime{expires}"
                    signature = hmac.new(
                        API_SECRET.encode(),
                        sign_payload.encode(),
                        digestmod="sha256",
                    ).hexdigest()

                    await ws.send_json(
                        {
                            "op": "auth",
                            "args": [API_KEY, expires, signature],
                        }
                    )

                    # wait for auth response
                    auth_ok = False
                    try:
                        msg = await ws.receive(timeout=5)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("op") == "auth" and data.get("success", True):
                                auth_ok = True
                    except Exception:
                        pass

                    if not auth_ok:
                        print("❌ PRIVATE WS auth failed, retrying...", flush=True)
                        await asyncio.sleep(2)
                        continue

                    print("✅ PRIVATE WS authenticated, subscribing topics...", flush=True)
                    await send_telegram("🔒 PRIVATE WS connected (order/execution/position).")

                    await ws.send_json(
                        {
                            "op": "subscribe",
                            "args": ["order", "execution", "position", "wallet"],
                        }
                    )

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type == aiohttp.WSMsgType.ERROR:
                                print("⚠ PRIVATE WS ERROR — reconnecting", flush=True)
                                break
                            continue

                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue

                        topic = data.get("topic")
                        if not topic:
                            continue

                        if topic.startswith("position"):
                            for row in data.get("data", []):
                                sym = row.get("symbol")
                                if not sym:
                                    continue
                                priv.positions[sym] = row

                        elif topic.startswith("wallet"):
                            for row in data.get("data", []):
                                ccy = row.get("coin", "USDT")
                                priv.wallet[ccy] = row

                        elif topic.startswith("order"):
                            for row in data.get("data", []):
                                oid = row.get("orderId")
                                if oid:
                                    priv.orders[oid] = row

                        elif topic.startswith("execution"):
                            priv.last_exec_ts = time.time()
                            # For now, we just keep timestamp. Future: use for fill confirmation.
        except Exception as e:
            print(f"❌ PRIVATE WS crashed: {e}", flush=True)
            await asyncio.sleep(2)


# --------------------------------------------------------------------
# DEBUG CONSOLE (tmux)
# --------------------------------------------------------------------
def debug_console(mkt: MarketState, bot: ScalperBot, priv: PrivateState):
    help_text = (
        "[DEBUG] Commands:\n"
        "  ws      - show websocket ready flags\n"
        "  book    - show last orderbook timestamps\n"
        "  trades  - show # of recent trades per symbol\n"
        "  pos     - show current open position (bot-side)\n"
        "  ppos    - show last private WS positions\n"
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
            print("[DEBUG] bot.position =", bot.position, flush=True)
        elif cmd == "ppos":
            print("[DEBUG] priv.positions (last) =", priv.positions, flush=True)
        elif cmd == "help":
            print(help_text, flush=True)
        elif cmd == "":
            continue
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)


# --------------------------------------------------------------------
# MAIN LOOP
# --------------------------------------------------------------------
async def main():
    exchange = ExchangeClient()
    mkt = MarketState()
    priv = PrivateState()
    bot = ScalperBot(exchange, mkt, priv)

    threading.Thread(
        target=debug_console, args=(mkt, bot, priv), daemon=True
    ).start()

    await bot.init_equity_and_leverage()

    pub_task = asyncio.create_task(public_ws_loop(mkt))
    priv_task = asyncio.create_task(private_ws_loop(priv))

    try:
        while True:
            await bot.maybe_kill_switch()
            await bot.eval_symbols_and_maybe_enter()
            await bot.watchdog_position()
            await asyncio.sleep(1.0)
    except Exception:
        logging.exception(">>> MAIN LOOP CRASH <<<")
        raise
    finally:
        pub_task.cancel()
        priv_task.cancel()
        try:
            await pub_task
        except Exception:
            pass
        try:
            await priv_task
        except Exception:
            pass


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
