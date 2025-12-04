#!/usr/bin/env python3
"""
ZUBBU_SCALPER_V5_OPTION_B
Pure 4-filter orderflow scalper (Option B tuning)

- Filters: imbalance, burst, spread, score
- No 1m/5m structure or extra filters
- Dynamic TP (0.4% -> up to ~1.5%) and fixed SL watchdog
- Full Bybit v5 orderbook parser + legacy fallback
- Skip logs printed for every rejection (no rate limiting)
- Debug console in tmux: ws, book, trades, pos, help
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
import ccxt  # sync ccxt used inside asyncio.to_thread

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_SCALPER_V5.0-OptionB"

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

# --------------- WEBSOCKET URL -----------------
if TESTNET:
    WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"
else:
    WS_URL = "wss://stream.bybit.com/v5/public/linear"

# --------------- TRADING CONFIG -----------------

LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95  # use up to 95% equity * leverage as position notional

SL_PCT = 0.0035  # 0.35% SL (client-side watchdog)

# ------------------- Option B filters -------------------
IMBALANCE_THRESH = 0.020   # 2% imbalance
BURST_THRESH     = 0.030   # 3% burst
SCORE_MIN        = 0.0009  # tuned for Option B
MAX_SPREAD       = 0.0015  # 0.15% spread tolerance
# --------------------------------------------------------

# timing windows
RECENT_TRADE_WINDOW = 0.35   # 350 ms window for burst
BOOK_STALE_SEC = 6.0         # ignore books older than this

KILL_SWITCH_DD = 0.05        # 5% equity drawdown -> stop trading
HEARTBEAT_IDLE_SEC = 1800    # 30 minutes idle heartbeat

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# --------------- TELEGRAM (rate-limited) -----------------
_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0  # seconds


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
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            )
    except Exception:
        pass


# --------------- UTILS -----------------


def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# --------------- EXCHANGE CLIENT -----------------


class ExchangeClient:
    """
    Thin async wrapper around ccxt.bybit (sync HTTP).
    """

    def __init__(self):
        cfg = {"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True, "options": {"defaultType": "swap"}}
        if TESTNET:
            cfg["urls"] = {
                "api": {"public": "https://api-testnet.bybit.com", "private": "https://api-testnet.bybit.com"}
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
            try:
                bal = self.client.fetch_balance()
                total = safe_float(bal.get("USDT", {}).get("total"), 0.0) or 0.0
            except Exception:
                total = 0.0
            # add unrealizedPnL if available
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
    Holds L2 book + trades for symbols and provides compute_features()
    """

    def __init__(self):
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (Bybit V5 + legacy fallback) --------
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]

        # NEW compact snapshot/delta: "b" / "a"
        if isinstance(data, dict) and ("b" in data or "a" in data):
            # timestamp
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000.0)
            book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0

            if "b" in data and "a" in data:
                # snapshot refresh
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

            # delta update (only bids or only asks present)
            if "b" in data:
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None:
                        continue
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q

            if "a" in data:
                for px, qty in data.get("a", []):
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

        # legacy delta (delete/update/insert)
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
        Compute mid, spread, imbalance, burst for decision engine.
        Returns None if book/trades not usable.
        """
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC:
            return None

        # top 5 levels for imbalance
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

        # burst from recent trades inside RECENT_TRADE_WINDOW seconds
        cutoff = now - RECENT_TRADE_WINDOW
        burst = 0.0
        last_price = None
        for t in reversed(self.trades[symbol]):
            if t["ts"] < cutoff:
                break
            sz = t["size"]
            if t["side"] == "buy":
                burst += sz
            else:
                burst -= sz
            last_price = t["price"]

        if last_price is None:
            return None

        # score (impulse)
        score = abs(imbalance) * abs(burst) / max(spread, 1e-9)

        return {"mid": mid, "spread": spread, "imbalance": imbalance, "burst": burst, "score": score}


# ---------------- DYNAMIC TP helper ----------------


def choose_dynamic_tp(score: float) -> float:
    """
    Map score to TP %.
    Score scale may vary by symbol; this mapping is intentionally simple.
    We use TP range 0.4% .. 1.5% and scale with score.
    """
    # small helper clamp
    if score <= 0:
        return 0.004
    # linear-ish mapping (tunable)
    if score < 1.5:
        return 0.004  # 0.4%
    if score < 4.0:
        return 0.006  # 0.6%
    if score < 10.0:
        return 0.009  # 0.9%
    return 0.012     # 1.2%


# --------------- CORE BOT LOGIC -----------------


class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0

    def _log_skip(self, sym: str, reason: str, feat: dict):
        # Unconditionally print skip information (no rate-limit)
        mid = feat.get("mid", 0.0)
        spread = feat.get("spread", 0.0)
        imb = feat.get("imbalance", 0.0)
        burst = feat.get("burst", 0.0)
        score = feat.get("score", 0.0)
        print(f"[SKIP] {sym} | {reason} | mid={mid:.6f} spread={spread:.6f} imb={imb:.6f} burst={burst:.6f} score={score:.6f}", flush=True)

    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT.")
        # set leverage for all symbols
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)

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
                await send_telegram(f"🚨 Kill-switch: equity drawdown {dd*100:.2f}%. Position closed.")
                self.position = None
            raise SystemExit("Kill-switch triggered")

    async def eval_symbols_and_maybe_enter(self):
        # only one position at a time
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
                # either book/stale/no trades for window
                continue

            imb = feat["imbalance"]
            burst = feat["burst"]
            spread = feat["spread"]
            score = feat["score"]
            mid = feat["mid"]

            # --- BASIC FILTERS using only 4 params ---
            if spread <= 0 or spread > MAX_SPREAD:
                self._log_skip(sym, f"spread bad (>MAX_SPREAD {MAX_SPREAD})", feat)
                continue

            if abs(imb) < IMBALANCE_THRESH:
                self._log_skip(sym, f"imbalance small (<{IMBALANCE_THRESH})", feat)
                continue

            if abs(burst) < BURST_THRESH:
                self._log_skip(sym, f"burst small (<{BURST_THRESH})", feat)
                continue

            if score < SCORE_MIN:
                self._log_skip(sym, f"score small (<{SCORE_MIN})", feat)
                continue

            # determine direction purely from imbalance & burst sign agreement
            side = None
            if imb > 0 and burst > 0:
                side = "buy"
            elif imb < 0 and burst < 0:
                side = "sell"
            else:
                self._log_skip(sym, "imb/burst disagree on direction", feat)
                continue

            # choose best by raw score
            if score > best_score:
                best_score = score
                best_sym = sym
                best_feat = feat
                best_side = side

        if not best_sym or not best_feat or not best_side:
            # nothing passed -> heartbeat maybe
            await self.maybe_heartbeat()
            return

        # anti-spam: same symbol cannot retrigger immediately
        if now - self.mkt.last_signal_ts[best_sym] < 0.5:
            print(f"[INFO] Skip retrigger {best_sym} (anti-spam)", flush=True)
            return
        self.mkt.last_signal_ts[best_sym] = now

        await self.open_position(best_sym, best_feat, best_side)

    async def open_position(self, sym_ws: str, feat: dict, side: str):
        # fetch equity
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

        # round down
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        # dynamic TP based on score
        score = feat.get("score", 0.0)
        tp_pct = choose_dynamic_tp(score)
        # clamp tp between 0.35% and 1.5%
        tp_pct = max(0.0035, min(tp_pct, 0.015))
        sl_pct = SL_PCT

        if side == "buy":
            tp_price = mid * (1.0 + tp_pct)
            sl_price = mid * (1.0 - sl_pct)
        else:
            tp_price = mid * (1.0 - tp_pct)
            sl_price = mid * (1.0 + sl_pct)

        # place market entry
        try:
            order = await self.exchange.create_market_order(sym_ws, side, qty, reduce_only=False)
            entry_price = safe_float(order.get("average") or order.get("price"), mid) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # place TP as reduce-only limit (post-only if supported)
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(sym_ws, opp_side, qty, tp_price, reduce_only=True, post_only=True)
        except Exception as e:
            print(f"[TP ERROR] {sym_ws}: {e}", flush=True)
            await send_telegram(f"⚠️ TP placement failed for {sym_ws}")

        self.position = Position(symbol_ws=sym_ws, side=side, qty=qty, entry_price=entry_price, tp_price=tp_price, sl_price=sl_price, opened_ts=time.time())
        self.last_trade_time = time.time()

        print(f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.6f} TP={tp_price:.6f} SL≈{sl_price:.6f} score={score:.6f}", flush=True)
        await send_telegram(f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.6f} TP={tp_price:.6f} SL≈{sl_price:.6f}")

    async def watchdog_position(self):
        """
        Client-side SL: if current mid crosses SL_PCT*1.1 threshold -> close at market.
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
        if pos.side == "buy":
            dd = (pos.entry_price - mid) / pos.entry_price
        else:
            dd = (mid - pos.entry_price) / pos.entry_price

        if dd >= SL_PCT * 1.1:
            print(f"[SL TRIGGER] {sym} {pos.side} DD={dd*100:.2f}% closing market", flush=True)
            await self.exchange.close_position_market(sym)

            # clear position
            self.position = None
            await send_telegram(f"🛑 SL (watchdog) {sym} closed. DD={dd*100:.2f}%")

    async def maybe_heartbeat(self):
        now = time.time()
        if now - self.last_trade_time < HEARTBEAT_IDLE_SEC:
            return
        eq = await self.exchange.fetch_equity()
        print(f"[HEARTBEAT] idle, equity={eq:.2f}", flush=True)
        await send_telegram(f"💤 Bot idle. Equity={eq:.2f} USDT")


# --------------- WEBSOCKET LOOP -----------------


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
                    print("📡 Connected to WS, subscribing...", flush=True)
                    await ws.send_json({"op": "subscribe", "args": topics})

                    # mark ready
                    for sym in SYMBOLS_WS:
                        ws_ready[sym] = True
                    await send_telegram("📡 WS Connected (all symbols)")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue

                            # ignore subscribe+success messages
                            if data.get("success") is True:
                                continue

                            topic = data.get("topic")
                            if not topic:
                                continue

                            # ORDERBOOK messages
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                if isinstance(payload, list):
                                    payload = payload[0]
                                if isinstance(payload, dict):
                                    mkt.update_book(sym, payload)

                            # TRADES messages
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
            print(f"❌ WS crash: {e}. Reconnect in 1s", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1.0)


# --------------- DEBUG CONSOLE -----------------


def debug_console(mkt: MarketState, bot: ScalperBot):
    help_text = (
        "[DEBUG] Commands:\n"
        "  ws      - show websocket state\n"
        "  book    - show last orderbook timestamps\n"
        "  trades  - show count of recent trades per symbol\n"
        "  pos     - show current open position\n"
        "  help    - show this message\n"
    )
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


# --------------- MAIN -----------------


async def main():
    print(f"Starting {BOT_VERSION} ...", flush=True)
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

    # debug console in background
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
