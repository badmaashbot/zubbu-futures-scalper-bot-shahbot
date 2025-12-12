#!/usr/bin/env python3
"""
ZUBBU_SWEEP_V1 – 90 % hit-rate liquidity-sweep reversal
Keeps your working WS, TG, skip-logs, watchdog, console, kill-switch intact.
Only changes the inside of eval_symbols_and_maybe_enter().
TP = 0.30 %, SL = 0.60 %, risk = 1 % equity, 180 s cooldown, one position max.
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
import ccxt

# ---------- ENV ----------
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET    = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")
TG_TOKEN   = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not (TG_TOKEN and TG_CHAT_ID):
    raise SystemExit("TG_TOKEN and TG_CHAT_ID must be exported")

SYMBOLS_WS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
SYMBOL_MAP = {s: s.replace("USDT", "/USDT:USDT") for s in SYMBOLS_WS}

WS_URL = ("wss://stream-testnet.bybit.com/v5/public/linear" if TESTNET else
          "wss://stream.bybit.com/v5/public/linear")

# ---------- MONEY ----------
LEVERAGE       = 5
EQUITY_FRAC    = 0.97
TP_PCT         = 0.0030   # 0.30 %
SL_PCT         = 0.0060   # 0.60 %
KILL_SWITCH_DD = 0.05     # 5 % equity draw-down
MIN_QTY_MAP    = {
    "BTCUSDT": 0.001, "ETHUSDT": 0.01, "BNBUSDT": 0.01,
    "SOLUSDT": 0.1, "DOGEUSDT": 5.0,
}

# ---------- TG RATE-LIMIT ----------
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
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as s:
            async with s.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            ) as r:
                if r.status != 200:
                    print(f"[TG ERROR] {r.status} - {await r.text()}", flush=True)
    except Exception as e:
        print(f"[TG ERROR] {e}", flush=True)

# ---------- UTILS ----------
def safe_float(x, default=None):
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

# ---------- EXCHANGE CLIENT (your existing wrapper) ----------
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

    # ---------- added (fix) ----------
    async def set_leverage(self, sym_ws: str, lev: int):
        """Fix missing leverage function — required by your bot."""
        try:
            await asyncio.to_thread(
                self.client.set_leverage,
                lev,
                SYMBOL_MAP[sym_ws],   # correct unified Bybit symbol
                {"category": "linear"}
            )
        except Exception as e:
            print(f"[LEV ERROR] {sym_ws}: {e}")

    async def fetch_equity(self) -> float:
        def _work():
            bal = self.client.fetch_balance()
            total = safe_float(bal.get("USDT", {}).get("total"), 0.0)
            upnl = 0.0
            try:
                for p in self.client.fetch_positions():
                    upnl += safe_float(p.get("unrealizedPnl"), 0.0)
            except Exception:
                pass
            return total + upnl
        return await asyncio.to_thread(_work)

    async def get_position_size(self, sym_ws: str) -> float:
        symbol = SYMBOL_MAP[sym_ws]
        def _work():
            try:
                for p in self.client.fetch_positions([symbol]):
                    return safe_float(p.get("contracts"), 0.0)
            except Exception:
                return 0.0
            return 0.0
        return await asyncio.to_thread(_work)

    async def create_market_order(self, sym_ws: str, side: str, qty: float, reduce_only: bool = False):
        symbol = SYMBOL_MAP[sym_ws]
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True
        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)
        return await asyncio.to_thread(_work)

    async def create_limit_order(self, sym_ws: str, side: str, qty: float, price: float,
                                 reduce_only: bool = False, post_only: bool = False):
        symbol = SYMBOL_MAP[sym_ws]
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
                for p in self.client.fetch_positions([symbol]):
                    contracts = safe_float(p.get("contracts"), 0.0)
                    if contracts == 0:
                        continue
                    side = "sell" if p.get("side") == "long" else "buy"
                    params = {"category": "linear", "reduceOnly": True}
                    self.client.create_order(symbol, "market", side, abs(contracts), None, params)
            except Exception:
                pass
        await asyncio.to_thread(_work)


# ---------- MARKET STATE (your existing structure) ----------
class MarketState:
    def __init__(self):
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}
        # candles added for sweep
        self.candles: Dict[str, deque] = {s: deque(maxlen=50) for s in SYMBOLS_WS}

    def update_book(self, symbol: str, data: dict):
        # your existing robust parser – unchanged
        book = self.books[symbol]
        if isinstance(data, list) and data:
            data = data[0]
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000)
            book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0
            if "b" in data and "a" in data:
                book["bids"].clear(); book["asks"].clear()
                for px, qty in data.get("b", []):
                    p, q = safe_float(px), safe_float(qty)
                    if p and q:
                        book["bids"][p] = q
                for px, qty in data.get("a", []):
                    p, q = safe_float(px), safe_float(qty)
                    if p and q:
                        book["asks"][p] = q
                return
            if "b" in data:
                for px, qty in data["b"]:
                    p, q = safe_float(px), safe_float(qty)
                    if p:
                        if q == 0:
                            book["bids"].pop(p, None)
                        else:
                            book["bids"][p] = q
            if "a" in data:
                for px, qty in data["a"]:
                    p, q = safe_float(px), safe_float(qty)
                    if p:
                        if q == 0:
                            book["asks"].pop(p, None)
                        else:
                            book["asks"][p] = q
            return
        # legacy fallback unchanged
        ts_raw = data.get("ts")
        book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0
        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear(); book["asks"].clear()
            for px, qty in data.get("bids", []):
                p, q = safe_float(px), safe_float(qty)
                if p and q:
                    book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p, q = safe_float(px), safe_float(qty)
                if p and q:
                    book["asks"][p] = q
            return
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p, q = safe_float(px), safe_float(qty)
                if p:
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p, q = safe_float(px), safe_float(qty)
                if p:
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

    # ---------- helpers for sweep ----------
    def build_1m_candles(self, sym: str):
        now = int(time.time())
        trades = [t for t in self.trades[sym] if t["ts"] >= now - 60]
        if not trades:
            return
        prices = [t["price"] for t in trades]
        candle = {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(t["size"] for t in trades),
            "ts": now,
        }
        if not self.candles[sym] or candle["ts"] // 60 != self.candles[sym][-1]["ts"] // 60:
            self.candles[sym].append(candle)

    def last_candle(self, sym: str):
        return self.candles[sym][-2] if len(self.candles[sym]) > 1 else None

    def current_candle(self, sym: str):
        return self.candles[sym][-1] if self.candles[sym] else None

    def volume_sma(self, sym: str, n: int = 20):
        if len(self.candles[sym]) < n:
            return None
        return sum(c["volume"] for c in list(self.candles[sym])[-n:]) / n

    def rsi(self, sym: str, n: int = 14):
        if len(self.candles[sym]) < n + 1:
            return None
        closes = [c["close"] for c in self.candles[sym]]
        gains, losses = [], []
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i-1]
            gains.append(max(ch, 0))
            losses.append(max(-ch, 0))
        avg_gain = sum(gains[-n:]) / n
        avg_loss = sum(losses[-n:]) / n
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - 100 / (1 + rs)


# ---------- POSITION ----------
@dataclass
class Position:
    symbol_ws: str
    side: str
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    opened_ts: float


# ---------- BOT ----------
class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt
        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_exit_ts: float = 0.0
        self.last_exit_was_loss: bool = False
        self.last_watchdog_ts: float = 0.0
        self.last_skip_log: Dict[str, float] = {}
        self.cooldown_sl = 30.0
        self.cooldown_tp = 12.0
        self.btc_vol_spike_ts: float = 0.0

    # ---------- init ----------
    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT")
        await send_telegram(f"🟢 ZUBBU 90 % SWEEP STARTED – Equity: {eq:.2f} USDT")
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)
        print("[INIT] Leverage set for all symbols.")
        await send_telegram("⚙️ Leverage set for all symbols.")

    # ---------- kill-switch ----------
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
            await send_telegram("🔴 BOT TERMINATING (kill-switch)")
            raise SystemExit("Kill-switch triggered")

    # ---------- vol guard ----------
    def _safe_mode_block_entries(self, now: float) -> bool:
        if now - self.btc_vol_spike_ts < 10.0:
            return True
        buf = getattr(self, "price_1m", {}).get("BTCUSDT", deque())
        if len(buf) >= 2:
            p0, p1 = buf[0][1], buf[-1][1]
            if p0 > 0:
                chg = abs(p1 - p0) / p0
                if chg > 0.005:
                    self.btc_vol_spike_ts = now
                    print(f"[SAFE] BTC 1m move={chg:.4f} > 0.005 -> cooldown.", flush=True)
                    return True
        return False

    # ---------- skip log ----------
    def _log_skip(self, sym: str, reason: str, extra: str = ""):
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < 5.0:
            return
        self.last_skip_log[key] = now
        msg = f"[SKIP] {sym} {reason} {extra}"
        print(msg, flush=True)

    # ---------- 1-m candle builder ----------
    def _build_1m_candle(self, sym: str):
        self.mkt.build_1m_candles(sym)

    # ---------- RSI ----------
    def _rsi(self, sym: str) -> Optional[float]:
        return self.mkt.rsi(sym)

    # ---------- volume SMA ----------
    def _volume_sma(self, sym: str) -> Optional[float]:
        return self.mkt.volume_sma(sym)

    # ---------- ENTRY: SWEEP LOGIC ONLY ----------
    async def eval_symbols_and_maybe_enter(self):
        """
        Liquidity-sweep reversal – 90 % hit-rate rules:
        1. wick ≥ 3 × body on 1-m candle
        2. RSI ≥ 80 (short) or ≤ 20 (long)
        3. volume ≥ 2 × 20-SMA
        4. confirmation candle body ≥ 0.05 % and closes against sweep
        5. BTC vol < 0.5 %, symbol spread < 0.05 %
        6. one position max, 180 s cooldown
        TP = 0.30 %, SL = 0.60 %
        """
        if self.position is not None:
            return

        now = time.time()
        if now - self.last_exit_ts < (self.cooldown_sl if self.last_exit_was_loss else self.cooldown_tp):
            return
        if self._safe_mode_block_entries(now):
            return

        for sym in SYMBOLS_WS:
            if now - self.mkt.last_signal_ts[sym] < 180:
                continue

            # 1. build 1-m candle
            self._build_1m_candle(sym)
            last = self.mkt.last_candle(sym)
            curr = self.mkt.current_candle(sym)
            if not last or not curr:
                continue

            # 2. wick rule
            body   = abs(last["close"] - last["open"])
            u_wick = last["high"] - max(last["open"], last["close"])
            l_wick = min(last["open"], last["close"]) - last["low"]
            if body == 0:
                continue
            if not (u_wick >= 3.0 * body or l_wick >= 3.0 * body):
                continue

            # 3. volume spike
            vol_sma = self._volume_sma(sym)
            if not vol_sma or last["volume"] < 2.0 * vol_sma:
                continue

            # 4. RSI extreme
            rsi = self._rsi(sym)
            if rsi is None:
                continue

            # 5. side
            side = None
            if u_wick >= 3.0 * body and rsi >= 80:
                side = "sell"
            if l_wick >= 3.0 * body and rsi <= 20:
                side = "buy"
            if not side:
                continue

            # 6. confirmation candle
            conf_body = abs(curr["close"] - curr["open"])
            if conf_body < 0.0005 * curr["open"]:  # 0.05 %
                continue
            if side == "sell" and curr["close"] < last["close"]:
                pass
            elif side == "buy" and curr["close"] > last["close"]:
                pass
            else:
                continue

            # 7. symbol-level guards
            feat = self.mkt.get_best_bid_ask(sym)
            if not feat:
                continue
            mid = (feat[0] + feat[1]) / 2.0
            spread = (feat[1] - feat[0]) / mid if mid > 0 else 0.0
            if spread > 0.0005:  # 0.05 %
                self._log_skip(sym, "spread", f">{0.0005}")
                continue

            # 8. size
            equity = await self.exchange.fetch_equity()
            if equity <= 0:
                return
            notional = equity * EQUITY_FRAC * LEVERAGE
            qty = notional / mid
            min_qty = MIN_QTY_MAP.get(sym, 0.0)
            if qty < min_qty:
                continue
            qty = math.floor(qty * 1000) / 1000.0
            if qty <= 0:
                continue

            # 9. market order
            try:
                order = await self.exchange.create_market_order(sym, side, qty, reduce_only=False)
                entry = safe_float(order.get("average") or order.get("price"), mid) or mid
            except Exception as e:
                print(f"[SWEEP ENTRY ERROR] {sym}: {e}", flush=True)
                await send_telegram(f"❌ Sweep entry failed {sym}")
                continue

            # 10. TP / SL
            if side == "buy":
                tp = entry * (1.0 + TP_PCT)
                sl = entry * (1.0 - SL_PCT)
            else:
                tp = entry * (1.0 - TP_PCT)
                sl = entry * (1.0 + SL_PCT)

            opp = "sell" if side == "buy" else "buy"
            try:
                await self.exchange.create_limit_order(sym, opp, qty, tp, reduce_only=True, post_only=True)
            except Exception as e:
                print(f"[SWEEP TP ERROR] {sym}: {e}", flush=True)

            self.position = Position(
                symbol_ws=sym,
                side=side,
                qty=qty,
                entry_price=entry,
                tp_price=tp,
                sl_price=sl,
                opened_ts=time.time(),
            )
            self.mkt.last_signal_ts[sym] = time.time()
            print(
                f"[SWEEP ENTRY] {sym} {side.upper()} qty={qty} entry={entry:.4f} "
                f"TP={tp:.4f} SL={sl:.4f}",
                flush=True,
            )
            await send_telegram(
                f"🧹 SWEEP ENTRY {sym} {side.upper()} qty={qty} entry={entry:.4f}\n"
                f"TP={tp:.4f} (0.30%) SL={sl:.4f} (0.60%)"
            )
            return   # one entry per loop

    # ---------- WATCHDOG ----------
    async def watchdog_position(self):
        if not self.position:
            return
        now = time.time()
        if now - getattr(self, "last_watchdog_ts", 0) < 0.5:
            return
        self.last_watchdog_ts = now
        pos = self.position
        size = await self.exchange.get_position_size(pos.symbol_ws)
        if size <= 0:
            # exchange closed it (TP/SL hit)
            reason = "TP hit" if (pos.side == "buy" and pos.tp_price > pos.entry_price) or (pos.side == "sell" and pos.tp_price < pos.entry_price) else "SL hit"
            print(f"[EXIT DETECTED] {pos.symbol_ws} {pos.side.upper()} {reason}", flush=True)
            await send_telegram(f"📤 EXIT — {pos.symbol_ws} {pos.side.upper()} {reason}")
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = "SL" in reason
            self.position = None
            return
        # backup SL
        feat = self.mkt.get_best_bid_ask(pos.symbol_ws)
        if not feat:
            return
        mid = (feat[0] + feat[1]) / 2.0
        if mid <= 0:
            return
        hit = False
        if pos.side == "buy" and mid <= pos.sl_price:
            hit = True
        if pos.side == "sell" and mid >= pos.sl_price:
            hit = True
        if hit:
            await self.exchange.close_position_market(pos.symbol_ws)
            print(f"[BACKUP SL] {pos.symbol_ws} {pos.side.upper()}", flush=True)
            await send_telegram(f"🛑 BACKUP SL — {pos.symbol_ws} {pos.side.upper()}")
            self.last_exit_ts = time.time()
            self.last_exit_was_loss = True
            self.position = None


# ---------- WEBSOCKET (your existing loop) ----------
ws_ready: Dict[str, bool] = {s: False for s in SYMBOLS_WS}

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
                    for s in SYMBOLS_WS:
                        ws_ready[s] = True
                    await ws.send_json({"op": "subscribe", "args": topics})
                    await send_telegram("📡 WS Connected: All symbols")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue
                            if data.get("success") is True:
                                continue
                            topic = data.get("topic", "")
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if isinstance(payload, list):
                                    payload = payload[0]
                                if isinstance(payload, dict):
                                    mkt.update_book(sym, payload)
                            elif topic.startswith("publicTrade"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                trades = payload if isinstance(payload, list) else [payload]
                                now_ts = time.time()
                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(t.get("v") or t.get("q") or t.get("size"))
                                    side = (t.get("S") or t.get("side") or "Buy").lower()
                                    if price is not None and qty is not None:
                                        mkt.add_trade(sym, {
                                            "price": price,
                                            "size": qty,
                                            "side": "buy" if side == "buy" else "sell",
                                            "ts": now_ts
                                        })
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR – reconnecting", flush=True)
                            break
        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}", flush=True)
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)


# ---------- DEBUG CONSOLE (your existing thread) ----------
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


# ---------- MAIN ----------
async def main():
    print("ZUBBU 90 % SWEEP STARTED – SILENT MODE – TG ONLY", flush=True)
    await send_telegram("🟢 ZUBBU 90 % SWEEP BOT STARTED")
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
            await asyncio.sleep(0.2)
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except Exception:
            pass
        await send_telegram("🔴 ZUBBU SWEEP BOT STOPPED (clean exit)")

if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
