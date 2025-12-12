#!/usr/bin/env python3
# zubbu_sweep_ws_fixed.py
# Only change: WebSocket subscription uses topic+params (category: "linear").
# All other logic, parser, skip-logs, TG, and sweep detection are preserved.

import os, time, asyncio, json, math, threading
from collections import deque
import aiohttp, ccxt
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

# ----------  PARAMETERS ----------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
CAPITAL_RISK_PCT = 1.0
TP_PCT, SL_PCT   = 0.30, 0.60
SWEEP_WICK_MIN   = 3.0
RSI_OB, RSI_OS   = 80, 20
VOL_SPIKE        = 2.0
CONFIRM_BODY_PCT = 0.05
MAX_SPREAD_PCT   = 0.04
BTC_VOL_MAX      = 0.004
SIGNAL_COOLDOWN  = 180

# ----------  ENV  ----------
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET    = os.getenv("BYBIT_TESTNET", "0") == "1"
TG_TOKEN   = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not (TG_TOKEN and TG_CHAT_ID):
    raise SystemExit("TG_TOKEN and TG_CHAT_ID must be exported")

WS_URL = ("wss://stream-testnet.bybit.com/v5/public/linear"
          if TESTNET else "wss://stream.bybit.com/v5/public/linear")

# ----------------- Telegram rate-limited --------------------
_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0   # at most 1 msg every 30s to avoid spam

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
        # never let TG issues break the bot
        pass

# alias for compatibility
async def send_tg(msg: str):
    return await send_telegram(msg)

# ----------------- utils --------------------
def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

# ----------------- MARKET STATE + PARSER --------------------
@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    ts: int

class MarketState:
    def __init__(self):
        self.books: Dict[str, dict] = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS}
        self.trades: Dict[str, deque] = {s: deque(maxlen=3000) for s in SYMBOLS}
        self.candles: Dict[str, deque] = {s: deque(maxlen=150) for s in SYMBOLS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS}
        self.last_skip_log: Dict[str, float] = {}

    # Robust orderbook parser (merged from old working parser)
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]

        # If payload is a list, take first element
        if isinstance(data, list) and data:
            data = data[0]

        # New V5 compact format: 'b'/'a'
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000.0)
            book["ts"] = safe_float(ts_raw, time.time() * 1000.0) / 1000.0

            # snapshot
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

            # delta updates
            if "b" in data:
                for px, qty in data["b"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None:
                        continue
                    if not q:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q

            if "a" in data:
                for px, qty in data["a"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p is None:
                        continue
                    if not q:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q
            return

        # Legacy fallback
        ts_raw = data.get("ts") or (time.time() * 1000.0)
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

        # incremental updates legacy (if present)
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

    def last_candle(self, sym):
        return self.candles[sym][-2] if len(self.candles[sym]) > 1 else None

    def current_candle(self, sym):
        return self.candles[sym][-1] if self.candles[sym] else None

    def volume_sma(self, sym, n=20):
        closed = list(self.candles[sym])[:-1] if len(self.candles[sym]) > 1 else list(self.candles[sym])
        if len(closed) < n: return None
        return sum(c["volume"] for c in closed[-n:]) / n

    def rsi(self, sym, n=14):
        closed = list(self.candles[sym])[:-1] if len(self.candles[sym]) > 1 else list(self.candles[sym])
        closes = [c["close"] for c in closed]
        if len(closes) < n + 1: return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i-1]
            gains.append(max(ch, 0)); losses.append(max(-ch, 0))
        avg_gain = sum(gains[-n:]) / n
        avg_loss = sum(losses[-n:]) / n
        if avg_loss == 0: return 100
        return 100 - 100 / (1 + avg_gain / avg_loss)

    def build_1m_candles(self, sym):
        now = int(time.time())
        trades = [t for t in self.trades[sym] if t["ts"] >= now - 60]
        if not trades: return
        prices = [t["price"] for t in trades]
        candle = {"open": prices[0], "high": max(prices), "low": min(prices),
                  "close": prices[-1], "volume": sum(t["size"] for t in trades), "ts": now}
        if not self.candles[sym] or candle["ts"] // 60 != self.candles[sym][-1]["ts"] // 60:
            self.candles[sym].append(candle)

    def get_best_bid_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        best_bid = max(book["bids"].keys())
        best_ask = min(book["asks"].keys())
        return best_bid, best_ask

    def _log_skip(self, sym: str, reason: str, feat: dict = None, extra: str = "") -> None:
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < 5.0:
            return
        self.last_skip_log[key] = now
        mid = feat.get("mid", 0.0) if feat else 0.0
        spread = feat.get("spread", 0.0) if feat else 0.0
        imb = feat.get("imbalance", 0.0) if feat else 0.0
        burst = feat.get("burst", 0.0) if feat else 0.0
        rng = feat.get("range_pct", 0.0) if feat else 0.0
        msg = (
            f"[SKIP] {sym} {reason} {extra} | "
            f"mid={mid:.4f} spread={spread:.6f} imb={imb:.4f} burst={burst:.4f} rng={rng:.6f}"
        )
        print(msg, flush=True)

# ----------------- EXCHANGE --------------------
class ExchangeClient:
    def __init__(self):
        cfg = {"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True,
               "options": {"defaultType": "swap"}}
        if TESTNET:
            cfg["urls"] = {"api": {"public": "https://api-testnet.bybit.com",
                                   "private": "https://api-testnet.bybit.com"}}
        self.client = ccxt.bybit(cfg)

    async def market_order(self, sym, side, qty):
        return await asyncio.to_thread(
            self.client.create_order, sym, "market", side, qty, None, {"category": "linear"}
        )

    async def fetch_balance(self):
        return await asyncio.to_thread(self.client.fetch_balance)

    async def fetch_ticker(self, sym):
        return await asyncio.to_thread(self.client.fetch_ticker, sym)

# ----------------- SWEEP BOT --------------------
class LiquiditySweepBot:
    def __init__(self, ex, mkt):
        self.ex  = ex
        self.mkt = mkt
        self.pos = None
        self.last_signal = {s: 0 for s in SYMBOLS}
        self.counters = {
            "attempts": 0, "executed": 0, "skipped_cooldown": 0,
            "skipped_spread": 0, "skipped_btc_vol": 0, "skipped_no_sweep": 0,
            "tg_sent": 0, "errors": 0,
        }

    def build_1m_candles(self, sym):
        self.mkt.build_1m_candles(sym)

    def detect_sweep(self, sym):
        self.build_1m_candles(sym)
        last = self.mkt.last_candle(sym)
        if not last: return False, None
        body  = abs(last["close"] - last["open"])
        uwick = last["high"] - max(last["open"], last["close"])
        lwick = min(last["open"], last["close"]) - last["low"]
        if body == 0: return False, None
        if not (uwick >= SWEEP_WICK_MIN * body or lwick >= SWEEP_WICK_MIN * body): return False, None
        vol_sma = self.mkt.volume_sma(sym)
        if not vol_sma or last["volume"] < VOL_SPIKE * vol_sma: return False, None
        rsi = self.mkt.rsi(sym)
        if rsi is None: return False, None
        side = None
        if uwick >= SWEEP_WICK_MIN * body and rsi >= RSI_OB: side = "Sell"
        if lwick >= SWEEP_WICK_MIN * body and rsi <= RSI_OS: side = "Buy"
        if not side: return False, None
        curr = self.mkt.current_candle(sym)
        if not curr: return False, None
        if abs(curr["close"] - curr["open"]) < CONFIRM_BODY_PCT * curr["open"]: return False, None
        if side == "Sell" and curr["close"] < last["close"]: return True, side
        if side == "Buy"  and curr["close"] > last["close"]: return True, side
        return False, None

    def btc_vol_high(self):
        btc = self.mkt.last_candle("BTCUSDT")
        if not btc: return False
        return (btc["high"] - btc["low"]) / btc["open"] > BTC_VOL_MAX

    def spread_ok(self, sym):
        book = self.mkt.books[sym]
        if not book["bids"] or not book["asks"]: return False
        best_bid, best_ask = max(book["bids"]), min(book["asks"])
        return (best_ask - best_bid) / best_ask <= MAX_SPREAD_PCT

    async def watch_exit(self):
        if not self.pos: return
        tick = await self.ex.fetch_ticker(self.pos["symbol"])
        mark = float(tick["last"])
        side, entry = self.pos["side"], self.pos["entry"]
        if side == "Buy":
            if mark >= entry * (1 + TP_PCT / 100) or mark <= entry * (1 - SL_PCT / 100):
                await self.ex.market_order(self.pos["symbol"], "Sell", self.pos["qty"])
                await send_tg(f"✅ {self.pos['symbol']} LONG closed @ {mark:.4f}")
                self.pos = None
        else:
            if mark <= entry * (1 - TP_PCT / 100) or mark >= entry * (1 + SL_PCT / 100):
                await self.ex.market_order(self.pos["symbol"], "Buy", self.pos["qty"])
                await send_tg(f"✅ {self.pos['symbol']} SHORT closed @ {mark:.4f}")
                self.pos = None

    async def eval_and_trade(self):
        await self.watch_exit()
        if self.btc_vol_high():
            self.counters["skipped_btc_vol"] += 1
            self.mkt._log_skip("BTCUSDT", "btc_vol_high", None, f"BTC_VOL_MAX={BTC_VOL_MAX}")
            return
        for sym in SYMBOLS:
            if not self.spread_ok(sym):
                self.counters["skipped_spread"] += 1
                self.mkt._log_skip(sym, "spread", None, f"> MAX_SPREAD_PCT({MAX_SPREAD_PCT})")
                continue
            if time.time() - self.last_signal[sym] < SIGNAL_COOLDOWN:
                self.counters["skipped_cooldown"] += 1
                self.mkt._log_skip(sym, "cooldown", None, f"cooldown={SIGNAL_COOLDOWN}s")
                continue
            sweep, side = self.detect_sweep(sym)
            if not sweep:
                self.counters["skipped_no_sweep"] += 1
                self.mkt._log_skip(sym, "no_sweep", None, "")
                continue
            try:
                bal   = await self.ex.fetch_balance()
                equity= float(bal["USDT"]["total"])
                best_bid = max(self.mkt.books[sym]["bids"])
                best_ask = min(self.mkt.books[sym]["asks"])
                qty   = round(equity * CAPITAL_RISK_PCT / 100 /
                              ((best_bid + best_ask) / 2), 3)
                if qty == 0:
                    self.mkt._log_skip(sym, "qty_zero", None, "")
                    continue
                await self.ex.market_order(sym, side, qty)
                self.pos = {"symbol": sym, "side": side, "qty": qty,
                            "entry": (best_bid + best_ask) / 2}
                self.last_signal[sym] = time.time()
                self.counters["executed"] += 1
                await send_tg(f"🧹 SWEEP {sym} {side} qty={qty}")
                self.counters["tg_sent"] += 1
            except Exception as e:
                self.counters["errors"] += 1
                print("EVAL CRASH:", str(e))

    async def periodic_report(self, interval=3600):
        while True:
            await asyncio.sleep(interval)
            c = self.counters
            msg = (f"REPORT last {interval}s — attempts:{c['attempts']} executed:{c['executed']} "
                   f"skipped_cd:{c['skipped_cooldown']} skipped_spread:{c['skipped_spread']} "
                   f"skipped_btc:{c['skipped_btc_vol']} tg:{c['tg_sent']} errors:{c['errors']}")
            print(msg, flush=True)
            try:
                await send_tg(msg)
            except Exception:
                pass

# ----------------- WEBSOCKET LOOP (FIXED) --------------------
async def ws_loop(mkt: MarketState):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(WS_URL, heartbeat=20) as ws:
            # FIX: subscribe using topic objects with params specifying category 'linear'
            args = []
            for s in SYMBOLS:
                args.append({"topic": f"orderbook.1.{s}", "params": {"category": "linear"}})
            for s in SYMBOLS:
                args.append({"topic": f"publicTrade.{s}", "params": {"category": "linear"}})

            await ws.send_json({"op": "subscribe", "args": args})

            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    # non-text (ping/pong or closed) — skip
                    continue
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue

                topic = data.get("topic", "")
                # ORDERBOOK
                if topic.startswith("orderbook"):
                    sym = topic.split(".")[-1]
                    payload = data.get("data")
                    if not payload:
                        continue
                    if isinstance(payload, list):
                        payload = payload[0]
                    if isinstance(payload, dict):
                        # pass to your robust parser
                        mkt.update_book(sym, payload)
                # TRADES
                elif topic.startswith("publicTrade"):
                    sym = topic.split(".")[-1]
                    payload = data.get("data")
                    if not payload:
                        continue
                    trades = payload if isinstance(payload, list) else [payload]
                    ts = time.time()
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
                            "ts": ts
                        })

# ----------------- DEBUG CONSOLE (optional) --------------------
def debug_console(mkt: MarketState, bot: LiquiditySweepBot):
    help_text = (
        "[DEBUG] Commands:\n"
        "  ws      - show last orderbook timestamps\n"
        "  book    - show last orderbook timestamps\n"
        "  trades  - show # of recent trades per symbol\n"
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
        if cmd == "ws" or cmd == "book":
            ts_map = {s: mkt.books[s]["ts"] for s in SYMBOLS}
            print("[DEBUG] book ts =", ts_map, flush=True)
        elif cmd == "trades":
            counts = {s: len(mkt.trades[s]) for s in SYMBOLS}
            print("[DEBUG] trades len =", counts, flush=True)
        elif cmd == "pos":
            print("[DEBUG] position =", getattr(bot, "pos", None), flush=True)
        elif cmd == "help":
            print(help_text, flush=True)
        elif cmd == "":
            continue
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)

# ----------------- MAIN --------------------
async def main():
    mkt = MarketState()
    ex  = ExchangeClient()
    bot = LiquiditySweepBot(ex, mkt)
    print("ZUBBU 90 % SWEEP STARTED – SILENT MODE – TG ONLY")
    # debug console thread (optional)
    threading.Thread(target=debug_console, args=(mkt, bot), daemon=True).start()
    ws_task = asyncio.create_task(ws_loop(mkt))
    asyncio.create_task(bot.periodic_report(3600))
    await asyncio.sleep(2)  # let first candles form
    try:
        while True:
            try:
                await bot.eval_and_trade()
            except Exception as e:
                print("EVAL CRASH:", str(e))
            await asyncio.sleep(0.2)
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
