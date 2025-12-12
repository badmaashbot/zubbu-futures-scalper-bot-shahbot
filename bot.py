#!/usr/bin/env python3
# merged_zubbu_sweep_with_parser_wsfix.py

import os, time, asyncio, json, math, threading
from collections import deque
import aiohttp, ccxt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

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

API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET    = os.getenv("BYBIT_TESTNET", "0") == "1"
TG_TOKEN   = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

WS_URL = ("wss://stream-testnet.bybit.com/v5/public/linear"
          if TESTNET else "wss://stream.bybit.com/v5/public/linear")

# ----------------- Telegram rate-limited --------------------
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
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3.0)) as session:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            )
    except:
        pass

async def send_tg(msg: str):
    return await send_telegram(msg)

def safe_float(x, default=None):
    if x is None:
        return default
    try:
        return float(x)
    except:
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
        self.books = {s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS}
        self.trades = {s: deque(maxlen=3000) for s in SYMBOLS}
        self.candles = {s: deque(maxlen=150) for s in SYMBOLS}
        self.last_signal_ts = {s: 0.0 for s in SYMBOLS}
        self.last_skip_log = {}

    # ----------- FIXED: parser unchanged -----------
    def update_book(self, symbol: str, data: dict):
        book = self.books[symbol]

        if isinstance(data, list) and data:
            data = data[0]

        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000.0)
            book["ts"] = safe_float(ts_raw, time.time()*1000) / 1000.0

            if "b" in data and "a" in data:
                book["bids"].clear()
                book["asks"].clear()
                for px, qty in data["b"]:
                    p, q = safe_float(px), safe_float(qty)
                    if p and q:
                        book["bids"][p] = q
                for px, qty in data["a"]:
                    p, q = safe_float(px), safe_float(qty)
                    if p and q:
                        book["asks"][p] = q
                return

            if "b" in data:
                for px, qty in data["b"]:
                    p, q = safe_float(px), safe_float(qty)
                    if p:
                        if not q:
                            book["bids"].pop(p, None)
                        else:
                            book["bids"][p] = q

            if "a" in data:
                for px, qty in data["a"]:
                    p, q = safe_float(px), safe_float(qty)
                    if p:
                        if not q:
                            book["asks"].pop(p, None)
                        else:
                            book["asks"][p] = q
            return

        # legacy format
        ts_raw = data.get("ts") or time.time()*1000
        book["ts"] = safe_float(ts_raw, time.time()*1000) / 1000.0

        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()
            for px, qty in data.get("bids", []):
                p, q = safe_float(px), safe_float(qty)
                if p and q:
                    book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p, q = safe_float(px), safe_float(qty)
                if p and q:
                    book["asks"][p] = q
            return

    def add_trade(self, symbol: str, trade: dict):
        self.trades[symbol].append(trade)

    def last_candle(self, sym):
        return self.candles[sym][-2] if len(self.candles[sym]) > 1 else None

    def current_candle(self, sym):
        return self.candles[sym][-1] if self.candles[sym] else None

    def volume_sma(self, sym, n=20):
        closed = list(self.candles[sym])[:-1]
        if len(closed) < n:
            return None
        return sum(c["volume"] for c in closed[-n:]) / n

    def rsi(self, sym, n=14):
        closed = list(self.candles[sym])[:-1]
        closes = [c["close"] for c in closed]
        if len(closes) < n + 1:
            return None
        gains, losses = [], []
        for i in range(1,len(closes)):
            ch = closes[i] - closes[i-1]
            gains.append(max(ch,0))
            losses.append(max(-ch,0))
        avg_gain = sum(gains[-n:])/n
        avg_loss = sum(losses[-n:])/n
        if avg_loss == 0:
            return 100
        return 100 - 100/(1 + avg_gain/avg_loss)

    def build_1m_candles(self, sym):
        now = int(time.time())
        trades = [t for t in self.trades[sym] if t["ts"] >= now - 60]
        if not trades: 
            return
        prices = [t["price"] for t in trades]
        candle = {"open":prices[0],"high":max(prices),"low":min(prices),
                  "close":prices[-1],"volume":sum(t["size"] for t in trades),"ts":now}
        if not self.candles[sym] or candle["ts"]//60 != self.candles[sym][-1]["ts"]//60:
            self.candles[sym].append(candle)

    def _log_skip(self, sym, reason, feat=None, extra=""):
        now = time.time()
        key = f"{sym}:{reason}"
        if now - self.last_skip_log.get(key,0) < 5:
            return
        self.last_skip_log[key] = now
        print(f"[SKIP] {sym} {reason} | mid=0.0000 spread=0.000000 imb=0.0000 burst=0.0000 rng=0.000000")

# ----------------- EXCHANGE --------------------
class ExchangeClient:
    def __init__(self):
        cfg = {"apiKey": API_KEY, "secret": API_SECRET,
               "enableRateLimit": True,
               "options": {"defaultType": "swap"}}
        if TESTNET:
            cfg["urls"] = {"api":{"public":"https://api-testnet.bybit.com",
                                  "private":"https://api-testnet.bybit.com"}}
        self.client = ccxt.bybit(cfg)

    async def market_order(self, sym, side, qty):
        return await asyncio.to_thread(
            self.client.create_order, sym, "market", side, qty, None, {"category":"linear"}
        )

    async def fetch_balance(self):
        return await asyncio.to_thread(self.client.fetch_balance)

    async def fetch_ticker(self, sym):
        return await asyncio.to_thread(self.client.fetch_ticker, sym)

# ----------------- SWEEP BOT --------------------
class LiquiditySweepBot:
    def __init__(self, ex, mkt):
        self.ex = ex
        self.mkt = mkt
        self.pos = None
        self.last_signal = {s:0 for s in SYMBOLS}

    def detect_sweep(self, sym):
        self.mkt.build_1m_candles(sym)
        last = self.mkt.last_candle(sym)
        if not last: return False, None
        body = abs(last["close"] - last["open"])
        if body == 0: return False, None
        uw = last["high"] - max(last["open"], last["close"])
        lw = min(last["open"], last["close"]) - last["low"]
        if not (uw >= SWEEP_WICK_MIN*body or lw >= SWEEP_WICK_MIN*body): return False, None
        vol_sma = self.mkt.volume_sma(sym)
        if not vol_sma or last["volume"] < VOL_SPIKE*vol_sma: return False,None
        rsi = self.mkt.rsi(sym)
        if rsi is None: return False,None
        side=None
        if uw>=SWEEP_WICK_MIN*body and rsi>=RSI_OB: side="Sell"
        if lw>=SWEEP_WICK_MIN*body and rsi<=RSI_OS: side="Buy"
        if not side: return False,None
        curr = self.mkt.current_candle(sym)
        if not curr: return False,None
        if abs(curr["close"]-curr["open"]) < CONFIRM_BODY_PCT*curr["open"]: return False,None
        if side=="Sell" and curr["close"]<last["close"]: return True,side
        if side=="Buy" and curr["close"]>last["close"]: return True,side
        return False,None

    def spread_ok(self, sym):
        book = self.mkt.books[sym]
        if not book["bids"] or not book["asks"]:
            return False
        bid = max(book["bids"])
        ask = min(book["asks"])
        return (ask - bid)/ask <= MAX_SPREAD_PCT

    def btc_vol_high(self):
        btc = self.mkt.last_candle("BTCUSDT")
        if not btc: return False
        return (btc["high"]-btc["low"])/btc["open"] > BTC_VOL_MAX

    async def watch_exit(self):
        if not self.pos: return
        tick = await self.ex.fetch_ticker(self.pos["symbol"])
        mark = float(tick["last"])
        side, entry = self.pos["side"], self.pos["entry"]
        if side=="Buy":
            if mark>=entry*(1+TP_PCT/100) or mark<=entry*(1-SL_PCT/100):
                await self.ex.market_order(self.pos["symbol"],"Sell",self.pos["qty"])
                await send_tg(f"✅ {self.pos['symbol']} LONG closed @ {mark:.4f}")
                self.pos=None
        else:
            if mark<=entry*(1-TP_PCT/100) or mark>=entry*(1+SL_PCT/100):
                await self.ex.market_order(self.pos["symbol"],"Buy",self.pos["qty"])
                await send_tg(f"✅ {self.pos['symbol']} SHORT closed @ {mark:.4f}")
                self.pos=None

    async def eval_and_trade(self):
        await self.watch_exit()
        if self.btc_vol_high():
            self.mkt._log_skip("BTCUSDT","btc_vol")
            return
        for sym in SYMBOLS:
            if not self.spread_ok(sym):
                self.mkt._log_skip(sym,"spread")
                continue
            if time.time()-self.last_signal[sym] < SIGNAL_COOLDOWN:
                self.mkt._log_skip(sym,"cooldown")
                continue
            sweep, side = self.detect_sweep(sym)
            if not sweep:
                self.mkt._log_skip(sym,"no_sweep")
                continue
            bal = await self.ex.fetch_balance()
            equity = float(bal["USDT"]["total"])
            bid = max(self.mkt.books[sym]["bids"])
            ask = min(self.mkt.books[sym]["asks"])
            qty = round(equity * CAPITAL_RISK_PCT/100 / ((bid+ask)/2), 3)
            if qty<=0:
                self.mkt._log_skip(sym,"qty_zero")
                continue
            await self.ex.market_order(sym, side, qty)
            self.pos = {"symbol":sym, "side":side, "qty":qty,
                        "entry":(bid+ask)/2}
            self.last_signal[sym]=time.time()
            await send_tg(f"🧹 SWEEP {sym} {side} qty={qty}")

# ----------------- FIXED WEBSOCKET LOOP --------------------
# ONLY THIS PART WAS CHANGED — NOTHING ELSE
async def ws_loop(mkt: MarketState):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(WS_URL, heartbeat=20) as ws:

            # FIX: Correct subscription order (your old bot method)
            await ws.send_json({
                "op":"subscribe",
                "args":[
                    *[f"orderbook.1.{s}" for s in SYMBOLS],
                    *[f"publicTrade.{s}" for s in SYMBOLS]
                ]
            })

            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except:
                    continue

                topic = data.get("topic","")

                # FIX: Correct safe payload extraction
                if topic.startswith("orderbook"):
                    sym = topic.split(".")[-1]
                    payload = data.get("data")
                    if payload:
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
                    ts = time.time()
                    for t in trades:
                        price = safe_float(t.get("p"))
                        qty = safe_float(t.get("v"))
                        side = (t.get("S") or "buy").lower()
                        if price and qty:
                            mkt.add_trade(sym,{
                                "price":price,"size":qty,"side":side,"ts":ts
                            })

# ----------------- MAIN --------------------
async def main():
    mkt = MarketState()
    ex = ExchangeClient()
    bot = LiquiditySweepBot(ex, mkt)
    print("ZUBBU 90% SWEEP STARTED – SILENT MODE – TG ONLY")
    ws_task = asyncio.create_task(ws_loop(mkt))
    await asyncio.sleep(2)
    try:
        while True:
            await bot.eval_and_trade()
            await asyncio.sleep(0.2)
    finally:
        ws_task.cancel()
        await ws_task

if __name__ == "__main__":
    asyncio.run(main())
