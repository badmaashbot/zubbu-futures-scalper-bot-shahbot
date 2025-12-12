#!/usr/bin/env python3
#  zubbu_sweep_v1.py  – 90 % accuracy edition  –  WITH CONSOLE PULSE + TG ERROR PRINT
import os
import time
import asyncio
import json
import math
from collections import deque
import aiohttp
import ccxt

# ------------------------------------------------
# 1.  PARAMETERS (same 90 % values)
# ------------------------------------------------
SYMBOLS          = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
CANDLE_TF        = "1m"
CAPITAL_RISK_PCT = 1.0
TP_PCT           = 0.30
SL_PCT           = 0.60
MAX_SPREAD_PCT   = 0.04
BTC_VOL_MAX      = 0.004
SIGNAL_COOLDOWN  = 180
SWEEP_WICK_MIN   = 3.0
RSI_OB           = 80
RSI_OS           = 20
VOL_SPIKE        = 2.0
CONFIRM_BODY_PCT = 0.05

# ------------------------------------------------
# 2.  ENVIRONMENT
# ------------------------------------------------
API_KEY     = os.getenv("BYBIT_API_KEY")
API_SECRET  = os.getenv("BYBIT_API_SECRET")
TESTNET     = os.getenv("BYBIT_TESTNET", "0") == "1"
TG_TOKEN    = os.getenv("TG_TOKEN")
TG_CHAT_ID  = os.getenv("TG_CHAT_ID")

WS_URL = ("wss://stream-testnet.bybit.com/v5/public/linear"
          if TESTNET else "wss://stream.bybit.com/v5/public/linear")

# ------------------------------------------------
# 3.  TELEGRAM  (console error if fails)
# ------------------------------------------------
async def send_tg(msg: str):
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            async with session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            ) as resp:
                if resp.status != 200:
                    print(f"[TG ERROR] {resp.status} - {await resp.text()}")
    except Exception as e:
        print(f"[TG ERROR] {e}")

# ------------------------------------------------
# 4.  MARKET STATE
# ------------------------------------------------
class MarketState:
    def __init__(self):
        self.books = {s: {"bids": {}, "asks": {}, "ts": 0} for s in SYMBOLS}
        self.trades = {s: deque(maxlen=3000) for s in SYMBOLS}
        self.candles = {s: deque(maxlen=150) for s in SYMBOLS}

    def update_book(self, symbol, data):
        self.books[symbol]["bids"] = {float(p): float(v) for p, v in data["b"]}
        self.books[symbol]["asks"] = {float(p): float(v) for p, v in data["a"]}
        self.books[symbol]["ts"] = time.time()

    def add_trade(self, symbol, trade):
        self.trades[symbol].append(trade)

    def last_candle(self, sym):
        return self.candles[sym][-2] if len(self.candles[sym]) > 1 else None

    def current_candle(self, sym):
        return self.candles[sym][-1] if self.candles[sym] else None

    def volume_sma(self, sym, length=20):
        if len(self.candles[sym]) < length:
            return None
        return sum(c["volume"] for c in list(self.candles[sym])[-length:]) / length

    def rsi(self, sym, length=14):
        closes = [c["close"] for c in self.candles[sym]]
        if len(closes) < length + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            ch = closes[i] - closes[i - 1]
            gains.append(max(ch, 0))
            losses.append(max(-ch, 0))
        avg_gain = sum(gains[-length:]) / length
        avg_loss = sum(losses[-length:]) / length
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - 100 / (1 + rs)

# ------------------------------------------------
# 5.  EXCHANGE
# ------------------------------------------------
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

    async def market_order(self, symbol, side, qty):
        def _run():
            return self.client.create_order(
                symbol, "market", side, qty, None, {"category": "linear"}
            )
        return await asyncio.to_thread(_run)

    async def fetch_balance(self):
        return await asyncio.to_thread(self.client.fetch_balance)

# ------------------------------------------------
# 6.  BOT
# ------------------------------------------------
class LiquiditySweepBot:
    def __init__(self, exchange, mkt):
        self.exchange = exchange
        self.mkt = mkt
        self.position = None
        self.last_signal = {s: 0 for s in SYMBOLS}

    # ---------- candle builder ----------
    def build_1m_candles(self, sym):
        now = int(time.time())
        trades = list(self.mkt.trades[sym])
        if not trades:
            return
        start = now - 60
        bucket = [t for t in trades if t["ts"] >= start]
        if not bucket:
            return
        prices = [t["price"] for t in bucket]
        volume = sum(t["size"] for t in bucket)
        candle = {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": volume,
            "ts": now,
        }
        if (
            not self.mkt.candles[sym]
            or candle["ts"] // 60 != self.mkt.candles[sym][-1]["ts"] // 60
        ):
            self.mkt.candles[sym].append(candle)

    # ---------- sweep detection ----------
    def detect_sweep(self, sym):
        self.build_1m_candles(sym)
        last = self.mkt.last_candle(sym)
        if not last:
            return False, None
        body = abs(last["close"] - last["open"])
        upper_wick = last["high"] - max(last["open"], last["close"])
        lower_wick = min(last["open"], last["close"]) - last["low"]
        if body == 0:
            return False, None
        if not (upper_wick >= SWEEP_WICK_MIN * body or lower_wick >= SWEEP_WICK_MIN * body):
            return False, None
        vol_sma = self.mkt.volume_sma(sym)
        if not vol_sma or last["volume"] < VOL_SPIKE * vol_sma:
            return False, None
        rsi = self.mkt.rsi(sym)
        if rsi is None:
            return False, None
        side = None
        if upper_wick >= SWEEP_WICK_MIN * body and rsi >= RSI_OB:
            side = "Sell"
        if lower_wick >= SWEEP_WICK_MIN * body and rsi <= RSI_OS:
            side = "Buy"
        if not side:
            return False, None
        curr = self.mkt.current_candle(sym)
        if not curr:
            return False, None
        conf_body = abs(curr["close"] - curr["open"])
        if conf_body < CONFIRM_BODY_PCT * curr["open"]:
            return False, None
        if side == "Sell" and curr["close"] < last["close"]:
            return True, side
        if side == "Buy" and curr["close"] > last["close"]:
            return True, side
        return False, None

    # ---------- guards ----------
    def btc_vol_high(self):
        btc = self.mkt.last_candle("BTCUSDT")
        if not btc:
            return False
        rng = (btc["high"] - btc["low"]) / btc["open"]
        return rng > BTC_VOL_MAX

    def spread_ok(self, sym):
        book = self.mkt.books[sym]
        if not book["bids"] or not book["asks"]:
            return False
        best_bid = max(book["bids"])
        best_ask = min(book["asks"])
        spr = (best_ask - best_bid) / best_ask
        return spr <= MAX_SPREAD_PCT

    # ---------- exit ----------
    async def watch_exit(self):
        if not self.position:
            return
        sym = self.position["symbol"]
        side = self.position["side"]
        entry = self.position["entry"]
        tick = await asyncio.to_thread(
            self.exchange.client.fetch_ticker, sym
        )
        mark = float(tick["last"])
        if side == "Buy":
            if mark >= entry * (1 + TP_PCT / 100) or mark <= entry * (1 - SL_PCT / 100):
                await self.exchange.market_order(sym, "Sell", self.position["qty"])
                await send_tg(f"✅ {sym} LONG closed @ {mark:.4f}")
                self.position = None
        else:
            if mark <= entry * (1 - TP_PCT / 100) or mark >= entry * (1 + SL_PCT / 100):
                await self.exchange.market_order(sym, "Buy", self.position["qty"])
                await send_tg(f"✅ {sym} SHORT closed @ {mark:.4f}")
                self.position = None

    # ---------- main eval ----------
    async def eval_and_trade(self):
        await self.watch_exit()
        if self.btc_vol_high():
            return
        for sym in SYMBOLS:
            if not self.spread_ok(sym):
                continue
            if time.time() - self.last_signal[sym] < SIGNAL_COOLDOWN:
                continue
            sweep, side = self.detect_sweep(sym)
            if not sweep:
                continue
            bal = await self.exchange.fetch_balance()
            equity = float(bal["USDT"]["total"])
            risk_usd = equity * CAPITAL_RISK_PCT / 100
            book = self.mkt.books[sym]
            mid = (max(book["bids"]) + min(book["asks"])) / 2
            qty = round(risk_usd / mid, 3)
            if qty == 0:
                continue
            await self.exchange.market_order(sym, side, qty)
            self.position = {"symbol": sym, "side": side, "qty": qty, "entry": mid}
            self.last_signal[sym] = time.time()
            await send_tg(f"🧹 SWEEP {sym} {side} qty={qty}")

# ------------------------------------------------
# 7.  WS LOOP
# ------------------------------------------------
async def ws_loop(mkt: MarketState):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(WS_URL, heartbeat=20) as ws:
            await ws.send_json(
                {
                    "op": "subscribe",
                    "args": [f"publicTrade.{s}" for s in SYMBOLS]
                    + [f"orderbook.1.{s}" for s in SYMBOLS],
                }
            )
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    topic = data.get("topic", "")
                    if topic.startswith("publicTrade"):
                        sym = topic.split(".")[-1]
                        for t in data.get("data", []):
                            mkt.add_trade(
                                sym,
                                {
                                    "price": float(t["p"]),
                                    "size": float(t["v"]),
                                    "side": t["S"].lower(),
                                    "ts": time.time(),
                                },
                            )
                    elif topic.startswith("orderbook"):
                        sym = topic.split(".")[-1]
                        mkt.update_book(sym, data["data"])

# ------------------------------------------------
# 8.  MAIN  (console pulse on start)
# ------------------------------------------------
async def main():
    mkt = MarketState()
    ex = ExchangeClient()
    bot = LiquiditySweepBot(ex, mkt)
    print("ZUBBU 90 % SWEEP BOT STARTED – console silent, TG alerts only.")
    ws_task = asyncio.create_task(ws_loop(mkt))
    await asyncio.sleep(2)
    while True:
        try:
            await bot.eval_and_trade()
        except Exception as e:
            print("Eval error:", e)
        await asyncio.sleep(0.2)

if __name__ == "__main__":
    asyncio.run(main())
