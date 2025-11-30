#!/usr/bin/env python3
import os, sys, time, json, logging, asyncio, threading, zlib, decimal
from collections import deque
from typing import Dict, List, Optional, Tuple
import aiohttp, uvloop
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
from prometheus_client import start_http_server, Gauge, Counter

# ------------------------------------------------------------
# CONFIG – only touch here
# ------------------------------------------------------------
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET    = os.getenv("BYBIT_TESTNET", "0") in ("1","true","True")
TG_TOKEN   = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

SYMBOLS       = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT"]
LEVERAGE      = 3
EQUITY_FRAC   = 0.95          # 95 % of equity
TP_PCT_EQUITY = 0.01          # 1 % of equity
SL_PCT_EQUITY = 0.005         # 0.5 % of equity
MAX_POS       = 3
MIN_GAP_SEC   = 3
STALE_SEC     = 8*60
KILL_PCT      = 0.05          # 5 % equity loss → halt
SNAP_DEPTH    = 50            # L2 levels
SNAP_INTERVAL = 0.2           # 200 ms book copy
TELEGRAM_CD   = 5

WS_MAIN = "wss://stream.bybit.com/v5/public/linear"
WS_TEST = "wss://stream-testnet.bybit.com/v5/public/linear"
REST_MAIN = "https://api.bybit.com"
REST_TEST = "https://api-testnet.bybit.com"

# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger("fixed-scalp")

prom_pos  = Gauge("pos_open", "Open positions")
prom_pnl  = Gauge("pnl_usdt", "Unrealised PnL")
prom_sig  = Counter("signals", "Signals", ["sym","side"])

# -------------  Telegram  -------------
_last_tg = 0
async def tg(msg: str):
    global _last_tg
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    if time.time() - _last_tg < TELEGRAM_CD:
        return
    _last_tg = time.time()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
        try:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg}
            )
        except Exception as e:
            log.warning("Telegram error: %s", e)

# ------------------------------------------------------------
# BOOK – always consistent
# ------------------------------------------------------------
class Book:
    __slots__ = ("sym","bids","asks","ts","_lock","_crc_miss")
    def __init__(self, sym: str):
        self.sym = sym
        self.bids: Dict[decimal.Decimal, decimal.Decimal] = {}
        self.asks: Dict[decimal.Decimal, decimal.Decimal] = {}
        self.ts = 0
        self._lock = asyncio.Lock()
        self._crc_miss = 0

    # crc32 exactly like Bybit
    def _crc(self) -> int:
        bidpx = sorted(self.bids.keys(), reverse=True)[:10]
        askpx = sorted(self.asks.keys())[:10]
        s = ""
        for i in range(10):
            b = bidpx[i] if i < len(bidpx) else decimal.Decimal(0)
            a = askpx[i] if i < len(askpx) else decimal.Decimal(0)
            s += f"{b}:{self.bids.get(b,0)}:{a}:{self.asks.get(a,0)}:"
        return zlib.crc32(s.encode())

    async def snapshot(self, rest: ccxt.Exchange):
        """pull REST snapshot when CRC fails or on start"""
        params = {"category": "linear", "symbol": self.sym, "limit": SNAP_DEPTH}
        try:
            snap = await rest.publicGetV5MarketOrderbook(params)
            d = snap["result"]
            async with self._lock:
                self.bids.clear(); self.asks.clear()
                for px, qty in d["bids"]:
                    self.bids[decimal.Decimal(px)] = decimal.Decimal(qty)
                for px, qty in d["asks"]:
                    self.asks[decimal.Decimal(px)] = decimal.Decimal(qty)
                self.ts = int(d["ts"])
            log.warning("%s snapshot reloaded", self.sym)
        except Exception as e:
            log.error("%s snap failed %s", self.sym, e)

    async def update(self, data: dict):
        async with self._lock:
            self.ts = int(data["ts"])
            if data["type"] == "snapshot":
                self.bids.clear(); self.asks.clear()
                for px, qty in data["bids"]:
                    self.bids[decimal.Decimal(px)] = decimal.Decimal(qty)
                for px, qty in data["asks"]:
                    self.asks[decimal.Decimal(px)] = decimal.Decimal(qty)
            else:
                for key in ("delete", "update", "insert"):
                    part = data.get(key, {})
                    for px, qty in part.get("bids", []):
                        p, q = decimal.Decimal(px), decimal.Decimal(qty)
                        if q == 0: self.bids.pop(p, None)
                        else: self.bids[p] = q
                    for px, qty in part.get("asks", []):
                        p, q = decimal.Decimal(px), decimal.Decimal(qty)
                        if q == 0: self.asks.pop(p, None)
                        else: self.asks[p] = q
            # checksum
            if "crc" in data:
                if self._crc() != int(data["crc"]):
                    self._crc_miss += 1
                    raise ValueError("crc mismatch")

    def copy(self) -> Tuple[np.ndarray, np.ndarray]:
        async with self._lock:
            bids = np.array(sorted(((float(k), float(v)) for k, v in self.bids.items()),
                                   key=lambda x: -x[0])[:SNAP_DEPTH], dtype=np.double)
            asks = np.array(sorted(((float(k), float(v)) for k, v in self.asks.items())
                                   )[:SNAP_DEPTH], dtype=np.double)
        return bids, asks

# ------------------------------------------------------------
# EXCHANGE – async ccxt wrapper
# ------------------------------------------------------------
class Exchange:
    def __init__(self):
        cfg = {"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True,
               "options": {"defaultType": "swap"}}
        if TESTNET:
            cfg["urls"] = {"api": {"public": REST_TEST, "private": REST_TEST}}
        self.cc = ccxt.bybit(cfg)
        self.lock = asyncio.Lock()

    async def close(self): await self.cc.close()

    async def balance(self) -> float:
        async with self.lock:
            bal = await self.cc.fetch_balance()
            return float(bal["USDT"]["total"])

    async def unrealised(self) -> float:
        async with self.lock:
            pos = await self.cc.fetch_positions()
            return sum(float(p.get("unrealizedPnl") or 0) for p in pos)

    async def set_lev(self, sym: str):
        try:
            async with self.lock:
                await self.cc.set_leverage(LEVERAGE, sym, params={"category": "linear"})
        except: pass

    async def batch_order(self, sym: str, orders: List[dict]):
        # orders = [{"side":"Buy","type":"limit","qty":..,"price":..,"reduceOnly":False}, ...]
        async with self.lock:
            return await self.cc.private_post_v5_order_create_batch(
                params={"category": "linear", "request": orders})

    async def close_market(self, sym: str):
        try:
            async with self.lock:
                pos = await self.cc.fetch_positions([sym])
                for p in pos:
                    if float(p["contracts"]) == 0: continue
                    side = "sell" if p["side"] == "long" else "buy"
                    await self.cc.create_order(sym, "market", side, abs(float(p["contracts"])),
                                               params={"category": "linear", "reduceOnly": True})
        except: pass

# ------------------------------------------------------------
# INDICATORS – only what we need
# ------------------------------------------------------------
def imbalance(bids: np.ndarray, asks: np.ndarray) -> float:
    bv = bids[:5, 1].sum()
    av = asks[:5, 1].sum()
    return (bv - av) / (bv + av) if (bv + av) else 0.0

def trade_burst(trades: deque, ms: int = 150) -> float:
    cut = int(time.time() * 1000) - ms
    return sum(t["size"] if t["side"] == "buy" else -t["size"]
               for t in trades if t["ts"] >= cut)

def volume_surge(closes: deque) -> bool:
    if len(closes) < 20: return False
    vol1 = closes[-1]["volume"]
    med = np.median([c["volume"] for c in list(closes)[-20:-1]])
    return vol1 > med * 1.5

# ------------------------------------------------------------
# ENGINE
# ------------------------------------------------------------
class Engine:
    def __init__(self, exch: Exchange, symbols: List[str]):
        self.exch = exch
        self.symbols = symbols
        self.books = {s: Book(s) for s in symbols}
        self.trades = {s: deque(maxlen=2000) for s in symbols}
        self.closes = {s: deque(maxlen=200) for s in symbols}
        self.pos: Dict[str, dict] = {}
        self.last_trade_ts = {s: 0 for s in symbols}
        self.start_eq = 0.0
        self._lock = asyncio.Lock()

    async def init(self):
        self.start_eq = await self.exch.balance()

    async def on_trade(self, sym: str, side: str, price: float, qty: float):
        ts = int(time.time() * 1000)
        self.trades[sym].append({"ts": ts, "side": side.lower(), "size": float(qty)})

    async def on_candle_close(self, sym: str, close: float, volume: float):
        self.closes[sym].append({"close": close, "volume": volume})

    async def eval(self, sym: str):
        async with self._lock:
            await self._eval(sym)

    async def _eval(self, sym: str):
        if now() - self.last_trade_ts[sym] < MIN_GAP_SEC: return
        if len(self.pos) >= MAX_POS and sym not in self.pos: return
        bids, asks = self.books[sym].copy()
        if len(bids) == 0 or len(asks) == 0: return
        imb = imbalance(bids, asks)
        burst = trade_burst(self.trades[sym])
        spr = (asks[0, 0] - bids[0, 0]) / bids[0, 0]
        if abs(imb) < 0.25: return
        if abs(burst) < 0.3 * asks[0, 1]: return
        if spr > 0.002: return

        side = "Buy" if imb > 0 and burst > 0 else "Sell" if imb < 0 and burst < 0 else None
        if not side: return

        equity = await self.exch.balance() + await self.exch.unrealised()
        risk_usd = equity * EQUITY_FRAC * LEVERAGE
        qty = risk_usd / (bids[0, 0] if side == "Buy" else asks[0, 0])
        qty = round(qty, 3)

        # fixed TP/SL on *equity*
        tp_usd = equity * TP_PCT_EQUITY
        sl_usd = equity * SL_PCT_EQUITY
        if side == "Buy":
            entry = bids[0, 0] + 0.1 * 0.01 * bids[0, 0]  # +1 tick
            tp_price = entry + tp_usd / qty
            sl_price = entry - sl_usd / qty
        else:
            entry = asks[0, 0] - 0.1 * 0.01 * asks[0, 0]
            tp_price = entry - tp_usd / qty
            sl_price = entry + sl_usd / qty

        # send bracket in one batch
        orders = [
            {"symbol": sym, "side": side, "type": "limit", "qty": str(qty), "price": f"{entry:.6f}",
             "reduceOnly": False, "timeInForce": "PostOnly"},
            {"symbol": sym, "side": "Sell" if side == "Buy" else "Buy", "type": "limit", "qty": str(qty),
             "price": f"{tp_price:.6f}", "reduceOnly": True, "timeInForce": "GTC"},
            {"symbol": sym, "side": "Sell" if side == "Buy" else "Buy", "type": "stop", "qty": str(qty),
             "stopPrice": f"{sl_price:.6f}", "reduceOnly": True}
        ]
        try:
            await self.exch.batch_order(sym, orders)
            self.pos[sym] = {"side": side, "qty": qty, "entry": entry, "tp": tp_price, "sl": sl_price, "ts": now()}
            self.last_trade_ts[sym] = now()
            prom_sig.labels(sym, side).inc()
            await tg(f"📌 {sym} {side} {qty}@{entry:.4f}  TP={tp_price:.4f}  SL={sl_price:.4f}")
        except Exception as e:
            await tg(f"❌ {sym} entry error {e}")

    async def kill_switch(self):
        eq = await self.exch.balance() + await self.exch.unrealised()
        if self.start_eq and (self.start_eq - eq) / self.start_eq >= KILL_PCT:
            await tg("🚨 Kill-switch – equity down 5 %")
            for s in self.symbols:
                await self.exch.close_market(s)
            asyncio.get_event_loop().stop()

# ------------------------------------------------------------
# WS WORKER – reconnect + snapshot fallback
# ------------------------------------------------------------
class WS(threading.Thread):
    def __init__(self, sym: str, eng: Engine, rest: Exchange):
        super().__init__(daemon=True)
        self.sym = sym
        self.eng = eng
        self.rest = rest
        self.url = (WS_TEST if TESTNET else WS_MAIN) + "/websocket"
        self._stop = threading.Event()

    def run(self):
        uvloop.install()
        asyncio.run(self._run())

async def _run(self):
    while not self._stop.is_set():
        try:
            await self._one_loop()
        except Exception as e:
            log.error("%s WS crash %s – reconnect in 1 s", self.sym, e)
            await asyncio.sleep(1)
            
    async def _one_loop(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.url, heartbeat=10) as ws:
                sub = {"op": "subscribe", "args": [f"orderbook.50.{self.sym}", f"publicTrade.{self.sym}"]}
                await ws.send_json(sub)
                log.info("%s WS connected", self.sym)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if "topic" in data:
                            if "orderbook" in data["topic"]:
                                try:
                                    await self.eng.books[self.sym].update(data["data"][0])
                                except ValueError:  # crc fail
                                    await self.eng.books[self.sym].snapshot(self.rest.cc)
                            if "publicTrade" in data["topic"]:
                                for t in (data["data"] if isinstance(data["data"], list) else [data["data"]]):
                                    await self.eng.on_trade(self.sym, t["S"], float(t["p"]), float(t["q"]))
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        break
                # reconnect
                await asyncio.sleep(0.1)

    def close(self): self._stop.set()

# ------------------------------------------------------------
# CANDLE BUILDER – 1-min from trades
# ------------------------------------------------------------
class CandleBuilder:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.candles = {s: deque(maxlen=200) for s in SYMBOLS}
        self._task = None

    async def start(self):
        self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while True:
            await asyncio.sleep(60)
            for s in SYMBOLS:
                trades = list(self.eng.trades[s])
                if not trades: continue
                px = [t["price"] for t in trades if t["ts"] >= int(time.time() - 60) * 1000]
                vol = [t["size"] for t in trades if t["ts"] >= int(time.time() - 60) * 1000]
                if not px: continue
                c = {"close": px[-1], "volume": sum(vol)}
                self.eng.closes[s].append(c)
                await self.eng.on_candle_close(s, c["close"], c["volume"])

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
async def main():
    start_http_server(8000)
    rest = Exchange()
    eng = Engine(rest, SYMBOLS)
    await eng.init()
    for s in SYMBOLS:
        await rest.set_lev(s)
    # start candle builder
    cb = CandleBuilder(eng)
    await cb.start()
    # start ws
    workers = [WS(s, eng, rest) for s in SYMBOLS]
    for w in workers: w.start()
    # eval + kill loops
    async def eval_loop():
        while True:
            for s in SYMBOLS:
                await eng.eval(s)
                prom_pos.set(len(eng.pos))
                prom_pnl.set(await rest.unrealised())
            await asyncio.sleep(SNAP_INTERVAL)
    async def kill_loop():
        while True:
            await eng.kill_switch()
            await asyncio.sleep(2)
    await asyncio.gather(eval_loop(), kill_loop())

if __name__ == "__main__":
    try:
        uvloop.install()
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutdown")
