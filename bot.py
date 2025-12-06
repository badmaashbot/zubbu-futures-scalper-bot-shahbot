#!/usr/bin/env python3
"""
Bybit USDT Perp Micro-Scalper V5 + EMA Trend Confirmation
- Orderbook + burst + imbalance + EMA trend
- Limit entries (IOC)
- Dynamic TP + dynamic BE SL
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

BOT_VERSION = "ZUBBU_V5_EMA_FINAL"

# ---------------- MOVE ANALYZER ----------------
class MoveAnalyzer:
    def __init__(self, max_len=500):
        self.moves = deque(maxlen=max_len)
    def add_move(self, pct: float):
        self.moves.append(pct)
    def avg_move(self):
        if not self.moves:
            return 0.006
        return sum(self.moves) / len(self.moves)

move_analyzer = MoveAnalyzer()

# ---------------- ENV ----------------
API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TG_TOKEN   = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
TESTNET    = os.getenv("BYBIT_TESTNET","0") in ("1","true","True")

SYMBOLS_WS = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","DOGEUSDT"]
SYMBOL_MAP = {
    "BTCUSDT":"BTC/USDT:USDT",
    "ETHUSDT":"ETH/USDT:USDT",
    "BNBUSDT":"BNB/USDT:USDT",
    "SOLUSDT":"SOL/USDT:USDT",
    "DOGEUSDT":"DOGE/USDT:USDT",
}
ws_ready = {s:False for s in SYMBOLS_WS}

WS_URL = (
    "wss://stream-testnet.bybit.com/v5/public/linear"
    if TESTNET else
    "wss://stream.bybit.com/v5/public/linear"
)

LEVERAGE               = 3
EQUITY_USE_FRACTION    = 0.95
SL_PCT                 = 0.0035
TP_MIN                 = 0.0040
TP_MAX                 = 0.0120
BE_TRIGGER             = 0.0040
BE_BUFFER              = 0.0002

SCORE_MIN        = 0.50
IMBALANCE_THRESH = 0.015
BURST_ACCUM_MIN  = 1.20
BURST_MICRO_MIN  = 0.20
MAX_SPREAD       = 0.0015
MIN_RANGE_PCT    = 0.00008

RECENT_TRADE_WINDOW_MICRO = 0.40
RECENT_TRADE_WINDOW_ACCUM = 3.00
BOOK_STALE_SEC            = 6.0
KILL_SWITCH_DD            = 0.05
HEARTBEAT_IDLE_SEC        = 1800

MIN_QTY_MAP={
  "BTCUSDT":0.001,
  "ETHUSDT":0.01,
  "BNBUSDT":0.01,
  "SOLUSDT":0.1,
  "DOGEUSDT":5.0,
}

# -------------- TG ----------------
_last_tg_ts=0.0
async def send_telegram(msg):
    global _last_tg_ts
    now=time.time()
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    if now-_last_tg_ts<30: return
    _last_tg_ts=now
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
              f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
              data={"chat_id":TG_CHAT_ID,"text":msg}
            )
    except: pass

# ----------- UTILS -----------------
def safe_float(x,default=None):
    try: return float(x)
    except: return default

# ----------- CCXT ------------------
class ExchangeClient:
    def __init__(self):
        cfg={"apiKey":API_KEY,"secret":API_SECRET,"enableRateLimit":True,
             "options":{"defaultType":"swap"}}
        if TESTNET:
            cfg["urls"]={"api":{"public":"https://api-testnet.bybit.com",
                                "private":"https://api-testnet.bybit.com"}}
        self.client=ccxt.bybit(cfg)

    async def set_leverage(self,sym_ws,lev):
        async def work():
            try:self.client.set_leverage(lev,SYMBOL_MAP[sym_ws],params={"category":"linear"})
            except: pass
        await asyncio.to_thread(work)

    async def fetch_equity(self):
        def work():
            bal=self.client.fetch_balance()
            total=safe_float(bal.get("USDT",{}).get("total"),0.0) or 0.0
            return total
        return await asyncio.to_thread(work)

    async def create_limit_order(self,sym_ws,side,qty,price,reduce_only=False,post_only=False,tif="IOC"):
        params={"category":"linear","timeInForce":tif}
        if reduce_only: params["reduceOnly"]=True
        if post_only:  params["postOnly"]=True
        def work():
            return self.client.create_order(
                SYMBOL_MAP[sym_ws],"limit",side,qty,price,params
            )
        return await asyncio.to_thread(work)

    async def close_position_market(self,sym_ws):
        sym=SYMBOL_MAP[sym_ws]
        def work():
            try:
                pos=self.client.fetch_positions([sym])
            except:return
            for p in pos:
                qty=safe_float(p.get("contracts"),0)
                if qty<=0:continue
                side="sell" if p.get("side")=="long" else "buy"
                try:self.client.create_order(sym,"market",side,abs(qty),None,
                                             {"category":"linear","reduceOnly":True})
                except:pass
        await asyncio.to_thread(work)

@dataclass
class Position:
    symbol_ws:str; side:str; qty:float; entry_price:float
    tp_price:float; sl_price:float; opened_ts:float; moved_to_be:bool=False

# -------- Market State ----------
class MarketState:
    def __init__(self):
        self.books={s:{"bids":{}, "asks":{}, "ts":0.0} for s in SYMBOLS_WS}
        self.trades={s:deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts={s:0.0 for s in SYMBOLS_WS}

        # ---------- EMA -----------
        self.ema_fast={s:0.0 for s in SYMBOLS_WS}
        self.ema_slow={s:0.0 for s in SYMBOLS_WS}
        self.ema_alpha_fast = 2/(9+1)   # EMA9
        self.ema_alpha_slow = 2/(21+1)  # EMA21

    def update_ema(self,sym,mid):
        f=self.ema_fast[sym]; s=self.ema_slow[sym]
        if f==0.0:
            self.ema_fast[sym]=mid; self.ema_slow[sym]=mid; return
        self.ema_fast[sym]= f + self.ema_alpha_fast*(mid-f)
        self.ema_slow[sym]= s + self.ema_alpha_slow*(mid-s)

    def update_book(self,symbol,data):
        book=self.books[symbol]
        if isinstance(data,dict) and ("b" in data or "a" in data):
            ts=data.get("ts") or time.time()*1000
            book["ts"]=safe_float(ts,time.time()*1000)/1000
            if "b" in data and "a" in data:
                book["bids"].clear(); book["asks"].clear()
                for px,qty in data["b"]:
                    p=safe_float(px); q=safe_float(qty)
                    if p and q:book["bids"][p]=q
                for px,qty in data["a"]:
                    p=safe_float(px); q=safe_float(qty)
                    if p and q:book["asks"][p]=q
                return
            if "b" in data:
                for px,qty in data["b"]:
                    p=safe_float(px); q=safe_float(qty)
                    if not p:continue
                    if q==0:book["bids"].pop(p,None)
                    else:   book["bids"][p]=q
            if "a" in data:
                for px,qty in data["a"]:
                    p=safe_float(px); q=safe_float(qty)
                    if not p:continue
                    if q==0:book["asks"].pop(p,None)
                    else:   book["asks"][p]=q
            return

    def add_trade(self,sym,t):
        self.trades[sym].append(t)

    def get_best_bid_ask(self,s):
        b=self.books[s]
        if not b["bids"] or not b["asks"]:return None
        return max(b["bids"]),min(b["asks"])

    def compute_features(self,s):
        b=self.books[s]
        if not b["bids"] or not b["asks"]:return None
        now=time.time()
        if now-b["ts"]>BOOK_STALE_SEC:return None
        bids_sorted=sorted(b["bids"].items(),key=lambda x:-x[0])[:5]
        asks_sorted=sorted(b["asks"].items(),key=lambda x:x[0]) [:5]
        bid_vol=sum(q for _,q in bids_sorted)
        ask_vol=sum(q for _,q in asks_sorted)
        if bid_vol+ask_vol==0:return None
        imbalance=(bid_vol-ask_vol)/(bid_vol+ask_vol)
        best_bid=bids_sorted[0][0]; best_ask=asks_sorted[0][0]
        mid=(best_bid+best_ask)/2
        spread=(best_ask-best_bid)/mid

        cutoff_micro=now-RECENT_TRADE_WINDOW_MICRO
        cutoff_accum=now-RECENT_TRADE_WINDOW_ACCUM
        burst_micro=0; burst_accum=0; prices=[]
        for t in reversed(self.trades[s]):
            ts=t["ts"]
            if ts<cutoff_accum:break
            q = t["size"] if t["side"]=="buy" else -t["size"]
            if ts>=cutoff_micro: burst_micro+=q
            burst_accum+=q
            prices.append(t["price"])
        if not prices:rng=0
        else: rng=(max(prices)-min(prices))/mid

        # update EMAs
        self.update_ema(s,mid)

        return {
         "mid":mid,
         "spread":spread,
         "imbalance":imbalance,
         "burst_micro":burst_micro,
         "burst_accum":burst_accum,
         "range_pct":rng
        }

# -------- SCORE --------
def compute_momentum_score(imb,b_accum,spread):
    if spread<=0:spread=1e-6
    return abs(imb)*abs(b_accum)/spread

def choose_dynamic_tp(imb,b_accum,b_micro,spread):
    score=compute_momentum_score(imb,b_accum,spread)
    if score<1.0: base=0.004
    elif score<2.0: base=0.006
    elif score<4.0: base=0.008
    else: base=0.012
    if abs(b_micro)>BURST_MICRO_MIN*2 and abs(b_accum)>BURST_ACCUM_MIN*1.2:
        base*=1.1
    return max(TP_MIN,min(base,TP_MAX))

# ===== CORE BOT =====
class ScalperBot:
    def __init__(self,ex,mkt):
        self.exchange=ex; self.mkt=mkt
        self.position=None
        self.start_equity=None
        self.last_trade_time=0.0
        self.last_heartbeat_ts=0.0
        self.last_skip_log={}

    def _log_skip(self,sym,reason,feat,extra=""):
        now=time.time(); key=f"{sym}:{reason}"
        if now-self.last_skip_log.get(key,0)<3:return
        self.last_skip_log[key]=now
        print(f"[SKIP] {sym} {reason} {extra}",flush=True)

    async def init_equity_and_leverage(self):
        eq=await self.exchange.fetch_equity()
        self.start_equity=eq
        print(f"[INIT] eq={eq:.2f} {BOT_VERSION}")
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s,LEVERAGE)

    async def eval_symbols_and_maybe_enter(self):
        if self.position:return
        best_score=0; best_sym=None; best_feat=None; best_side=None
        now=time.time()

        for sym in SYMBOLS_WS:
            feat=self.mkt.compute_features(sym)
            if not feat:continue
            imb=feat["imbalance"]; b_micro=feat["burst_micro"]
            b_accum=feat["burst_accum"]; spread=feat["spread"]
            rng=feat["range_pct"]

            if spread<=0 or spread>MAX_SPREAD: continue
            if rng<MIN_RANGE_PCT: continue
            if abs(imb)<IMBALANCE_THRESH: continue
            if abs(b_accum)<BURST_ACCUM_MIN: continue
            if abs(b_micro)<BURST_MICRO_MIN: continue

            # --------- Direction (Orderflow) ----------
            if imb>0 and b_micro>0 and b_accum>0:
                side="buy"
            elif imb<0 and b_micro<0 and b_accum<0:
                side="sell"
            else:
                continue

            # --------- EMA Trend Confirmation ----------
            ema_f=self.mkt.ema_fast[sym]
            ema_s=self.mkt.ema_slow[sym]
            if side=="buy" and not (ema_f>ema_s):
                continue
            if side=="sell" and not (ema_f<ema_s):
                continue

            score=compute_momentum_score(imb,b_accum,spread)
            if score<SCORE_MIN: continue

            if score>best_score:
                best_score=score; best_sym=sym
                best_feat=feat;   best_side=side

        if not best_sym:
            return

        if now-self.mkt.last_signal_ts[best_sym]<0.5:return
        self.mkt.last_signal_ts[best_sym]=now
        await self.open_position(best_sym,best_feat,best_side)

    async def open_position(self,sym,feat,side):
        eq=await self.exchange.fetch_equity()
        mid=feat["mid"]
        qty = (eq*EQUITY_USE_FRACTION*LEVERAGE)/mid
        qty = math.floor(qty*1000)/1000
        best=self.mkt.get_best_bid_ask(sym)
        if not best:return
        best_bid,best_ask=best
        price= best_bid if side=="buy" else best_ask

        # IOC ENTRY
        order=await self.exchange.create_limit_order(sym,side,qty,price,tif="IOC")
        entry=safe_float(order.get("average") or order.get("price"))
        if entry is None: return

        tp_pct=choose_dynamic_tp(feat["imbalance"],
                                 feat["burst_accum"],
                                 feat["burst_micro"],
                                 feat["spread"])
        if side=="buy":
            tp=entry*(1+tp_pct)
            sl=entry*(1-SL_PCT)
        else:
            tp=entry*(1-tp_pct)
            sl=entry*(1+SL_PCT)

        opp="sell" if side=="buy" else "buy"
        await self.exchange.create_limit_order(sym,opp,qty,tp,reduce_only=True,post_only=True,tif="GTC")

        self.position=Position(sym,side,qty,entry,tp,sl,time.time())
        print(f"[ENTRY] {sym} {side} qty={qty} entry={entry} TP={tp} SL={sl}")

    async def watchdog_position(self):
        if not self.position:return
        sym=self.position.symbol_ws
        feat=self.mkt.compute_features(sym)
        if not feat:return
        mid=feat["mid"]
        pos=self.position
        if pos.side=="buy":
            move=(mid-pos.entry_price)/pos.entry_price
            down=(pos.entry_price-mid)/pos.entry_price
        else:
            move=(pos.entry_price-mid)/pos.entry_price
            down=(mid-pos.entry_price)/pos.entry_price

        if (not pos.moved_to_be) and move>=BE_TRIGGER:
            if pos.side=="buy":
                pos.sl_price=pos.entry_price*(1-BE_BUFFER)
            else:
                pos.sl_price=pos.entry_price*(1+BE_BUFFER)
            pos.moved_to_be=True

        if pos.side=="buy" and mid<=pos.sl_price:
            await self.exchange.close_position_market(sym)
            self.position=None; return
        if pos.side=="sell" and mid>=pos.sl_price:
            await self.exchange.close_position_market(sym)
            self.position=None; return

# ========== WS & MAIN =========
async def ws_loop(mkt):
    topics=[]
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")
    while True:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(WS_URL) as ws:
                    await ws.send_json({"op":"subscribe","args":topics})
                    for msg in ws:
                        d=json.loads(msg.data)
                        topic=d.get("topic")
                        if not topic: continue
                        if topic.startswith("orderbook"):
                            sym=topic.split(".")[-1]
                            payload=d.get("data")
                            if isinstance(payload,list):payload=payload[0]
                            if isinstance(payload,dict):
                                mkt.update_book(sym,payload)
                        elif topic.startswith("publicTrade"):
                            sym=topic.split(".")[-1]
                            arr=d.get("data",[])
                            now=time.time()
                            for t in arr:
                                p=safe_float(t.get("p")); q=safe_float(t.get("v"))
                                side="buy" if (t.get("S")=="Buy") else "sell"
                                mkt.add_trade(sym,{"price":p,"size":q,"side":side,"ts":now})
        except:
            await asyncio.sleep(1)

async def main():
    ex=ExchangeClient(); mkt=MarketState(); bot=ScalperBot(ex,mkt)
    await bot.init_equity_and_leverage()
    ws_task=asyncio.create_task(ws_loop(mkt))
    while True:
        await bot.eval_symbols_and_maybe_enter()
        await bot.watchdog_position()
        await asyncio.sleep(0.5)

if __name__=="__main__":
    asyncio.run(main())
