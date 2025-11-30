#!/usr/bin/env python3
import os
import sys
import time
import json
import logging
import threading
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional, List

import ccxt
import websocket
import requests

# ================================================================
# CONFIG (UPDATED)
# ================================================================

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")

if not API_KEY or not API_SECRET:
    raise Exception("API keys missing. Please set BYBIT_API_KEY and BYBIT_API_SECRET.")

# True = testnet, False = live
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

# Symbols
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT"]

CCXT_SYMBOL_MAP = {
    "BTCUSDT": "BTC/USDT:USDT",
    "ETHUSDT": "ETH/USDT:USDT",
    "SOLUSDT": "SOL/USDT:USDT",
    "BNBUSDT": "BNB/USDT:USDT",
    "DOGEUSDT": "DOGE/USDT:USDT",
}

PUBLIC_WS_MAINNET = "wss://stream.bybit.com/v5/public/linear"
PUBLIC_WS_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"

SCAN_INTERVAL = 4.0      # FASTER scanning for more scalps
LEVERAGE = 3
MARGIN_FRACTION = 0.95

TP_PCT_ON_POSITION = 0.01    # 1% TP
SL_PCT_ON_POSITION = 0.005   # 0.5% SL

GLOBAL_KILL_TRIGGER = 0.05   # 5% equity loss kill switch
STARTING_EQUITY: Optional[float] = None
bot_killed: bool = False
kill_alert_sent: bool = False

MAX_CONCURRENT_POSITIONS = 1

POST_ONLY_TIMEOUT = 2.5

# RELAXED FILTERS (as you requested)
MICRO_DELTA_THRESHOLD = 0.02
MICRO_BURST_THRESHOLD = 0.15
VOL_MOVE_PCT_1S = 0.006      # relaxed unpredictability
VOL_MOVE_PCT_3S = 0.012
IMBALANCE_THRESHOLD = 0.03
SPREAD_MAX_PCT = 0.0015
MIN_CANDLE_VOLATILITY = 0.0015

MIN_SL_DIST_PCT = 0.0005
MIN_TRADE_INTERVAL_SEC = 5.0
ERROR_PAUSE_SEC = 8.0
STALE_OB_MAX_SEC = 2.0

WS_SILENCE_SEC = 3.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("scalper_bot")

def now_ts() -> float:
    return time.time()

# ================================================================
# TELEGRAM (unchanged, safe)
# ================================================================

TELEGRAM_TOKEN = os.getenv("TG_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TG_CHAT_ID")

_last_tg_time: float = 0.0
_last_tg_text: str = ""
_last_tg_text_time: float = 0.0

TELEGRAM_MIN_INTERVAL_SEC = 10.0
TELEGRAM_DUP_SUPPRESS_SEC = 30.0
TELEGRAM_HTTP_TIMEOUT = 3.0

def tg(msg: str) -> None:
    global _last_tg_time, _last_tg_text, _last_tg_text_time

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    now = now_ts()

    if now - _last_tg_time < TELEGRAM_MIN_INTERVAL_SEC:
        return

    if msg == _last_tg_text and (now - _last_tg_text_time) < TELEGRAM_DUP_SUPPRESS_SEC:
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        requests.post(url, data=payload, timeout=TELEGRAM_HTTP_TIMEOUT)
        _last_tg_time = now
        _last_tg_text = msg
        _last_tg_text_time = now
    except Exception as e:
        logger.warning(f"Telegram Error: {e}")

# ================================================================
# HEARTBEAT
# ================================================================

def heartbeat_loop():
    while True:
        tg("💓 Bot running (heartbeat)")
        time.sleep(600)

def start_heartbeat():
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()

# ================================================================
# POSITION STRUCT
# ================================================================

@dataclass
class Position:
    symbol: str
    side: str
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    notional: float
    ts_open: float = field(default_factory=now_ts)
# ================================================================
# PnL & POSITION SIZING
# ================================================================

def calc_pnl(side: str, entry: float, exit: float, qty: float) -> float:
    if side.lower() == "buy":
        return round((exit - entry) * qty, 4)
    else:
        return round((entry - exit) * qty, 4)


def compute_position_and_prices(
    equity_usd: float,
    entry_price: float,
    side: str,
) -> Tuple[float, float, float, float]:

    if equity_usd <= 0 or entry_price <= 0:
        return 0.0, 0.0, 0.0, 0.0

    notional = equity_usd * MARGIN_FRACTION * LEVERAGE
    qty = notional / entry_price
    qty = float(f"{qty:.6f}")

    # TP/SL based on side
    if side == "buy":
        tp = entry_price * (1 + TP_PCT_ON_POSITION)
        sl = entry_price * (1 - SL_PCT_ON_POSITION)
    else:
        tp = entry_price * (1 - TP_PCT_ON_POSITION)
        sl = entry_price * (1 + SL_PCT_ON_POSITION)

    return qty, notional, tp, sl

# ================================================================
# EXCHANGE CLIENT (unchanged)
# ================================================================

class ExchangeClient:
    def __init__(self, key: str, secret: str, testnet: bool = True):
        cfg = {
            "apiKey": key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if testnet:
            cfg["urls"] = {
                "api": {
                    "public": "https://api-testnet.bybit.com",
                    "private": "https://api-testnet.bybit.com",
                }
            }

        self.client = ccxt.bybit(cfg)
        self.lock = threading.Lock()

    def _sym(self, s: str) -> str:
        return CCXT_SYMBOL_MAP[s]

    def get_balance(self) -> float:
        with self.lock:
            bal = self.client.fetch_balance()
        usdt = bal.get("USDT", {})
        return float(usdt.get("total", 0.0))

    def set_leverage(self, symbol: str, lev: int):
        try:
            with self.lock:
                self.client.set_leverage(lev, self._sym(symbol))
        except Exception as e:
            logger.warning(f"{symbol}: Failed to set leverage: {e}")

    def get_position(self, symbol: str):
        try:
            with self.lock:
                p = self.client.fetch_positions([self._sym(symbol)])
        except Exception as e:
            logger.warning(f"{symbol}: fetch_positions error: {e}")
            return None

        for x in p:
            size = float(x.get("contracts") or 0.0)
            if size != 0:
                return x
        return None

    def place_limit(self, symbol, side, price, qty, reduce=False, post=True):
        params = {}
        if reduce:
            params["reduce_only"] = True
        if post:
            params["timeInForce"] = "PostOnly"

        with self.lock:
            return self.client.create_order(
                self._sym(symbol),
                "limit",
                side.lower(),
                qty,
                price,
                params,
            )

    def place_market(self, symbol, side, qty, reduce=False):
        params = {}
        if reduce:
            params["reduce_only"] = True

        with self.lock:
            return self.client.create_order(
                self._sym(symbol),
                "market",
                side.lower(),
                qty,
                None,
                params,
            )

    def place_stop_market(self, symbol, side, qty, stop_price):
        params = {"stopLossPrice": stop_price, "reduce_only": True}
        with self.lock:
            return self.client.create_order(
                self._sym(symbol),
                "market",
                side.lower(),
                qty,
                None,
                params,
            )

    def order_status(self, symbol, oid):
        if not oid:
            return {}
        try:
            with self.lock:
                o = self.client.fetch_order(oid, self._sym(symbol))

            return {
                "status": (o.get("status") or "").lower(),
                "avg_price": o.get("average") or o.get("price"),
                "amount": o.get("amount"),
            }
        except Exception as e:
            logger.warning(f"{symbol}: order_status error: {e}")
            return {}

    def cancel(self, symbol, oid):
        try:
            with self.lock:
                self.client.cancel_order(oid, self._sym(symbol))
            return True
        except Exception as e:
            logger.warning(f"{symbol}: cancel error: {e}")
            return False

    def close_market(self, symbol):
        p = self.get_position(symbol)
        if not p:
            return

        size = float(p.get("contracts") or 0.0)
        if size == 0:
            return

        side = (p.get("side") or "").lower()
        close_side = "sell" if side == "long" else "buy"

        with self.lock:
            try:
                self.client.create_order(
                    self._sym(symbol),
                    "market",
                    close_side,
                    abs(size),
                    None,
                    {"reduce_only": True},
                )
                logger.warning(f"{symbol}: emergency close {size}")
            except Exception as e:
                logger.error(f"{symbol}: emergency close FAILED: {e}")

# ================================================================
# EMERGENCY CLOSE ALL
# ================================================================

def emergency_close_all(exchange: ExchangeClient, symbols: List[str]):
    for s in symbols:
        try:
            exchange.close_market(s)
        except Exception as e:
            logger.error(f"{s}: emergency close error: {e}")

# ================================================================
# MICRO FILTER HELPERS (UPDATED & RELAXED)
# ================================================================

def compute_imbalance(book):
    bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:5]
    asks = sorted(book["asks"].items(), key=lambda x: x[0])[:5]
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    tot = bv + av
    if tot == 0:
        return 0
    return (bv - av) / tot


def micro_delta(book):
    bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:2]
    asks = sorted(book["asks"].items(), key=lambda x: x[0])[:2]
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    tot = bv + av
    if tot == 0:
        return 0
    return (bv - av) / tot


def micro_burst(trades, window_sec=0.15):
    if not trades:
        return 0
    now = trades[-1]["ts"]
    b = 0
    for t in reversed(trades):
        if now - t["ts"] > window_sec:
            break
        b += t["size"] if t["side"] == "buy" else -t["size"]
    return b


def unpredictable(prices):
    if len(prices) < 3:
        return False

    t0, p0 = prices[-1]
    p1s = None
    p3s = None

    for ts, p in reversed(prices):
        dt = t0 - ts
        if p1s is None and dt >= 1:
            p1s = p
        if p3s is None and dt >= 3:
            p3s = p
        if p1s and p3s:
            break

    if p1s and abs(p0 - p1s) / p1s >= VOL_MOVE_PCT_1S:
        return True
    if p3s and abs(p0 - p3s) / p3s >= VOL_MOVE_PCT_3S:
        return True

    return False
# ================================================================
# DECISION ENGINE (UPDATED WITH NEW FILTERS)
# ================================================================

class DecisionEngine:
    def __init__(self, exchange: ExchangeClient, symbols: List[str]):
        self.exchange = exchange
        self.symbols = symbols
        self.open_positions: Dict[str, Position] = {}

        self.pause_until = 0.0
        self.last_trade_ts = 0.0
        self.lock = threading.Lock()

    # ------------------------------------------------------------
    def pause(self, sec: float):
        self.pause_until = max(self.pause_until, now_ts() + sec)

    def is_paused(self) -> bool:
        return now_ts() < self.pause_until

    # ------------------------------------------------------------
    def check_kill(self, equity: float) -> bool:
        global STARTING_EQUITY, bot_killed, kill_alert_sent

        if bot_killed:
            return True

        if STARTING_EQUITY is None:
            STARTING_EQUITY = equity
            return False

        if STARTING_EQUITY <= 0:
            return False

        loss_pct = (STARTING_EQUITY - equity) / STARTING_EQUITY
        if loss_pct >= GLOBAL_KILL_TRIGGER:
            bot_killed = True

            if not kill_alert_sent:
                tg(f"❌ GLOBAL KILL SWITCH\nLoss = {loss_pct*100:.2f}%")
                kill_alert_sent = True

            emergency_close_all(self.exchange, self.symbols)
            self.pause(99999999)
            return True

        return False

    # ------------------------------------------------------------
    def on_close(self, symbol: str, exit_price: float):
        pos = self.open_positions.get(symbol)
        if not pos:
            return

        del self.open_positions[symbol]
        pnl = calc_pnl(pos.side, pos.entry_price, exit_price, pos.qty)
        tg(f"📤 EXIT | {symbol}\nPnL: {pnl} USDT")

    # ------------------------------------------------------------
    def evaluate(self, symbol, book, trades, c5, c1, prices, last_ob_ts):
        with self.lock:
            self._evaluate_locked(symbol, book, trades, c5, c1, prices, last_ob_ts)

    # ------------------------------------------------------------
    def _evaluate_locked(self, symbol, book, trades, c5, c1, prices, last_ob_ts):
        now = now_ts()

        # ---------------------------
        # Balance
        # ---------------------------
        try:
            equity = self.exchange.get_balance()
        except:
            return

        if self.check_kill(equity):
            return

        if self.is_paused():
            return

        if now - self.last_trade_ts < MIN_TRADE_INTERVAL_SEC:
            return

        if now - last_ob_ts > STALE_OB_MAX_SEC:
            self.pause(2)
            return

        if unpredictable(prices):
            self.pause(4)
            return

        if len(c1) < 20 or len(trades) < 10:
            return

        last = c1[-1]
        rng = last["high"] - last["low"]
        if rng <= 0 or last["close"] <= 0:
            return

        # --------------------------------------------
        # Minimum volatility (loosened)
        # --------------------------------------------
        if (rng / last["close"]) < 0.0010:
            return

        # --------------------------------------------
        # Orderbook
        # --------------------------------------------
        if not book["bids"] or not book["asks"]:
            return

        best_bid = max(book["bids"])
        best_ask = min(book["asks"])
        mid = (best_bid + best_ask) / 2
        spread = (best_ask - best_bid) / mid

        if spread > SPREAD_MAX_PCT:
            return

        # --------------------------------------------
        # L2 MICRO FILTERS (LOOSENED)
        # --------------------------------------------
        md = micro_delta(book)
        if abs(md) < 0.02:   # was 0.05
            return

        tb = micro_burst(trades)
        if abs(tb) < 0.15:   # was 0.5
            return

        imb = compute_imbalance(book)
        if abs(imb) < 0.03:  # was 0.05
            return

        # --------------------------------------------
        # RANGE DETECTOR (NEW)
        # --------------------------------------------
        # 1) Bollinger width small → market dead
        closes = [x["close"] for x in c1[-20:]]
        mean = sum(closes) / 20
        variance = sum((c - mean) ** 2 for c in closes) / 20
        stdev = variance ** 0.5

        upper = mean + (2 * stdev)
        lower = mean - (2 * stdev)
        boll_width_pct = (upper - lower) / mean

        if boll_width_pct < 0.0035:  # 0.35%
            return  # range, avoid trading

        # 2) Price stuck in 20s range
        last_20_high = max(x["high"] for x in c1[-20:])
        last_20_low = min(x["low"] for x in c1[-20:])
        if (last_20_high - last_20_low) / mid < 0.0020:
            return

        # --------------------------------------------
        # EMA Trend Filter (20/50)
        # --------------------------------------------
        if len(c1) >= 50:
            closes = [x["close"] for x in c1]
            ema20 = sum(closes[-20:]) / 20
            ema50 = sum(closes[-50:]) / 50

            uptrend = ema20 > ema50
            downtrend = ema20 < ema50

            if tb > 0 and not uptrend:
                return
            if tb < 0 and not downtrend:
                return

        # --------------------------------------------
        # Volume spike (safety)
        # --------------------------------------------
        last_vol = last["volume"]
        avg_vol = sum(x["volume"] for x in c1[-10:]) / 10

        if last_vol < 1.3 * avg_vol:
            return

        # --------------------------------------------
        # Bollinger confirmation (NEW)
        # --------------------------------------------
        # bounce or break logic
        price = last["close"]

        if tb > 0 and price < lower:    # bullish bounce from lower band
            pass
        elif tb < 0 and price > upper:  # bearish rejection near upper band
            pass
        else:
            # small wiggle inside → ignore
            return

        # --------------------------------------------
        # Final Side
        # --------------------------------------------
        side = "Buy" if tb > 0 else "Sell"

        qty, notional, tp, sl = compute_position_and_prices(equity, mid, side.lower())

        if qty <= 0:
            return

        sl_dist = abs(mid - sl) / mid
        if sl_dist < MIN_SL_DIST_PCT:
            return

        # --------------------------------------------
        # ENTRY ORDER
        # --------------------------------------------
        try:
            entry = self.exchange.place_limit(symbol, side, mid, qty, post=True)
            oid = entry.get("id")
            filled = False
            fill_price = mid
            stime = now_ts()

            while now_ts() - stime < POST_ONLY_TIMEOUT:
                st = self.exchange.order_status(symbol, oid)
                if st.get("status") in ("closed", "filled"):
                    filled = True
                    fill_price = float(st.get("avg_price") or fill_price)
                    break
                time.sleep(0.1)

            if not filled:
                self.exchange.cancel(symbol, oid)
                mkt = self.exchange.place_market(symbol, side, qty)
                fill_price = float(mkt.get("average") or fill_price)

        except Exception as e:
            tg(f"❌ ENTRY FAILED {symbol}")
            self.pause(3)
            return

        # --------------------------------------------
        # TP / SL
        # --------------------------------------------
        reduce_side = "Sell" if side == "Buy" else "Buy"

        try:
            self.exchange.place_limit(symbol, reduce_side, tp, qty, reduce=True, post=False)
            self.exchange.place_stop_market(symbol, reduce_side, qty, sl)
        except Exception:
            tg(f"❌ TP/SL FAIL {symbol}")
            self.exchange.close_market(symbol)
            return

        self.open_positions[symbol] = Position(symbol, side, qty, fill_price, tp, sl, notional)
        self.last_trade_ts = now_ts()

        tg(
            f"📌 ENTRY | {symbol}\nSide: {side}\nEntry: {fill_price}\nTP: {tp}\nSL: {sl}"
        )
# ================================================================
# MARKET WORKER (STABLE L2 + TRADES, NO RECONNECT SPAM)
# ================================================================

# Safer WS settings (overrides older values above)
WS_SILENCE_SEC = 20.0      # we only log if silent; no forced close
STALE_OB_MAX_SEC = 8.0     # orderbook can be slightly old before we skip

class MarketWorker(threading.Thread):
    """
    - Connects to Bybit public WS v5 (mainnet or testnet)
    - Subscribes to:
         orderbook.25.<SYMBOL>
         publicTrade.<SYMBOL>
    - Maintains:
         Level-2 orderbook
         Trades buffer
         1s & 5s candles
         Price timeline
    - Periodically calls DecisionEngine.evaluate() for this symbol
    """

    def __init__(self, symbol: str, engine: DecisionEngine, exchange: ExchangeClient, testnet: bool = True):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.engine = engine
        self.exchange = exchange
        self.testnet = testnet

        # IMPORTANT: PUBLIC_WS_MAINNET / TESTNET must be:
        #   wss://stream.bybit.com/v5/public
        #   wss://stream-testnet.bybit.com/v5/public
        self.ws_url = PUBLIC_WS_TESTNET if testnet else PUBLIC_WS_MAINNET
        self.topic_ob = f"orderbook.1.{symbol}"
        self.topic_trade = f"publicTrade.{symbol}"

        self.ws: Optional[websocket.WebSocketApp] = None
        self._stop = threading.Event()

        # L2 orderbook
        self.book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.last_ob_ts: float = 0.0

        # Trades + prices
        self.trades: deque = deque(maxlen=1000)
        self.prices: deque = deque(maxlen=1000)

        # Candles
        self.c1: List[Dict[str, Any]] = []
        self.c5: List[Dict[str, Any]] = []

        # Eval timing
        self.last_eval: float = 0.0
        self.last_msg_ts: float = now_ts()

    # ------------------------------------------------------------
    # WEBSOCKET CALLBACKS
    # ------------------------------------------------------------
    def on_open(self, ws):
        logger.info(f"{self.symbol}: WS opened.")
        try:
            sub = {"op": "subscribe", "args": [self.topic_ob, self.topic_trade]}
            ws.send(json.dumps(sub))
            logger.info(f"{self.symbol}: subscribed to L2 + trades")
        except Exception as e:
            logger.error(f"{self.symbol}: subscribe error: {e}")

    def on_message(self, ws, message: str):
        self.last_msg_ts = now_ts()
        try:
            msg = json.loads(message)
        except Exception:
            return

        try:
            topic = msg.get("topic", "")
            if topic.startswith("orderbook"):
                self.handle_ob(msg)
            elif topic.startswith("publicTrade"):
                self.handle_trades(msg)
        except Exception as e:
            logger.exception(f"{self.symbol}: on_message error: {e}")

    def on_error(self, ws, error):
        logger.error(f"{self.symbol}: WS error: {error}")

    def on_close(self, ws, code, msg):
        logger.warning(f"{self.symbol}: WS closed: {code} {msg}")

    # ------------------------------------------------------------
    # CONNECT / RECONNECT LOOP (BACKOFF, NO FORCE CLOSE)
    # ------------------------------------------------------------
    def connect(self):
        backoff = 1

        while not self._stop.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )

                logger.info(f"{self.symbol}: WS connecting...")
                # run_forever blocks until socket really closes or error
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.error(f"{self.symbol}: WS crash: {e}")

            if self._stop.is_set():
                break

            logger.warning(f"{self.symbol}: WS reconnecting in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)  # exponential backoff

        logger.info(f"{self.symbol}: connect() loop exit")

    def close(self):
        self._stop.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    # ------------------------------------------------------------
    # ORDERBOOK HANDLER (SNAPSHOT + DELTA)
    # ------------------------------------------------------------
    def handle_ob(self, msg: Dict[str, Any]) -> None:
        try:
            data = msg.get("data")
            if not data:
                return

            item = data[0] if isinstance(data, list) else data
            typ = msg.get("type") or item.get("type", "")

            # snapshot
            if typ == "snapshot" or ("bids" in item and "asks" in item):
                self.book["bids"].clear()
                self.book["asks"].clear()

                for b in item.get("bids", []):
                    p, q = float(b[0]), float(b[1])
                    if q > 0:
                        self.book["bids"][p] = q

                for a in item.get("asks", []):
                    p, q = float(a[0]), float(a[1])
                    if q > 0:
                        self.book["asks"][p] = q

                self.last_ob_ts = float(item.get("ts", now_ts()))

            # delta
            else:
                self.last_ob_ts = float(item.get("ts", now_ts()))

                def apply_side(lst, side: str):
                    bookside = self.book["bids"] if side == "buy" else self.book["asks"]
                    for x in lst:
                        p, q = float(x[0]), float(x[1])
                        if q <= 0:
                            bookside.pop(p, None)
                        else:
                            bookside[p] = q

                for key in ("delete", "update", "insert"):
                    part = item.get(key)
                    if isinstance(part, dict):
                        if "bids" in part:
                            apply_side(part["bids"], "buy")
                        if "asks" in part:
                            apply_side(part["asks"], "sell")

            # update mid-price history
            if self.book["bids"] and self.book["asks"]:
                best_bid = max(self.book["bids"])
                best_ask = min(self.book["asks"])
                mid = (best_bid + best_ask) / 2.0
                self.prices.append((now_ts(), mid))

        except Exception as e:
            logger.exception(f"{self.symbol}: OB error: {e}")

    # ------------------------------------------------------------
    # TRADES HANDLER
    # ------------------------------------------------------------
    def handle_trades(self, msg: Dict[str, Any]) -> None:
        try:
            data = msg.get("data")
            if not data:
                return
            arr = data if isinstance(data, list) else [data]

            for item in arr:
                price = float(item.get("price") or item.get("p", 0.0))
                size = float(item.get("size") or item.get("q", 0.0))
                side = (item.get("side") or "buy").lower()

                ts = float(item.get("ts") or item.get("trade_time_ms") or now_ts())
                if ts > 1e12:
                    ts /= 1000.0

                self.trades.append({"ts": ts, "price": price, "size": size, "side": side})
                self.prices.append((now_ts(), price))

            self.update_candles()

        except Exception as e:
            logger.exception(f"{self.symbol}: trades error: {e}")

    # ------------------------------------------------------------
    # CANDLE BUILDER (1s + 5s)
    # ------------------------------------------------------------
    def update_candles(self) -> None:
        try:
            if not self.trades:
                return

            now = now_ts()
            trades = list(self.trades)
            c1_map: Dict[int, Dict[str, Any]] = {}

            for t in trades:
                sec = int(t["ts"])
                if sec < int(now) - 120:
                    continue

                if sec not in c1_map:
                    c1_map[sec] = {
                        "ts": sec,
                        "open": t["price"],
                        "high": t["price"],
                        "low": t["price"],
                        "close": t["price"],
                        "volume": t["size"],
                    }
                else:
                    c = c1_map[sec]
                    c["high"] = max(c["high"], t["price"])
                    c["low"] = min(c["low"], t["price"])
                    c["close"] = t["price"]
                    c["volume"] += t["size"]

            self.c1 = [c1_map[k] for k in sorted(c1_map)][-60:]

            c5_map: Dict[int, Dict[str, Any]] = {}
            for c in self.c1:
                bucket = (c["ts"] // 5) * 5
                if bucket not in c5_map:
                    c5_map[bucket] = dict(c)
                else:
                    agg = c5_map[bucket]
                    agg["high"] = max(agg["high"], c["high"])
                    agg["low"] = min(agg["low"], c["low"])
                    agg["close"] = c["close"]
                    agg["volume"] += c["volume"]

            self.c5 = [c5_map[k] for k in sorted(c5_map)][-60:]

        except Exception as e:
            logger.exception(f"{self.symbol}: candle error: {e}")

    # ------------------------------------------------------------
    # EVALUATION THROTTLE
    # ------------------------------------------------------------
    def maybe_eval(self) -> None:
        try:
            now = now_ts()
            if now - self.last_eval < SCAN_INTERVAL:
                return

            if not self.book["bids"] or not self.book["asks"]:
                return

            prices_list = list(self.prices)
            trades_list = list(self.trades)
            self.update_candles()

            self.engine.evaluate(
                self.symbol,
                self.book,
                trades_list,
                self.c5,
                self.c1,
                prices_list,
                self.last_ob_ts,
            )

            self.last_eval = now

        except Exception as e:
            logger.exception(f"{self.symbol}: eval error: {e}")

    # ------------------------------------------------------------
    # MAIN RUN LOOP
    # ------------------------------------------------------------
    def run(self) -> None:
        # Start WS in background thread
        th = threading.Thread(target=self.connect, daemon=True)
        th.start()

        try:
            while not self._stop.is_set():
                # Only log if WS quiet; no forced close (avoids NoneType sock + spam)
                if now_ts() - self.last_msg_ts > WS_SILENCE_SEC:
                    logger.warning(f"{self.symbol}: WS silent for {WS_SILENCE_SEC}s (no force reconnect)")
                    # reset timer so we don't log every 0.2s
                    self.last_msg_ts = now_ts()

                self.maybe_eval()
                time.sleep(0.2)

        except Exception as e:
            logger.exception(f"{self.symbol}: worker crash: {e}")
        finally:
            self.close()


# ================================================================
# MAIN
# ================================================================

def main() -> None:
    global STARTING_EQUITY, bot_killed, kill_alert_sent

    logger.info("Starting Bybit scalper bot…")
    tg("🟢 Bot started")

    # Heartbeat every 10 minutes
    start_heartbeat()

    # Exchange client
    exchange = ExchangeClient(API_KEY, API_SECRET, testnet=TESTNET)

    # Close any leftover positions from previous run
    emergency_close_all(exchange, SYMBOLS)

    # Kill-switch starting equity
    try:
        STARTING_EQUITY = exchange.get_balance()
    except Exception as e:
        logger.warning(f"Failed to fetch starting equity: {e}")
        STARTING_EQUITY = 0.0

    bot_killed = False
    kill_alert_sent = False

    logger.info(
        f"Kill-switch starting equity = {STARTING_EQUITY:.4f}, "
        f"trigger = {GLOBAL_KILL_TRIGGER * 100:.1f}%"
    )
    tg(
        f"📊 Starting equity: {STARTING_EQUITY:.4f} USDT\n"
        f"Kill-switch at {GLOBAL_KILL_TRIGGER * 100:.1f}% loss."
    )

    # Set leverage for all symbols
    for s in SYMBOLS:
        try:
            exchange.set_leverage(s, LEVERAGE)
        except Exception as e:
            logger.warning(f"{s}: leverage set failed: {e}")

    # Decision engine (shared brain)
    engine = DecisionEngine(exchange, SYMBOLS)

    # Start workers: one WS + L2 + eval per symbol
    workers: List[MarketWorker] = []
    for s in SYMBOLS:
        w = MarketWorker(s, engine, exchange, TESTNET)
        w.start()
        workers.append(w)

    logger.info("Workers started. Bot running… (Ctrl+C to stop)")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Stopping workers…")
        tg("🛑 Bot stopping (KeyboardInterrupt).")
        for w in workers:
            try:
                w.close()
            except Exception:
                pass
        time.sleep(1.0)


if __name__ == "__main__":
    main()
