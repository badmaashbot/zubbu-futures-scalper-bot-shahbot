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
# CONFIG
# ================================================================

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")

if not API_KEY or not API_SECRET:
    raise Exception("API keys missing. Please set BYBIT_API_KEY and BYBIT_API_SECRET.")

# True = testnet, False = live
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

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

SCAN_INTERVAL = 5.0               # seconds between decision scans
LEVERAGE = 3
MARGIN_FRACTION = 0.95

TP_PCT_ON_POSITION = 0.01         # 1% TP
SL_PCT_ON_POSITION = 0.005        # 0.5% SL

GLOBAL_KILL_TRIGGER = 0.05        # 5% equity loss
STARTING_EQUITY: Optional[float] = None
bot_killed: bool = False
kill_alert_sent: bool = False     # only one Telegram alert for kill-switch

MAX_CONCURRENT_POSITIONS = 1

POST_ONLY_TIMEOUT = 3.0
VOL_MOVE_PCT_1S = 0.004
VOL_MOVE_PCT_3S = 0.008

IMBALANCE_LEVELS = 5
IMBALANCE_THRESHOLD = 0.05

STALE_OB_MAX_SEC = 2.0
LATENCY_MAX_SEC = 1.5
MIN_SL_DIST_PCT = 0.0005
MIN_TRADE_INTERVAL_SEC = 5.0
SPREAD_MAX_PCT = 0.001
ERROR_PAUSE_SEC = 10.0

# WebSocket safety: if no messages for this many seconds, treat as dead
WS_SILENCE_SEC = 3.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("scalper_bot")


def now_ts() -> float:
    return time.time()


# ================================================================
# TELEGRAM (NO SPAM)
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
    """
    Telegram helper:
    - Skips if no token/chat.
    - Rate-limited.
    - Duplicate-suppressed.
    - Never crashes the bot.
    """
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
        logger.warning(f"Telegram send failed: {e}")


# ================================================================
# HEARTBEAT (EVERY 10 MIN)
# ================================================================

def heartbeat_loop() -> None:
    while True:
        tg("💓 Bot running (heartbeat)")
        time.sleep(600)  # 10 minutes


def start_heartbeat() -> None:
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

    if side == "buy":
        tp = entry_price * (1.0 + TP_PCT_ON_POSITION)
        sl = entry_price * (1.0 - SL_PCT_ON_POSITION)
    else:
        tp = entry_price * (1.0 - TP_PCT_ON_POSITION)
        sl = entry_price * (1.0 + SL_PCT_ON_POSITION)

    return qty, notional, tp, sl


# ================================================================
# EXCHANGE CLIENT
# ================================================================

class ExchangeClient:
    def __init__(self, key: str, secret: str, testnet: bool = True):
        cfg: Dict[str, Any] = {
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

    def set_leverage(self, symbol: str, lev: int) -> None:
        try:
            with self.lock:
                self.client.set_leverage(lev, self._sym(symbol))
        except Exception as e:
            logger.warning(f"{symbol}: leverage set failed: {e}")

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
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

    def place_limit(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        reduce: bool = False,
        post: bool = True,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
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

    def place_market(
        self,
        symbol: str,
        side: str,
        qty: float,
        reduce: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if reduce:
            params["reduce_only"] = True
        with self.lock:
            # price must be None for market orders in ccxt
            return self.client.create_order(
                self._sym(symbol),
                "market",
                side.lower(),
                qty,
                None,
                params,
            )

    def place_stop_market(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
    ) -> Dict[str, Any]:
        # Bybit stop-loss via ccxt: market with stopLossPrice param
        params: Dict[str, Any] = {
            "stopLossPrice": stop_price,
            "reduce_only": True,
        }
        with self.lock:
            return self.client.create_order(
                self._sym(symbol),
                "market",
                side.lower(),
                qty,
                None,
                params,
            )

    def order_status(self, symbol: str, oid: Optional[str]) -> Dict[str, Any]:
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
            logger.warning(f"{symbol}: fetch_order error: {e}")
            return {}

    def cancel(self, symbol: str, oid: str) -> bool:
        try:
            with self.lock:
                self.client.cancel_order(oid, self._sym(symbol))
            return True
        except Exception as e:
            logger.warning(f"{symbol}: cancel_order error: {e}")
            return False

    def close_market(self, symbol: str) -> None:
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
                logger.warning(f"{symbol}: emergency market close size={size}")
            except Exception as e:
                logger.error(f"{symbol}: emergency close failed: {e}")


# ================================================================
# EMERGENCY CLOSE ALL
# ================================================================

def emergency_close_all(exchange: ExchangeClient, symbols: List[str]) -> None:
    for s in symbols:
        try:
            exchange.close_market(s)
        except Exception as e:
            logger.error(f"{s}: emergency close error: {e}")


# ================================================================
# SPOOF / WHALE DETECTORS
# ================================================================

class SpoofDetector:
    def __init__(self, window: float = 1.0, repeat: int = 3):
        self.window = window
        self.repeat = repeat
        self.data: Dict[Tuple[str, float], deque] = defaultdict(deque)

    def on_event(self, side: str, price: float, etype: str, ts: float) -> None:
        k = (side, float(price))
        dq = self.data[k]
        dq.append((etype, ts))
        while dq and ts - dq[0][1] > self.window:
            dq.popleft()

    def check(self, side: str, price: float) -> bool:
        dq = self.data.get((side, float(price)), deque())
        cancels = sum(1 for x, _ in dq if x == "cancel")
        return cancels >= self.repeat


class WhaleCancelDetector:
    def __init__(self, th: float, window: float = 5.0):
        self.th = th
        self.window = window
        self.data: Dict[float, deque] = defaultdict(deque)

    def on_event(self, price: float, size: float, etype: str, ts: float) -> None:
        if size < self.th:
            return
        dq = self.data[float(price)]
        dq.append((etype, ts, size))
        while dq and ts - dq[0][1] > self.window:
            dq.popleft()

    def check(self, price: float) -> bool:
        dq = self.data.get(float(price), deque())
        if not dq:
            return False
        cancels = sum(1 for t, _, _ in dq if t == "cancel")
        adds = sum(1 for t, _, _ in dq if t == "new")
        if adds + cancels == 0:
            return False
        return (cancels / (adds + cancels)) > 0.3


# ================================================================
# MICRO FILTER HELPERS
# ================================================================

def compute_imbalance(book: Dict[str, Dict[float, float]]) -> float:
    bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:IMBALANCE_LEVELS]
    asks = sorted(book["asks"].items(), key=lambda x: x[0])[:IMBALANCE_LEVELS]
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    tot = bv + av
    if tot == 0:
        return 0.0
    return (bv - av) / tot


def micro_delta(book: Dict[str, Dict[float, float]]) -> float:
    bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:2]
    asks = sorted(book["asks"].items(), key=lambda x: x[0])[:2]
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    if bv + av == 0:
        return 0.0
    return (bv - av) / (bv + av)


def micro_burst(trades: List[Dict[str, Any]], window_sec: float = 0.15) -> float:
    if not trades:
        return 0.0
    now = trades[-1]["ts"]
    b = 0.0
    for t in reversed(trades):
        if now - t["ts"] > window_sec:
            break
        if t["side"] == "buy":
            b += t["size"]
        else:
            b -= t["size"]
    return b


def liquidity_gap(book: Dict[str, Dict[float, float]], th: float = 0.15) -> bool:
    bids = sum(x for _, x in sorted(book["bids"].items(), reverse=True)[:5])
    asks = sum(x for _, x in sorted(book["asks"].items())[:5])
    if bids == 0 or asks == 0:
        return True
    return bids < th * asks or asks < th * bids


def unpredictable(prices: List[Tuple[float, float]]) -> bool:
    if len(prices) < 3:
        return False
    t0, p0 = prices[-1]
    p1s = None
    p3s = None
    for ts, p in reversed(prices):
        dt = t0 - ts
        if p1s is None and dt >= 1.0:
            p1s = p
        if p3s is None and dt >= 3.0:
            p3s = p
        if p1s and p3s:
            break
    if p1s and abs(p0 - p1s) / p1s >= VOL_MOVE_PCT_1S:
        return True
    if p3s and abs(p0 - p3s) / p3s >= VOL_MOVE_PCT_3S:
        return True
    return False


# ================================================================
# DECISION ENGINE
# ================================================================

class DecisionEngine:
    def __init__(self, exchange: ExchangeClient, symbols: List[str]):
        self.exchange = exchange
        self.symbols = symbols
        self.open_positions: Dict[str, Position] = {}
        self.spoof = SpoofDetector()
        self.whale = {s: WhaleCancelDetector(1.0) for s in symbols}
        self.pause_until = 0.0
        self.last_trade_ts = 0.0
        self.lock = threading.Lock()

    def pause(self, sec: float) -> None:
        self.pause_until = max(self.pause_until, now_ts() + sec)
        logger.warning(f"Engine paused for {sec:.1f}s")

    def is_paused(self) -> bool:
        return now_ts() < self.pause_until

    def check_kill(self, equity: float) -> bool:
        global STARTING_EQUITY, bot_killed, kill_alert_sent

        if bot_killed:
            return True

        if STARTING_EQUITY is None:
            STARTING_EQUITY = equity
            logger.info(f"Recorded STARTING_EQUITY={STARTING_EQUITY:.4f}")
            return False

        loss_pct = (STARTING_EQUITY - equity) / STARTING_EQUITY if STARTING_EQUITY > 0 else 0.0
        if loss_pct >= GLOBAL_KILL_TRIGGER:
            bot_killed = True
            if not kill_alert_sent:
                tg(f"❌ GLOBAL KILL SWITCH TRIGGERED\nEquity loss={loss_pct*100:.2f}%")
                kill_alert_sent = True
            logger.error("Global kill-switch active, closing all positions & pausing.")
            emergency_close_all(self.exchange, self.symbols)
            self.pause(99999999)
            return True

        return False

    def on_close(self, symbol: str, exit_price: float) -> None:
        pos = self.open_positions.get(symbol)
        if not pos:
            return
        del self.open_positions[symbol]

        dist_tp = abs(exit_price - pos.tp_price)
        dist_sl = abs(exit_price - pos.sl_price)
        hit = "TP" if dist_tp < dist_sl else "SL"

        pnl = calc_pnl(pos.side, pos.entry_price, exit_price, pos.qty)
        tg(f"📤 EXIT | {symbol}\nReason: {hit}\nPnL: {pnl} USDT")

    def evaluate(
        self,
        symbol: str,
        book: Dict[str, Dict[float, float]],
        trades: List[Dict[str, Any]],
        c5: List[Dict[str, Any]],
        c1: List[Dict[str, Any]],
        prices: List[Tuple[float, float]],
        last_ob_ts: float,
    ) -> None:
        with self.lock:
            self._evaluate_locked(symbol, book, trades, c5, c1, prices, last_ob_ts)

    def _evaluate_locked(
        self,
        symbol: str,
        book: Dict[str, Dict[float, float]],
        trades: List[Dict[str, Any]],
        c5: List[Dict[str, Any]],
        c1: List[Dict[str, Any]],
        prices: List[Tuple[float, float]],
        last_ob_ts: float,
    ) -> None:
        now = now_ts()
        try:
            equity = self.exchange.get_balance()
        except Exception as e:
            logger.warning(f"{symbol}: failed to fetch balance: {e}")
            return

        if self.check_kill(equity):
            return

        if self.is_paused():
            return

        if self.last_trade_ts and (now - self.last_trade_ts) < MIN_TRADE_INTERVAL_SEC:
            return

        if now - last_ob_ts > STALE_OB_MAX_SEC:
            logger.warning(f"{symbol}: stale orderbook, pausing briefly.")
            self.pause(2.0)
            return

        if unpredictable(prices):
            logger.warning(f"{symbol}: unpredictable micro-move, short pause.")
            self.pause(5.0)
            return

        if len(c1) < 5 or len(trades) < 10:
            return

        last = c1[-1]
        rng = last["high"] - last["low"]
        if last["close"] == 0:
            return
        if (rng / last["close"]) < 0.002:
            return

        bids = book["bids"]
        asks = book["asks"]
        if not bids or not asks:
            return

        best_bid = max(bids)
        best_ask = min(asks)
        mid = (best_bid + best_ask) / 2.0
        spread = (best_ask - best_bid) / mid
        if spread > SPREAD_MAX_PCT:
            return

        md = micro_delta(book)
        if abs(md) < 0.05:
            return

        tb = micro_burst(trades)
        if abs(tb) < 0.5:
            return

        if liquidity_gap(book):
            return

        imb = compute_imbalance(book)
        if abs(imb) < IMBALANCE_THRESHOLD:
            return

        if self.spoof.check("bid", best_bid) or self.spoof.check("ask", best_ask):
            return
        if self.whale[symbol].check(best_bid):
            return

        direction = 1 if tb > 0 else -1
        side = "Buy" if direction == 1 else "Sell"

        qty, notional, tp, sl = compute_position_and_prices(equity, mid, side.lower())
        if qty <= 0 or notional <= 0:
            return

        sl_dist = abs(mid - sl) / mid
        if sl_dist < MIN_SL_DIST_PCT:
            return

        # ENTRY
        try:
            entry = self.exchange.place_limit(symbol, side, mid, qty, reduce=False, post=True)
            oid = entry.get("id")
            start_t = now_ts()
            filled = False
            fill_price = mid

            while now_ts() - start_t < POST_ONLY_TIMEOUT:
                st = self.exchange.order_status(symbol, oid)
                status = st.get("status", "")
                if status in ("closed", "filled"):
                    filled = True
                    fill_price = float(st.get("avg_price", mid) or mid)
                    break
                time.sleep(0.1)

            if not filled:
                if oid:
                    self.exchange.cancel(symbol, oid)
                mkt = self.exchange.place_market(symbol, side, qty, reduce=False)
                fill_price = float(mkt.get("average", mid) or mid)
        except Exception as e:
            logger.exception(f"{symbol}: entry failed: {e}")
            tg(f"❌ Entry failed {symbol}: {e}")
            self.pause(ERROR_PAUSE_SEC)
            return

        # TP & SL
        try:
            reduce_side = "Sell" if side == "Buy" else "Buy"
            self.exchange.place_limit(symbol, reduce_side, tp, qty, reduce=True, post=False)
            self.exchange.place_stop_market(symbol, reduce_side, qty, sl)
        except Exception as e:
            logger.exception(f"{symbol}: TP/SL placement failed: {e}")
            tg(f"❌ TP/SL failed {symbol}: {e}")
            self.exchange.close_market(symbol)
            self.pause(ERROR_PAUSE_SEC)
            return

        pos = Position(symbol, side, qty, fill_price, tp, sl, notional)
        self.open_positions[symbol] = pos
        self.last_trade_ts = now_ts()
        tg(f"📌 ENTRY | {symbol}\nSide: {side}\nEntry: {fill_price}\nTP: {tp}\nSL: {sl}")


# ================================================================
# MARKET WORKER (FULL)
# ================================================================

class MarketWorker(threading.Thread):
    """
    MarketWorker:
    - Connects to Bybit public websocket v5.
    - Subscribes to orderbook.25.<SYMBOL> and trade.<SYMBOL>
    - Maintains in-memory orderbook, trade buffer, 1s & 5s candles.
    - Calls DecisionEngine.evaluate periodically (maybe_eval).
    """

    def __init__(self, symbol: str, engine: DecisionEngine, exchange: ExchangeClient, testnet: bool = True):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.engine = engine
        self.exchange = exchange
        self.testnet = testnet

        self.ws_url = PUBLIC_WS_TESTNET if testnet else PUBLIC_WS_MAINNET
        self.topic_ob = f"orderbook.25.{symbol}"
        self.topic_trade = f"publicTrade.{symbol}"

        self.ws: Optional[websocket.WebSocketApp] = None
        self._stop = threading.Event()

        # book representation: price->size
        self.book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.last_ob_ts: float = 0.0

        # trades buffer (deque of dicts with ts, price, size, side)
        self.trades: deque = deque(maxlen=1000)

        # prices timeline for unpredictability checks: list of (ts, mid)
        self.prices: deque = deque(maxlen=1000)

        # candles
        self.c1: List[Dict[str, Any]] = []
        self.c5: List[Dict[str, Any]] = []

        # last eval timestamp
        self.last_eval = 0.0

        # for ws inactivity detection
        self.last_msg_ts = now_ts()

    # Websocket callbacks
    def on_open(self, ws) -> None:
        logger.info(f"{self.symbol}: WS opened, subscribing...")
        try:
            sub = {"op": "subscribe", "args": [self.topic_ob, self.topic_trade]}
            ws.send(json.dumps(sub))
            logger.info(f"{self.symbol}: subscribed to {self.topic_ob} & {self.topic_trade}")
        except Exception as e:
            logger.error(f"{self.symbol}: subscribe failed: {e}")

    def on_message(self, ws, message: str) -> None:
        self.last_msg_ts = now_ts()
        try:
            msg = json.loads(message)
        except Exception:
            # Some servers may send ping/pong or plain strings
            return

        # Bybit v5 sends {"topic": "..", "type": "snapshot"/"delta", "data": [...]} typical
        try:
            topic = msg.get("topic") or msg.get("type") or ""
            # Trade messages may come with "topic":"trade.BTCUSDT" and "data":[{...}, ...]
            if isinstance(msg, dict) and ("topic" in msg and msg.get("topic", "").startswith("orderbook")):
                self.handle_ob(msg)
            elif isinstance(msg, dict) and ("topic" in msg and msg.get("topic", "").startswith("publicTrade")):
                self.handle_trades(msg)
            else:
                # ignore other messages (info/pong)
                pass
        except Exception as e:
            logger.exception(f"{self.symbol}: on_message handling failed: {e}")

    def on_error(self, ws, error) -> None:
        logger.error(f"{self.symbol}: WS error: {error}")

    def on_close(self, ws, close_status_code, close_msg) -> None:
        logger.warning(f"{self.symbol}: WS closed: code={close_status_code} msg={close_msg}")

    def connect(self) -> None:
        # Create WebSocketApp
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        # Run in this thread (blocking) until stop/exception; reconnect on failures
        while not self._stop.is_set():
            try:
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.exception(f"{self.symbol}: run_forever exception: {e}")
            # short backoff before reconnect
            if not self._stop.is_set():
                logger.info(f"{self.symbol}: reconnecting websocket in 1s...")
                time.sleep(1.0)

    def close(self) -> None:
        self._stop.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    # --- Orderbook handling (error-free) ---
    def handle_ob(self, msg: Dict[str, Any]) -> None:
        """
        Parse orderbook snapshot and delta messages and update self.book.
        Works with Bybit v5 'orderbook.25.SYMBOL' snapshot/delta structure.
        """
        try:
            data = msg.get("data")
            if not data:
                return
            # data is usually a list with one item struct
            item = data[0] if isinstance(data, list) else data
            typ = msg.get("type") or item.get("type") or ""

            # snapshot contains bids/asks arrays
            if typ == "snapshot" or "bids" in item and "asks" in item:
                bids = item.get("bids", [])
                asks = item.get("asks", [])
                # bids/asks are lists of [price, size] or dicts
                self.book["bids"].clear()
                self.book["asks"].clear()
                for b in bids:
                    if isinstance(b, (list, tuple)) and len(b) >= 2:
                        price = float(b[0])
                        size = float(b[1])
                    elif isinstance(b, dict):
                        price = float(b.get("price", 0.0))
                        size = float(b.get("size", 0.0))
                    else:
                        continue
                    if size > 0:
                        self.book["bids"][price] = size
                for a in asks:
                    if isinstance(a, (list, tuple)) and len(a) >= 2:
                        price = float(a[0])
                        size = float(a[1])
                    elif isinstance(a, dict):
                        price = float(a.get("price", 0.0))
                        size = float(a.get("size", 0.0))
                    else:
                        continue
                    if size > 0:
                        self.book["asks"][price] = size

                ts = float(item.get("ts", now_ts()))
                self.last_ob_ts = ts
                # update price timeline
                try:
                    best_bid = max(self.book["bids"])
                    best_ask = min(self.book["asks"])
                    mid = (best_bid + best_ask) / 2.0
                    self.prices.append((now_ts(), mid))
                except Exception:
                    pass

            elif typ == "delta" or "delete" in item or "update" in item or "insert" in item:
                # delta messages may contain 'delete', 'update', 'insert' lists
                ts = float(item.get("ts", now_ts()))
                self.last_ob_ts = ts

                # helper to apply changes
                def apply_list(lst, side):
                    for entry in lst:
                        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                            price = float(entry[0])
                            size = float(entry[1])
                        elif isinstance(entry, dict):
                            price = float(entry.get("price", 0.0))
                            size = float(entry.get("size", 0.0))
                        else:
                            continue
                        book_side = self.book["bids"] if side == "buy" else self.book["asks"]
                        if size <= 0:
                            if price in book_side:
                                del book_side[price]
                        else:
                            book_side[price] = size

                # Bybit delta keys can be 'delete','update','insert' with nested bids/asks
                for k in ("delete", "update", "insert"):
                    parts = item.get(k, {})
                    if isinstance(parts, dict):
                        bids = parts.get("bids", [])
                        asks = parts.get("asks", [])
                        if bids:
                            apply_list(bids, "buy")
                        if asks:
                            apply_list(asks, "sell")
                    elif isinstance(parts, list):
                        # sometimes it's a list of lists: [ [price,size], ... ]
                        # assume it's bids or asks not specified - try to infer
                        # try treating as bids first: update bids only
                        for entry in parts:
                            # can't know side, skip
                            pass

                # update price timeline if possible
                try:
                    best_bid = max(self.book["bids"]) if self.book["bids"] else 0.0
                    best_ask = min(self.book["asks"]) if self.book["asks"] else 0.0
                    if best_bid and best_ask:
                        mid = (best_bid + best_ask) / 2.0
                        self.prices.append((now_ts(), mid))
                except Exception:
                    pass

        except Exception as e:
            logger.exception(f"{self.symbol}: handle_ob failed: {e}")

    # --- Trade handling ---
    def handle_trades(self, msg: Dict[str, Any]) -> None:
        """
        Parse trade messages and append to self.trades.
        Bybit v5 trade msg usually has data: [ { "id", "price","size","side","ts" }, ... ]
        """
        try:
            data = msg.get("data")
            if not data:
                return
            arr = data if isinstance(data, list) else [data]
            for item in arr:
                # item may be a dict with price,size,side,trade_time_ms/ts
                price = float(item.get("price") or item.get("p") or 0.0)
                size = float(item.get("size") or item.get("q") or item.get("qty") or 0.0)
                side = (item.get("side") or item.get("s") or "").lower()
                # normalize side values
                if side in ("buy", "sell"):
                    pass
                elif side in ("b", "s", "buyer", "seller"):
                    side = "buy" if "b" in side else "sell"
                else:
                    # fallback via price movement or leave unknown
                    side = "buy"

                ts = float(item.get("ts") or item.get("trade_time_ms") or now_ts())
                trade = {"ts": ts / 1000.0 if ts > 1e12 else ts, "price": price, "size": size, "side": side}
                self.trades.append(trade)
                # update price series
                try:
                    # if we have a recent mid, append trade price as a mid substitute
                    self.prices.append((now_ts(), price))
                except Exception:
                    pass

            # update candles from trades
            self.update_candles()
        except Exception as e:
            logger.exception(f"{self.symbol}: handle_trades failed: {e}")

    # --- Candle building ---
    def update_candles(self) -> None:
        """
        Build / refresh 1s and 5s candles from self.trades.
        Keep c1 (list of 1s candles) and c5 (list of 5s candles).
        Candle structure: {"ts": ts_start, "open": , "high": , "low": , "close": , "volume": }
        """
        try:
            if not self.trades:
                return

            # Convert trades deque to list for processing
            trades = list(self.trades)
            # Normalize timestamps to seconds (floats)
            for t in trades:
                if t["ts"] > 1e12:
                    t["ts"] = t["ts"] / 1000.0

            # Build 1s candles (last N seconds until now)
            now = now_ts()
            # We will build up to last 60 1s candles
            c1_map: Dict[int, Dict[str, Any]] = {}
            for t in trades:
                sec = int(t["ts"])
                if sec < int(now) - 120:
                    continue
                entry = c1_map.get(sec)
                if not entry:
                    c1_map[sec] = {"ts": sec, "open": t["price"], "high": t["price"], "low": t["price"], "close": t["price"], "volume": t["size"]}
                else:
                    entry["high"] = max(entry["high"], t["price"])
                    entry["low"] = min(entry["low"], t["price"])
                    entry["close"] = t["price"]
                    entry["volume"] += t["size"]

            c1_list = [c1_map[k] for k in sorted(c1_map.keys())]
            # keep last 60 seconds
            self.c1 = c1_list[-60:]

            # Build 5s candles by aggregating 1s candles
            c5_map: Dict[int, Dict[str, Any]] = {}
            for c in self.c1:
                # bucket by 5s
                bucket = (c["ts"] // 5) * 5
                entry = c5_map.get(bucket)
                if not entry:
                    c5_map[bucket] = {"ts": bucket, "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": c["volume"]}
                else:
                    entry["high"] = max(entry["high"], c["high"])
                    entry["low"] = min(entry["low"], c["low"])
                    entry["close"] = c["close"]
                    entry["volume"] += c["volume"]

            c5_list = [c5_map[k] for k in sorted(c5_map.keys())]
            # keep last 60 5-second candles
            self.c5 = c5_list[-60:]
        except Exception as e:
            logger.exception(f"{self.symbol}: update_candles failed: {e}")

    # --- maybe_eval: decide when to call engine.evaluate ---
    def maybe_eval(self) -> None:
        """
        Called periodically by the worker loop. Throttles evaluate calls to SCAN_INTERVAL.
        """
        try:
            now = now_ts()
            if (now - self.last_eval) < SCAN_INTERVAL:
                return
            # must have some data
            if not self.book or (not self.book["bids"] and not self.book["asks"]):
                return
            # trades & candles required by decision engine
            trades_list = list(self.trades)
            prices_list = list(self.prices)
            self.update_candles()
            # call engine.evaluate
            try:
                self.engine.evaluate(self.symbol, self.book, trades_list, self.c5, self.c1, prices_list, self.last_ob_ts)
            except Exception as e:
                logger.exception(f"{self.symbol}: engine.evaluate error: {e}")
            self.last_eval = now
        except Exception as e:
            logger.exception(f"{self.symbol}: maybe_eval failed: {e}")

    # --- main thread run ---
    def run(self) -> None:
        # Run the websocket connection in the current thread (blocking)
        # but also periodically check for inactivity and call maybe_eval.
        th = threading.Thread(target=self.connect, daemon=True)
        th.start()

        # Monitor loop
        try:
            while not self._stop.is_set():
                # if websocket silent for too long, try reconnect
                if now_ts() - self.last_msg_ts > WS_SILENCE_SEC:
                    logger.warning(f"{self.symbol}: ws silence detected, reconnecting...")
                    try:
                        if self.ws:
                            self.ws.close()
                    except Exception:
                        pass
                    # ensure connect thread will restart
                    time.sleep(1.0)

                # run evaluation throttle & logic
                self.maybe_eval()

                time.sleep(0.2)
        except Exception as e:
            logger.exception(f"{self.symbol}: worker run failed: {e}")
        finally:
            try:
                self.close()
            except Exception:
                pass


# ================================================================
# MAIN
# ================================================================

def main() -> None:
    global STARTING_EQUITY, bot_killed, kill_alert_sent

    logger.info("Starting Bybit scalper bot...")
    tg("🟢 Bot started")

    start_heartbeat()

    exchange = ExchangeClient(API_KEY, API_SECRET, testnet=TESTNET)

    # Close any leftover positions from previous run
    emergency_close_all(exchange, SYMBOLS)

    try:
        STARTING_EQUITY = exchange.get_balance()
    except Exception as e:
        logger.warning(f"Failed to fetch starting equity: {e}")
        STARTING_EQUITY = 0.0
    bot_killed = False
    kill_alert_sent = False

    logger.info(f"Kill-switch starting equity: {STARTING_EQUITY:.4f}, trigger={GLOBAL_KILL_TRIGGER*100:.1f}%")
    tg(f"📊 Starting equity: {STARTING_EQUITY:.4f} USDT. Kill at {GLOBAL_KILL_TRIGGER*100:.1f}% loss.")

    for s in SYMBOLS:
        try:
            exchange.set_leverage(s, LEVERAGE)
        except Exception as e:
            logger.warning(f"{s}: leverage set failed: {e}")

    engine = DecisionEngine(exchange, SYMBOLS)
    workers: List[MarketWorker] = []

    for s in SYMBOLS:
        w = MarketWorker(s, engine, exchange, TESTNET)
        w.start()
        workers.append(w)

    logger.info("Workers started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Stopping workers...")
        tg("🛑 Bot stopping (KeyboardInterrupt).")
        for w in workers:
            try:
                w.close()
            except Exception:
                pass
        # allow threads to exit gracefully
        time.sleep(1.0)


if __name__ == "__main__":
    main()
