import asyncio
import logging
import time
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Literal


Side = Literal["long", "short"]


@dataclass
class Candle:
    ts: float          # timestamp (seconds)
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Position:
    side: Side
    entry_price: float
    size: float           # in contracts or coin, up to your exchange
    tp: float             # current take-profit price
    sl: float             # current stop-loss price
    step: int = 0         # 0 = TP0.4, 1 = TP0.8, 2 = TP1.0+
    opened_ts: float = field(default_factory=time.time)


@dataclass
class OrderBookSnapshot:
    best_bid: float = 0.0
    best_ask: float = 0.0
    bid_vol_top: float = 0.0   # sum of top levels
    ask_vol_top: float = 0.0   # sum of top levels


@dataclass
class BurstScalperConfig:
    symbol: str = "BTCUSDT"
    max_micro_window_sec: float = 10.0      # look-back window for burst calc
    min_burst_pct: float = 0.0010          # 0.10% burst to qualify
    target0_pct: float = 0.0040            # 0.40% first TP
    target1_pct: float = 0.0080            # 0.80% second TP
    target2_pct: float = 0.0100            # 1.00% third TP
    sl0_pct: float = 0.0025                # 0.25% initial SL
    sl1_lock_pct: float = 0.0005           # 0.05% when TP0 hit
    sl2_lock_pct: float = 0.0040           # 0.40% when TP1 hit
    sl3_lock_pct: float = 0.0070           # 0.70% when TP2 hit
    max_spread_pct: float = 0.0007         # 0.07% max allowed spread
    min_candle_body_pct: float = 0.0012    # 0.12% minimum 1m candle body
    recent_candles_to_check: int = 3       # last 3 candles same direction
    watchdog_spread_panic_pct: float = 0.0015  # if spread > 0.15% -> exit
    watchdog_max_position_sec: float = 600.0   # close if stuck > 600s (10 min)
    risk_per_trade_usd: float = 10.0       # example size logic; tune as needed


class BurstScalperBot:
    """
    0.4% burst-opportunity scalper:
    - Opens trade ONLY when it sees a realistic 0.4% continuation chance.
    - Uses dynamic TP/SL (0.4 -> 0.8 -> 1.0+) with watchdog style safety.
    """

    def __init__(self, config: BurstScalperConfig):
        self.cfg = config
        self.position: Optional[Position] = None

        # micro-price buffer: (ts, price)
        self.micro_prices: Deque[tuple[float, float]] = deque()

        # last completed 1m candles (you must feed these)
        self.candles_1m: Deque[Candle] = deque(maxlen=50)

        # latest mid price and orderbook
        self.last_mid: Optional[float] = None
        self.orderbook: OrderBookSnapshot = OrderBookSnapshot()

        # delta volume (approx): positive = buyers, negative = sellers
        self.delta_vol: float = 0.0

        # logger
        self.log = logging.getLogger("BurstScalperBot")

    # ----------------------------------------------------------------------
    # PUBLIC HOOKS YOU CALL FROM YOUR EXISTING CODE
    # ----------------------------------------------------------------------

    def on_ws_tick(self, price: float, side: Literal["buy", "sell"], size: float):
        """
        Call this from your trade WebSocket handler every time you get a trade tick.
        - price: last trade price
        - side: "buy" if aggressor is buyer, "sell" if aggressor is seller
        - size: trade quantity
        """
        now = time.time()

        # track micro prices
        self.micro_prices.append((now, price))
        self.last_mid = price if self.last_mid is None else price

        # prune old points
        cutoff = now - self.cfg.max_micro_window_sec
        while self.micro_prices and self.micro_prices[0][0] < cutoff:
            self.micro_prices.popleft()

        # delta volume update
        if side == "buy":
            self.delta_vol += size
        else:
            self.delta_vol -= size

    def update_orderbook(
        self,
        best_bid: float,
        best_ask: float,
        bid_vol_top: float,
        ask_vol_top: float,
    ):
        """
        Call this from your orderbook WebSocket whenever you get an update.
        """
        self.orderbook.best_bid = best_bid
        self.orderbook.best_ask = best_ask
        self.orderbook.bid_vol_top = bid_vol_top
        self.orderbook.ask_vol_top = ask_vol_top

        if best_bid > 0 and best_ask > 0:
            self.last_mid = (best_bid + best_ask) / 2.0

    def add_completed_candle_1m(self, candle: Candle):
        """
        Call this each time a new 1-minute candle completes.
        You can build candles from WebSocket trades or from REST OHLCV.
        """
        self.candles_1m.append(candle)

    async def maybe_enter(self):
        """
        Call this frequently (e.g., once per second) in your main loop.
        If there is no position and 0.4% chance appears, it opens a trade.
        """
        if self.position is not None:
            return  # already in a trade

        if not self.last_mid:
            return

        if not self._spread_ok():
            return

        direction = self._detect_burst_direction()
        if direction is None:
            # no clear 0.4% burst opportunity
            return

        await self._open_position(direction)

    async def watchdog(self):
        """
        Call this frequently (e.g., every second).
        Manages dynamic TP/SL and safety exits.
        """
        if self.position is None or not self.last_mid:
            return

        await self._manage_dynamic_tp_sl()
        await self._check_watchdog_conditions()

    # ----------------------------------------------------------------------
    # INTERNAL LOGIC
    # ----------------------------------------------------------------------

    def _spread_ok(self) -> bool:
        ob = self.orderbook
        if ob.best_bid <= 0 or ob.best_ask <= 0:
            return False
        spread_pct = (ob.best_ask - ob.best_bid) / ((ob.best_ask + ob.best_bid) / 2.0)
        ok = spread_pct <= self.cfg.max_spread_pct
        if not ok:
            self.log.debug(f"Spread too wide: {spread_pct:.6f}")
        return ok

    def _detect_burst_direction(self) -> Optional[Side]:
        """
        Core '0.4% opportunity' detection:
        Returns "long", "short", or None.
        """

        if not self.micro_prices or len(self.candles_1m) < self.cfg.recent_candles_to_check:
            return None

        # 1) Check last N 1m candles in same direction
        last_candles = list(self.candles_1m)[-self.cfg.recent_candles_to_check:]
        up_candles = all(c.close > c.open and self._body_pct(c) >= self.cfg.min_candle_body_pct
                         for c in last_candles)
        down_candles = all(c.close < c.open and self._body_pct(c) >= self.cfg.min_candle_body_pct
                           for c in last_candles)

        # 2) Check micro burst (0.10–0.20%+ within window)
        micro_dir = self._micro_burst_direction()

        # 3) Delta volume direction
        delta_dir: Optional[Side] = None
        if self.delta_vol > 0:
            delta_dir = "long"
        elif self.delta_vol < 0:
            delta_dir = "short"

        # 4) Orderbook pressure (not strict imbalance)
        ob_dir = self._orderbook_push_direction()

        # Combine these: if majority agrees in one direction, treat as 0.4% chance
        score_long = 0
        score_short = 0

        if up_candles:
            score_long += 1
        if down_candles:
            score_short += 1

        if micro_dir == "long":
            score_long += 1
        elif micro_dir == "short":
            score_short += 1

        if delta_dir == "long":
            score_long += 1
        elif delta_dir == "short":
            score_short += 1

        if ob_dir == "long":
            score_long += 1
        elif ob_dir == "short":
            score_short += 1

        # require at least 2 "votes" in one direction
        if score_long >= 2 and score_long > score_short:
            self.log.debug(f"0.4% LONG opportunity: scores L={score_long}, S={score_short}")
            # reset delta after taking signal
            self.delta_vol = 0.0
            return "long"
        if score_short >= 2 and score_short > score_long:
            self.log.debug(f"0.4% SHORT opportunity: scores L={score_long}, S={score_short}")
            self.delta_vol = 0.0
            return "short"

        return None

    def _body_pct(self, c: Candle) -> float:
        mid = (c.high + c.low) / 2.0
        if mid <= 0:
            return 0.0
        return abs(c.close - c.open) / mid

    def _micro_burst_direction(self) -> Optional[Side]:
        if len(self.micro_prices) < 2:
            return None

        oldest_ts, oldest_price = self.micro_prices[0]
        newest_ts, newest_price = self.micro_prices[-1]

        if oldest_price <= 0:
            return None

        move_pct = (newest_price - oldest_price) / oldest_price

        if abs(move_pct) < self.cfg.min_burst_pct:
            return None

        return "long" if move_pct > 0 else "short"

    def _orderbook_push_direction(self) -> Optional[Side]:
        ob = self.orderbook
        if ob.bid_vol_top <= 0 or ob.ask_vol_top <= 0:
            return None

        # if bids > asks by 10% -> long pressure
        # if asks > bids by 10% -> short pressure
        ratio = ob.bid_vol_top / ob.ask_vol_top
        if ratio > 1.10:
            return "long"
        if ratio < 0.90:
            return "short"
        return None

    async def _open_position(self, side: Side):
        entry_price = self.last_mid
        if not entry_price:
            return

        size = self._calc_size(entry_price)
        if size <= 0:
            self.log.warning("Size calculated as 0, skipping open_position")
            return

        # compute TP/SL
        if side == "long":
            tp = entry_price * (1.0 + self.cfg.target0_pct)
            sl = entry_price * (1.0 - self.cfg.sl0_pct)
        else:
            tp = entry_price * (1.0 - self.cfg.target0_pct)
            sl = entry_price * (1.0 + self.cfg.sl0_pct)

        # ====== YOUR EXCHANGE ORDER HERE ======
        # Replace this function with your real order placement (Bybit / ccxt etc).
        order_id = await self.place_market_order(side, size, tp, sl)

        if not order_id:
            self.log.error("Failed to place entry order, aborting position creation")
            return

        self.position = Position(
            side=side,
            entry_price=entry_price,
            size=size,
            tp=tp,
            sl=sl,
            step=0,
        )
        self.log.info(
            f"OPEN {side.upper()} size={size}, entry={entry_price:.4f}, "
            f"TP={tp:.4f}, SL={sl:.4f}"
        )

    def _calc_size(self, entry_price: float) -> float:
        # Example: risk_per_trade_usd / (entry_price * sl_pct)
        # This is just a simple size helper. You can replace with your own.
        risk_usd = self.cfg.risk_per_trade_usd
        sl_pct = self.cfg.sl0_pct
        if entry_price <= 0 or sl_pct <= 0:
            return 0.0
        # risk = size * entry_price * sl_pct  =>  size = risk / (entry_price * sl_pct)
        size = risk_usd / (entry_price * sl_pct)
        return size

    async def _manage_dynamic_tp_sl(self):
        assert self.position is not None
        pos = self.position
        price = self.last_mid
        if not price:
            return

        # compute pnl % relative to entry
        if pos.side == "long":
            pnl_pct = (price - pos.entry_price) / pos.entry_price
        else:
            pnl_pct = (pos.entry_price - price) / pos.entry_price

        # Step 0 -> 1: hit 0.4%
        if pos.step == 0 and pnl_pct >= self.cfg.target0_pct:
            # lock tiny profit and go for 0.8%
            if pos.side == "long":
                new_sl = pos.entry_price * (1.0 + self.cfg.sl1_lock_pct)
                new_tp = pos.entry_price * (1.0 + self.cfg.target1_pct)
            else:
                new_sl = pos.entry_price * (1.0 - self.cfg.sl1_lock_pct)
                new_tp = pos.entry_price * (1.0 - self.cfg.target1_pct)

            await self._update_tp_sl(new_tp, new_sl)
            pos.tp = new_tp
            pos.sl = new_sl
            pos.step = 1
            self.log.info(
                f"[STEP1] {pos.side.upper()} hit +0.4%, SL locked, TP -> 0.8%"
            )

        # Step 1 -> 2: hit 0.8%
        elif pos.step == 1 and pnl_pct >= self.cfg.target1_pct:
            if pos.side == "long":
                new_sl = pos.entry_price * (1.0 + self.cfg.sl2_lock_pct)
                new_tp = pos.entry_price * (1.0 + self.cfg.target2_pct)
            else:
                new_sl = pos.entry_price * (1.0 - self.cfg.sl2_lock_pct)
                new_tp = pos.entry_price * (1.0 - self.cfg.target2_pct)

            await self._update_tp_sl(new_tp, new_sl)
            pos.tp = new_tp
            pos.sl = new_sl
            pos.step = 2
            self.log.info(
                f"[STEP2] {pos.side.upper()} hit +0.8%, SL locked more, TP -> 1.0%"
            )

        # Step 2+: trailing or exit logic once >1.0%
        elif pos.step >= 2 and pnl_pct >= self.cfg.target2_pct:
            # simple trailing: keep SL at max(prev_sl, entry + 0.7%)
            if pos.side == "long":
                trail_sl = pos.entry_price * (1.0 + self.cfg.sl3_lock_pct)
                if trail_sl > pos.sl:
                    await self._update_tp_sl(None, trail_sl)
                    pos.sl = trail_sl
                    self.log.info(
                        f"[TRAIL] LONG SL moved up to secure >0.7%"
                    )
            else:
                trail_sl = pos.entry_price * (1.0 - self.cfg.sl3_lock_pct)
                if trail_sl < pos.sl:
                    await self._update_tp_sl(None, trail_sl)
                    pos.sl = trail_sl
                    self.log.info(
                        f"[TRAIL] SHORT SL moved down to secure >0.7%"
                    )

    async def _check_watchdog_conditions(self):
        """
        Kill-switch like safety:
        - spread too wide
        - position open too long
        """
        assert self.position is not None
        pos = self.position

        # spread panic
        ob = self.orderbook
        if ob.best_bid > 0 and ob.best_ask > 0:
            spread_pct = (ob.best_ask - ob.best_bid) / ((ob.best_ask + ob.best_bid) / 2.0)
            if spread_pct >= self.cfg.watchdog_spread_panic_pct:
                self.log.warning(
                    f"[WATCHDOG] Spread panic {spread_pct:.6f}, closing position"
                )
                await self._close_position("spread_panic")
                return

        # too long in trade
        age = time.time() - pos.opened_ts
        if age >= self.cfg.watchdog_max_position_sec:
            self.log.warning(
                f"[WATCHDOG] Position age {age:.1f}s exceeded, closing"
            )
            await self._close_position("timeout")

    # ----------------------------------------------------------------------
    # EXCHANGE INTERACTION — YOU MUST IMPLEMENT THESE
    # ----------------------------------------------------------------------

    async def place_market_order(self, side: Side, size: float, tp: float, sl: float) -> Optional[str]:
        """
        TODO: Replace this with your real order placement logic.
        Should:
        - send a MARKET order in 'side' direction
        - attach TP/SL or create separate TP/SL orders
        - return an order_id or position_id string
        """
        self.log.info(
            f"[SIM] place_market_order side={side}, size={size}, tp={tp}, sl={sl}"
        )
        # For now, simulate success:
        await asyncio.sleep(0)  # keep it awaitable
        return "SIM_ORDER_ID"

    async def _update_tp_sl(self, new_tp: Optional[float], new_sl: Optional[float]):
        """
        TODO: Implement TP/SL modification on the exchange.
        - If new_tp is None, keep previous TP
        - If new_sl is None, keep previous SL
        """
        self.log.info(f"[SIM] update_tp_sl tp={new_tp}, sl={new_sl}")
        await asyncio.sleep(0)

    async def _close_position(self, reason: str):
        """
        TODO: Implement full position close on the exchange.
        After successful close, set self.position = None.
        """
        if not self.position:
            return
        self.log.info(
            f"[SIM] close_position side={self.position.side}, reason={reason}"
        )
        # send market close here in your real implementation
        self.position = None
        await asyncio.sleep(0)


# ----------------------------------------------------------------------
# EXAMPLE MAIN LOOP (You probably already have something similar)
# ----------------------------------------------------------------------

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    cfg = BurstScalperConfig(
        symbol="BTCUSDT",
        risk_per_trade_usd=10.0,
    )
    bot = BurstScalperBot(cfg)

    # TODO:
    # - Start your WebSocket(s)
    # - In your ws handlers, call:
    #   bot.on_ws_tick(price, side, size)
    #   bot.update_orderbook(best_bid, best_ask, bid_vol_top, ask_vol_top)
    #   bot.add_completed_candle_1m(Candle(...))

    while True:
        await bot.maybe_enter()
        await bot.watchdog()
        await asyncio.sleep(1.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
