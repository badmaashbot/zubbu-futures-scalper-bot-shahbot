import aiohttp
import asyncio
import json
import time

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT"]

WS_URL = "wss://stream.bybit.com/v5/public"   # correct namespace

async def ws_test():
    topics = []
    for s in SYMBOLS:
        topics.append(f"orderbook.50.{s}")      # correct depth
        topics.append(f"publicTrade.{s}")       # trade feed

    print("🔌 Connecting to Bybit WS…")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(WS_URL, heartbeat=20) as ws:
            print("✅ Connected. Subscribing…")
            await ws.send_json({"op": "subscribe", "args": topics})

            last_book_ts = {s: 0 for s in SYMBOLS}
            last_trade_ts = {s: 0 for s in SYMBOLS}

            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                data = json.loads(msg.data)
                topic = data.get("topic")

                if not topic:
                    continue

                # ORDERBOOK
                if topic.startswith("orderbook"):
                    sym = topic.split(".")[-1]
                    payload = data["data"][0] if isinstance(data["data"], list) else data["data"]
                    ts = payload.get("ts", time.time() * 1000)
                    last_book_ts[sym] = int(ts // 1000)

                # TRADES
                if topic.startswith("publicTrade"):
                    sym = topic.split(".")[-1]
                    last_trade_ts[sym] = int(time.time())

                # Print status every 2 sec
                now = int(time.time())
                if now % 2 == 0:
                    print("\n====== WS STATUS ======")
                    for s in SYMBOLS:
                        print(
                            f"{s}: book_ts={last_book_ts[s]}   trades_ts={last_trade_ts[s]}"
                        )

asyncio.run(ws_test())
