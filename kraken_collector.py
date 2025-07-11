import asyncio
import json
import os
import traceback
from datetime import datetime, timezone

import redis.asyncio as aioredis
import psycopg2
import psycopg2.extras
import websockets
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Init PostgreSQL connection
pg_conn = psycopg2.connect(POSTGRES_DSN)
pg_conn.autocommit = False

with pg_conn.cursor() as cursor:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kraken_trades (
        event_time TIMESTAMP,
        pair TEXT,
        price NUMERIC,
        volume NUMERIC,
        side TEXT,
        order_type TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kraken_orderbook (
        event_time TIMESTAMP,
        pair TEXT,
        bids JSONB,
        asks JSONB
    );
    """)
pg_conn.commit()

redis = None

async def init_redis():
    global redis
    redis = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")

async def handle_trade(pair, data):
    for trade in data:
        try:
            price = float(trade[0])
            volume = float(trade[1])
            timestamp = datetime.utcfromtimestamp(float(trade[2])).replace(tzinfo=timezone.utc)
            side = trade[3]
            order_type = trade[4]

            record = {
                "event_time": timestamp,
                "pair": pair,
                "price": price,
                "volume": volume,
                "side": side,
                "order_type": order_type
            }

            with pg_conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO kraken_trades (event_time, pair, price, volume, side, order_type) VALUES (%s, %s, %s, %s, %s, %s);",
                    (timestamp, pair, price, volume, side, order_type)
                )
            pg_conn.commit()

            record_json = record.copy()
            record_json["event_time"] = record_json["event_time"].isoformat()
            await redis.publish("kraken_trades", json.dumps(record_json))
            logger.debug(f"Trade: {record_json}")

        except Exception:
            logger.error("Failed to handle trade:")
            logger.error(traceback.format_exc())

async def handle_orderbook(pair, data):
    # Only save full snapshots
    if "as" in data and "bs" in data:
        asks = data["as"]
        bids = data["bs"]
    else:
        return  # Skip updates

    if not asks or not bids:
        return  # Require both

    event_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    record = {
        "event_time": event_time,
        "pair": pair,
        "bids": bids,
        "asks": asks
    }

    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO kraken_orderbook (event_time, pair, bids, asks) VALUES (%s, %s, %s, %s);",
                (event_time, pair, json.dumps(bids), json.dumps(asks))
            )
        pg_conn.commit()

        record_json = record.copy()
        record_json["event_time"] = record_json["event_time"].isoformat()
        await redis.publish("kraken_orderbook", json.dumps(record_json))
        logger.debug(f"Stored full snapshot: {pair} @ {event_time.isoformat()}")

    except Exception:
        logger.error("Failed to handle orderbook:")
        logger.error(traceback.format_exc())

async def collector(pairs=["XBT/USD"]):
    url = "wss://ws.kraken.com"

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "trade"}
        }))
        await ws.send(json.dumps({
            "event": "subscribe",
            "pair": pairs,
            "subscription": {"name": "book", "depth": 10}
        }))
        logger.info("Subscribed to Kraken streams")

        async for message in ws:
            try:
                msg_data = json.loads(message)

                if isinstance(msg_data, dict):
                    event = msg_data.get("event")
                    if event == "subscriptionStatus":
                        logger.info(f"Subscription confirmed: {msg_data}")
                    elif event == "heartbeat":
                        logger.debug("Heartbeat received")
                    continue

                if isinstance(msg_data, list) and len(msg_data) >= 4:
                    channel = msg_data[2]
                    pair = msg_data[3]
                    data = msg_data[1]

                    if channel == "trade":
                        await handle_trade(pair, data)
                    elif channel == "book-10":
                        await handle_orderbook(pair, data)

            except Exception:
                logger.error("Collector error inside stream loop:")
                logger.error(traceback.format_exc())

async def main():
    await init_redis()
    while True:
        try:
            await collector()
        except Exception:
            logger.error("Collector crashed. Restarting in 5 seconds.")
            logger.error(traceback.format_exc())
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
