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

POSTGRES_DSN = os.getenv("DATABASE_URL")
if not POSTGRES_DSN:
    raise ValueError("Missing DATABASE_URL in environment variables")

# Redis will not be used in cloud (or could be optional)
USE_REDIS = os.getenv("USE_REDIS", "false").lower() == "true"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

redis = None
pg_conn = None


def init_pg():
    global pg_conn
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


async def init_redis():
    global redis
    if USE_REDIS:
        redis = aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        logger.info("Connected to Redis")


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
                cursor.execute("""
                    INSERT INTO kraken_trades (event_time, pair, price, volume, side, order_type)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (timestamp, pair, price, volume, side, order_type))
            pg_conn.commit()

            if redis:
                record_json = {**record, "event_time": timestamp.isoformat()}
                await redis.publish("kraken_trades", json.dumps(record_json))

        except Exception:
            logger.error("Failed to handle trade:")
            logger.error(traceback.format_exc())


async def handle_orderbook(pair, data):
    event_time = datetime.utcnow().replace(tzinfo=timezone.utc)

    if "as" in data or "bs" in data:
        asks = data.get("as", [])
        bids = data.get("bs", [])
        update_type = "snapshot"
    elif "a" in data or "b" in data:
        asks = data.get("a", [])
        bids = data.get("b", [])
        update_type = "update"
    else:
        return

    if not asks and not bids:
        return

    record = {
        "event_time": event_time,
        "pair": pair,
        "bids": bids,
        "asks": asks,
        "type": update_type
    }

    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO kraken_orderbook (event_time, pair, bids, asks, type)
                VALUES (%s, %s, %s, %s, %s);
            """, (event_time, pair, json.dumps(bids), json.dumps(asks), update_type))
        pg_conn.commit()

        if redis:
            record_json = {**record, "event_time": event_time.isoformat()}
            await redis.publish("kraken_orderbook", json.dumps(record_json))

    except Exception:
        logger.error("Failed to handle orderbook:")
        logger.error(traceback.format_exc())



async def main():
    init_pg()
    await init_redis()
    while True:
        try:
            await collector()
        except Exception:
            logger.error("Collector crashed. Restarting in 5s...")
            logger.error(traceback.format_exc())
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
