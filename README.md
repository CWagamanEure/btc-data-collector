# BTC Market Data Collector

This service streams real-time BTC/USD trade and order book data from Kraken via WebSocket and writes it into a PostgreSQL database. It is designed for deployment on cloud platforms like Render, with optional Redis integration for pub/sub pipelines.

---

## Features

- WebSocket-based collection of:
  - Trade data (`price`, `volume`, `side`, `order_type`, `timestamp`)
  - Level 2 order book updates (up to depth 1000)
- Writes to PostgreSQL (`kraken_trades`, `kraken_orderbook`)
- Handles both snapshots and incremental order book updates
- Optional Redis pub/sub support

---

## PostgreSQL Schema

### `kraken_trades`
| Column      | Type       |
|-------------|------------|
| event_time  | TIMESTAMP  |
| pair        | TEXT       |
| price       | NUMERIC    |
| volume      | NUMERIC    |
| side        | TEXT       |
| order_type  | TEXT       |

### `kraken_orderbook`
| Column      | Type       |
|-------------|------------|
| event_time  | TIMESTAMP  |
| pair        | TEXT       |
| bids        | JSONB      |
| asks        | JSONB      |
| type        | TEXT       | — 'snapshot' or 'update'

---

## Environment Variables

These must be set either in a `.env` file or through your cloud platform’s environment configuration:

```
DATABASE_URL=postgresql://user:password@host:port/database
USE_REDIS=false
REDIS_HOST=localhost
REDIS_PORT=6379
```

---

## Setup

### Local

```bash
pip install -r requirements.txt
python collector.py
```

### Render

1. Create a **Background Worker** service
2. Link to your GitHub repository
3. Set Build and Start commands:
   - Build: `pip install -r requirements.txt`
   - Start: `python collector.py`
4. Add environment variables from above

---

## Redis (Optional)

If `USE_REDIS=true`, the collector will publish trade and order book messages as JSON to:
- `kraken_trades`
- `kraken_orderbook`

This is useful for building real-time pipelines or dashboards on top of the collector.

---

## Notes

- All timestamps are stored in UTC
- Order book entries are stored as raw JSON arrays of `[price, size]`
- Database writes are buffered for performance but autocommit is enforced for safety
- Supports reconnect and resume on WebSocket drops

---

## License

MIT
