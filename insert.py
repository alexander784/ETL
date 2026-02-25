import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ale_scrape:scrape@localhost:5432/scrape_db"
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id             SERIAL PRIMARY KEY,
    url            TEXT        UNIQUE NOT NULL,
    name           TEXT        NOT NULL,
    image_url      TEXT,
    first_seen     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS price_logs (
    id             SERIAL PRIMARY KEY,
    product_id     INTEGER     NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    scraped_at     TIMESTAMPTZ NOT NULL,
    current_price  INTEGER     NOT NULL,
    original_price INTEGER,
    discount_pct   INTEGER,
    rating         NUMERIC(3,1),
    review_count   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_price_logs_product ON price_logs(product_id);
CREATE INDEX IF NOT EXISTS idx_price_logs_date    ON price_logs(scraped_at);
"""

def get_conn():
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        log.info("Connected to PostgreSQL successfully")
        return conn
    except psycopg2.OperationalError as e:
        log.error(f"Cannot connect to PostgreSQL: {e}")
        log.error("Check DATABASE_URL in your .env file")
        raise

def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()
    log.info("Database schema initialized")