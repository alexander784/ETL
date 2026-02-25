import logging
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()



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
        return conn
    except psycopg2.OperationalError as e:
        raise

def init_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()

def load(products: list[dict]):
   
    conn = get_conn()
    init_schema(conn)

    inserted = updated = failed = 0

    with conn:
        with conn.cursor() as cur:
            for p in products:
                try:
                    cur.execute(
                        """
                        INSERT INTO products (url, name, image_url, first_seen, last_updated)
                        VALUES (%s, %s, %s, NOW(), NOW())
                        ON CONFLICT (url) DO UPDATE
                            SET name         = EXCLUDED.name,
                                last_updated = NOW()
                        RETURNING id, (xmax = 0) AS is_insert
                        """,
                        (p["url"], p["name"], p.get("image_url"))
                    )
                    product_id, is_insert = cur.fetchone()
                    inserted += 1 if is_insert else 0
                    updated  += 0 if is_insert else 1

                    cur.execute(
                        """
                        INSERT INTO price_logs
                            (product_id, scraped_at, current_price, original_price,
                             discount_pct, rating, review_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            product_id,
                            p["scraped_at"],
                            p["current_price_ksh"],
                            p.get("original_price_ksh"),
                            p.get("discount_pct"),
                            p.get("rating"),
                            p.get("review_count"),
                        )
                    )

                except Exception as e:
                    conn.rollback()
                    failed += 1
                    continue

    conn.close()
