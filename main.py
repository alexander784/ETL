from extract import extract
from transform import transform
from insert import get_conn, init_schema
if __name__ == "__main__":
    pages = extract(max_pages=3)

    print(f"Extracted {len(pages)} pages")

    products = transform(pages)

    print(f"Final product count: {len(products)}")

    conn = get_conn()
    init_schema(conn)
    conn.close()