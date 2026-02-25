from extract import extract
from transform import transform
from insert import load
if __name__ == "__main__":
    pages = extract(max_pages=3)
    products = transform(pages)

    load(products)
