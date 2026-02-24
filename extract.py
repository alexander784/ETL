import requests
import random
from typing import Optional
import time
from bs4 import BeautifulSoup


BASE_URL = "https://www.jumia.co.ke/home-office/"
DELAY_RANGE = (2, 5)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
    "Gecko/20100101 Firefox/117.0",
]

def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         "https://www.jumia.co.ke/",

    }


def fetch_page(page: int) -> Optional[str]:
    url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"

    try:
        res = requests.get(url, headers=get_headers())
        res.raise_for_status()
    except requests.RequestException as e:
        return None

    return res.text


def extract(max_pages: int) -> list[str]:
    pages = []

    for page_num in range(1, max_pages + 1):
        html = fetch_page(page_num)
        if not html:
            print(f"Page {page_num}: fetch failed")
            continue


        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("article.prd")


        if not cards:
            print("No products found — stopping pagination")
            break

        title = cards[0].select_one("h3.name")
        price = cards[0].select_one("div.prc")


        pages.append(html)

        if page_num < max_pages:
            time.sleep(random.uniform(*DELAY_RANGE))

    return pages
        
