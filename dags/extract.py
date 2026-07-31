import requests
import random
from typing import Optional
import time
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BASE_URL = "https://www.jumia.co.ke/home-office/"
DELAY_RANGE = (2, 5)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
  
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.jumia.co.ke/",
    }


def fetch_page(page: int) -> Optional[str]:
    url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"

    if page > 1:
        url += f"?page={page}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        )
        page_obj = context.new_page()

        page_obj.goto(url)
        html = page_obj.content()
        browser.close()
    return html

    


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

        
