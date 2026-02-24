import requests
import random
from typing import Optional


BASE_URL = "https://www.jumia.co.ke/home-office/"


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

if __name__ == "__main__":
    html = fetch_page(1)

    