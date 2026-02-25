import re
from typing import Optional
from datetime import datetime
from bs4 import BeautifulSoup



def parse_price(raw: str) -> Optional[int]:

    numbers = re.findall(r"[\d,]+", raw or "")


    if not numbers:
         return None
    try:
        return int(numbers[0].replace(",", ""))
    except ValueError:
        return None
    
def parse_discount(raw: str) -> Optional[int]:

    m = re.search(r"(\d+)", raw or "")
    return int(m.group(1)) if m else None


def parse_rating(raw: str) -> Optional[float]:
    m = re.search(r"(\d+\.?\d*)", raw or "")
    return float(m.group(1)) if m else None


def parse_reviews(raw: str) -> Optional[int]:
   
    m = re.search(r"(\d+)", raw or "")
    return int(m.group(1)) if m else None


def transform_card(card) -> Optional[dict]:
   
    try:
        name_el      = card.select_one("h3.name")
        link_el      = card.select_one("a.core")
        price_el     = card.select_one("div.prc")
        old_price_el = card.select_one("div.old")
        discount_el  = card.select_one("div.bdg._dsct")
        rating_el    = card.select_one("div.stars._s")
        reviews_el   = card.select_one("div.rev")
        img_el       = card.select_one("img.img")

        name  = name_el.get_text(strip=True) if name_el else None
        price = parse_price(price_el.get_text(strip=True) if price_el else "")

        if not name or not price:
            return None

        if price <= 0:
            return None

        original_price = parse_price(old_price_el.get_text(strip=True) if old_price_el else "")
        discount_pct   = parse_discount(discount_el.get_text(strip=True) if discount_el else "")

        if original_price and original_price < price:
            original_price = None

        return {
            "scraped_at":         datetime.utcnow().isoformat(),
            "name":               name.strip(),
            "url":                "https://www.jumia.co.ke" + link_el["href"] if link_el else None,
            "current_price_ksh":  price,
            "original_price_ksh": original_price,
            "discount_pct":       discount_pct,
            "rating":             parse_rating(rating_el.get_text(strip=True) if rating_el else ""),
            "review_count":       parse_reviews(reviews_el.get_text(strip=True) if reviews_el else ""),
            "image_url":          (img_el.get("data-src") or img_el.get("src")) if img_el else None,
        }

    except Exception as e:
        return None
    
def transform(pages: list[str]) -> list[dict]:
   
    all_products = []
    seen_urls    = set()

    for i, html in enumerate(pages, 1):
        soup  = BeautifulSoup(html, "html.parser")
        cards = soup.select("article.prd")

        for card in cards:
            product = transform_card(card)
            if not product:
                continue

            key = product.get("url") or product["name"]
            if key in seen_urls:
                continue

            seen_urls.add(key)
            all_products.append(product)

    print(f"Transform complete — {len(all_products)} clean products")
    return all_products