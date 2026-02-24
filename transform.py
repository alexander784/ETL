import re
from typing import Optional
import datetime




def parse_price(raw: str) -> Optional[int]:

    numbers = re.find_all(r"[\d,]+", raw or "")


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


