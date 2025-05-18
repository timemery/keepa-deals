# Chunk 1 starts
# stable_products.py (partial, showing fixed used_30_days_avg)
import requests
import logging
from retrying import retry
from datetime import datetime, timedelta
from pytz import timezone
from stable_deals import validate_asin
import json
from keepa import Keepa
from typing import Dict, Any

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_product_for_retry(asin):
    with open('config.json') as f:
        config = json.load(f)
    api = Keepa(config['api_key'])
    product = api.query(asin, product_code_is_asin=True, stats=90, domain='US', history=False)
    return product[0] if product else {}

# Constants
KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')

# Shared globals
API_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}

# Global stuff starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def percent_down_90(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            logging.error(f"percent_down_90 request failed for ASIN {asin}: {response.status_code}")
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"percent_down_90 no product data for ASIN {asin}")
            return '-'
        stats = products[0].get('stats', {})
        current_price = stats.get('current', [None] * 11)[1]
        avg_90 = stats.get('avg90', [None] * 11)[1]
        if current_price is not None and avg_90 is not None and avg_90 > 0:
            percent_down = ((avg_90 - current_price) / avg_90) * 100
            return f"{percent_down:.0f}%"
        return '-'
    except Exception as e:
        logging.error(f"percent_down_90 fetch failed for ASIN {asin}: {str(e)}")
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def avg_price_90(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def percent_down_365(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        current_price = stats.get('current', [None] * 11)[1]
        avg_365 = stats.get('avg365', [None] * 11)[1]
        if current_price is not None and avg_365 is not None and avg_365 > 0:
            percent_down = ((avg_365 - current_price) / avg_365) * 100
            return f"{percent_down:.0f}%"
        return '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def avg_price_365(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def price_now(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def price_now_source(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[1]
        return 'Buy Box' if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_title(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"get_title response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"get_title request failed for ASIN {asin}: {response.status_code}")
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"get_title no product data for ASIN {asin}")
            return '-'
        title = products[0].get('title', '-')
        logging.debug(f"get_title result for ASIN {asin}: {title[:50]}")
        return title
    except Exception as e:
        logging.error(f"get_title fetch failed for ASIN {asin}: {str(e)}")
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def tracking_since(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        start_time = products[0].get('stats', {}).get('trackedSince', None)
        if start_time:
            return datetime.fromtimestamp(start_time).strftime('%Y-%m-%d')
        return '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def categories_root(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        categories = products[0].get('categories', [])
        return categories[0] if categories else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def categories_sub(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        categories = products[0].get('categories', [])
        return ', '.join(categories[1:]) if len(categories) > 1 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def categories_tree(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        categories = products[0].get('categories', [])
        return ' > '.join(categories) if categories else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_asin(asin, api_key):
    if not validate_asin(asin):
        return '-'
    return f'="{asin}"'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def freq_bought_together(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def product_type(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        return products[0].get('type', '-') or '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def manufacturer(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        return products[0].get('manufacturer', '-') or '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def brand(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        return products[0].get('brand', '-') or '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def product_group(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        return products[0].get('productGroup', '-') or '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def variation_attributes(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def item_type(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def author(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        return products[0].get('author', '-') or '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def contributors(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def binding(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        return products[0].get('binding', '-') or '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def number_of_items(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def number_of_pages(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def publication_date(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def languages(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_quantity(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_weight(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        weight = products[0].get('packageWeight', None)
        return f"{weight / 1000:.2f} kg" if weight is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_height(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        height = products[0].get('packageHeight', None)
        return f"{height / 10:.1f} cm" if height is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_length(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        length = products[0].get('packageLength', None)
        return f"{length / 10:.1f} cm" if length is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_width(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        width = products[0].get('packageWidth', None)
        return f"{width / 10:.1f} cm" if width is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def listed_since(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        listed_time = products[0].get('listedSince', None)
        if listed_time:
            return datetime.fromtimestamp(listed_time).strftime('%Y-%m-%d')
        return '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def edition(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def release_date(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def format_type(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        sales_rank = products[0].get('stats', {}).get('salesRank', None)
        return f"{sales_rank:,}" if sales_rank is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        avg_30 = products[0].get('stats', {}).get('avg30', [None] * 11)[0]
        return f"{avg_30:,}" if avg_30 is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        avg_60 = products[0].get('stats', {}).get('avg60', [None] * 11)[0]
        return f"{avg_60:,}" if avg_60 is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        avg_90 = products[0].get('stats', {}).get('avg90', [None] * 11)[0]
        return f"{avg_90:,}" if avg_90 is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        avg_180 = products[0].get('stats', {}).get('avg180', [None] * 11)[0]
        return f"{avg_180:,}" if avg_180 is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        avg_365 = products[0].get('stats', {}).get('avg365', [None] * 11)[0]
        return f"{avg_365:,}" if avg_365 is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_drops_last_30_days(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        drops = products[0].get('stats', {}).get('salesRankDrops30', None)
        return str(drops) if drops is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_drops_last_60_days(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        drops = products[0].get('stats', {}).get('salesRankDrops60', None)
        return str(drops) if drops is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_drops_last_90_days(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        drops = products[0].get('stats', {}).get('salesRankDrops90', None)
        return str(drops) if drops is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_drops_last_180_days(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        drops = products[0].get('stats', {}).get('salesRankDrops180', None)
        return str(drops) if drops is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def sales_rank_drops_last_365_days(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        drops = products[0].get('stats', {}).get('salesRankDrops365', None)
        return str(drops) if drops is not None else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg60', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg180', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[1]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_90_days_oos(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_stock(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[10]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[10]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg60', [None] * 11)[10]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[10]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg180', [None] * 11)[10]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[10]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_90_days_oos(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_stock(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[7]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[7]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg60', [None] * 11)[7]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[7]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg180', [None] * 11)[7]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[7]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_90_days_oos(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_stock(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[9]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[9]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg60', [None] * 11)[9]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[9]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg180', [None] * 11)[9]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[9]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_90_days_oos(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fba_stock(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[8]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[8]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg60', [None] * 11)[8]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[8]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg180', [None] * 11)[8]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[8]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_90_days_oos(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def new_3rd_party_fbm_stock(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_60_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=60"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg60', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_90_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=90"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg90', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_180_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=180"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg180', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_365_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('avg365', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_lowest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_lowest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_highest(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_highest_365_days(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_90_days_oos(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def buy_box_used_stock(asin, api_key):
    return '-'  # Placeholder

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def used_current(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'
        stats = products[0].get('stats', {})
        value = stats.get('current', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def used_30_days_avg(asin, api_key):
    if not validate_asin(asin):
        return '-'
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=30"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        if response.status_code != 200:
            return '-'
        data = response.json()
        products = data.get('products', [])
        if not products:
            return '-'  # Fixed: 4-space indent
        stats = products[0].get('stats', {})
        value = stats.get('avg30', [None] * 11)[2]
        return f"${value / 100.0:.2f}" if value is not None and value >= 0 else '-'
    except Exception:
        return '-'