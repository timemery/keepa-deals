# Keepa_Deals.py
import json, csv, logging, keepa, sys, requests, urllib.parse
from retrying import retry

# Config & Headers
with open('config.json') as f:
    config = json.load(f)
    api_key = config['api_key']
with open('headers.json') as f:
    HEADERS = json.load(f)

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_deals(page):
    logging.debug(f"Fetching deals page {page}...")
    deal_query = {
        "page": page,
        "domainId": "1",
        "excludeCategories": [],
        "includeCategories": [283155],
        "priceTypes": [2],
        "deltaRange": [1950, 9900],
        "deltaPercentRange": [50, 2147483647],
        "salesRankRange": [50000, 1500000],
        "currentRange": [2000, 30100],
        "minRating": 10,
        "isLowest": False,
        "isLowest90": False,
        "isLowestOffer": False,
        "isOutOfStock": False,
        "titleSearch": "",
        "isRangeEnabled": True,
        "isFilterEnabled": True,
        "filterErotic": False,
        "singleVariation": True,
        "hasReviews": False,
        "isPrimeExclusive": False,
        "mustHaveAmazonOffer": False,
        "mustNotHaveAmazonOffer": False,
        "sortType": 4,
        "dateRange": "3",
        "warehouseConditions": [2, 3, 4, 5]
    }
    encoded_selection = urllib.parse.quote_plus(urllib.parse.quote(json.dumps(deal_query, separators=(',', ':'), sort_keys=True)))
    url = f"https://api.keepa.com/deal?key={api_key}&selection={encoded_selection}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    logging.debug(f"Deal URL: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
        return []
    data = response.json()
    deals = data.get('deals', {}).get('dr', [])
    logging.debug(f"Fetched {len(deals)} deals: {json.dumps([d['asin'] for d in deals], default=str)}")
    return [{'asin': deal['asin'], 'title': deal.get('title', '-')} for deal in deals]

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_product(asin, days, api):
    logging.debug(f"Fetching ASIN {asin} for {days} days...")
    try:
        product = api.query(asin, stats=days, offers=20, stock=True, rating=True, update=0)[0]
        if not product:
            logging.error(f"No product data for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        stats = product.get('stats', {})
        logging.debug(f"Raw stats for ASIN {asin}: {json.dumps(stats, default=str)}")
        if not stats or len(stats.get('current', [])) < 20:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def get_stat_value(stats, key, index, divisor=100, is_percentage=False):
    value = stats.get(key, [-1] * 30)[index]
    if isinstance(value, list):  # Handle min/max arrays [price, timestamp]
        value = value[0] if value else -1
    if value <= 0:
        return '-'
    return f"{value / divisor:.2f}%" if is_percentage else f"${value / divisor:.2f}"

def get_stock_value(product, key):
    return '-'  # DROP sparse fields as per data types document

def buy_box_used_fields(product):
    stats = product.get('stats', {})
    return {
        'Buy Box Used - Current': get_stat_value(stats, 'current', 3),
        'Buy Box Used - 30 days avg.': get_stat_value(stats, 'avg30', 3),
        'Buy Box Used - 60 days avg.': get_stat_value(stats, 'avg', 3),
        'Buy Box Used - 90 days avg.': get_stat_value(stats, 'avg90', 3),
        'Buy Box Used - 180 days avg.': get_stat_value(stats, 'avg180', 3),
        'Buy Box Used - 365 days avg.': get_stat_value(stats, 'avg365', 3),
        'Buy Box Used - Lowest': get_stat_value(stats, 'min', 3),
        'Buy Box Used - Lowest 365 days': get_stat_value(stats, 'min365', 3),
        'Buy Box Used - Highest': get_stat_value(stats, 'max', 3),
        'Buy Box Used - Highest 365 days': get_stat_value(stats, 'max365', 3),
        'Buy Box Used - 90 days OOS': get_stat_value(stats, 'outOfStockPercentage90', 3, divisor=1, is_percentage=True),
        'Buy Box Used - Stock': get_stock_value(product, 'stockBuyBoxUsed')
    }

def used_fields(product):
    stats = product.get('stats', {})
    return {
        'Used - Current': get_stat_value(stats, 'current', 10),
        'Used - 30 days avg.': get_stat_value(stats, 'avg30', 10),
        'Used - 60 days avg.': get_stat_value(stats, 'avg', 10),
        'Used - 90 days avg.': get_stat_value(stats, 'avg90', 10),
        'Used - 180 days avg.': get_stat_value(stats, 'avg180', 10),
        'Used - 365 days avg.': get_stat_value(stats, 'avg365', 10),
        'Used - Lowest': get_stat_value(stats, 'min', 10),
        'Used - Lowest 365 days': get_stat_value(stats, 'min365', 10),
        'Used - Highest': get_stat_value(stats, 'max', 10),
        'Used - Highest 365 days': get_stat_value(stats, 'max365', 10),
        'Used - 90 days OOS': get_stat_value(stats, 'outOfStockPercentage90', 10, divisor=1, is_percentage=True),
        'Used - Stock': get_stock_value(product, 'stockUsed')
    }

def used_condition_fields(product, condition, index):
    stats = product.get('stats', {})
    prefix = f"Used, {condition} - "
    stock_key = f"stockUsed{condition.replace(' ', '').replace(',', '')}"
    return {
        f"{prefix}Current": get_stat_value(stats, 'current', index),
        f"{prefix}30 days avg.": get_stat_value(stats, 'avg30', index),
        f"{prefix}60 days avg.": get_stat_value(stats, 'avg', index),
        f"{prefix}90 days avg.": get_stat_value(stats, 'avg90', index),
        f"{prefix}180 days avg.": get_stat_value(stats, 'avg180', index),
        f"{prefix}365 days avg.": get_stat_value(stats, 'avg365', index),
        f"{prefix}Lowest": get_stat_value(stats, 'min', index),
        f"{prefix}Lowest 365 days": get_stat_value(stats, 'min365', index),
        f"{prefix}Highest": get_stat_value(stats, 'max', index),
        f"{prefix}Highest 365 days": get_stat_value(stats, 'max365', index),
        f"{prefix}90 days OOS": get_stat_value(stats, 'outOfStockPercentage90', index, divisor=1, is_percentage=True),
        f"{prefix}Stock": get_stock_value(product, stock_key)
    }

def write_csv(rows):
    with open('Keepa_Deals_Export.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(rows)

def main():
    logging.info("Starting Keepa_Deals...")
    api = keepa.Keepa(api_key)
    deals = fetch_deals(0)
    rows = []
    for deal in deals[:3]:
        asin = deal['asin']
        product = fetch_product(asin, 365, api)
        row = []
        field_values = {
            **buy_box_used_fields(product),
            **used_fields(product),
            **used_condition_fields(product, 'like new', 12),
            **used_condition_fields(product, 'very good', 13),
            **used_condition_fields(product, 'good', 14),
            **used_condition_fields(product, 'acceptable', 15)
        }
        for header in HEADERS:
            value = field_values.get(header, '-')
            row.append(value)
        rows.append(row)
    write_csv(rows)
    logging.info("Script completed!")

if __name__ == "__main__":
    main()