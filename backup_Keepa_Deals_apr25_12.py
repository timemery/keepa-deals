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
    encoded_selection = urllib.parse.quote_plus(json.dumps(deal_query, separators=(',', ':'), sort_keys=True))
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
        offers = product.get('offers', [])
        logging.debug(f"Raw stats for ASIN {asin}: {json.dumps(stats, default=str)}")
        logging.debug(f"Offers for ASIN {asin}: {json.dumps(offers, default=str)}")
        if not stats or len(stats.get('current', [])) < 20:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def price_now(data):
    asin = data.get('asin', '-')
    stats = data.get('stats', {}).get('current', [-1] * 30)
    buy_box_used = stats[3] if len(stats) > 3 else -1
    logging.debug(f"Price Now for ASIN {asin}: stats.current[3]={buy_box_used}")
    if buy_box_used > 0 and buy_box_used < 100000:
        return f"${buy_box_used / 100:.2f}"
    return '-'

def validate_header(asin, header, value, api):
    product = api.query(asin, stats=365, offers=20, stock=True, rating=True, update=0)[0]
    stats = product.get('stats', {}).get('current', [-1] * 30)
    buy_box_used = stats[3] if len(stats) > 3 else -1
    expected = buy_box_used / 100 if buy_box_used > 0 else -1
    logging.debug(f"Validate {header} for {asin}: got {value}, expected ${expected:.2f}")
    return value == f"${expected:.2f}" or expected <= 0

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
        for header in HEADERS:
            value = price_now(product) if header == "Price Now" else '-'
            if header == "Price Now" and not validate_header(asin, header, value, api):
                logging.error(f"Validation failed for {asin}, {header}: {value}")
            row.append(value)
        rows.append(row)
    write_csv(rows)
    logging.info("Script completed!")

if __name__ == "__main__":
    main()