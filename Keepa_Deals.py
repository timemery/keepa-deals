# Chunk 1 starts
# Keepa_Deals.py
import json, csv, logging, sys, requests, urllib.parse
from stable import get_stat_value, get_title, get_asin, sales_rank_current

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

try:
    # Config & Headers
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        print(f"API key loaded: {api_key[:5]}...")
    with open('headers.json') as f:
        HEADERS = json.load(f)
        logging.debug(f"Loaded headers: {HEADERS}")
        print(f"Headers loaded: {HEADERS}")
except Exception as e:
    logging.error(f"Startup failed: {str(e)}")
    print(f"Startup failed: {str(e)}")
    sys.exit(1)
# Chunk 1 ends

# Chunk 2 starts
def fetch_deals(page):
    logging.debug(f"Fetching deals page {page}...")
    print(f"Fetching deals page {page}...")
    deal_query = {
        "page": 0,
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
    query_json = json.dumps(deal_query, separators=(',', ':'), sort_keys=True)
    encoded_selection = urllib.parse.quote(query_json)
    url = f"https://api.keepa.com/deal?key={api_key}&selection={encoded_selection}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    logging.debug(f"Deal URL: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        logging.debug(f"Deal response: {response.text}")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            print(f"Deal fetch failed: {response.status_code}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        logging.debug(f"Fetched {len(deals)} deals: {json.dumps([d['asin'] for d in deals], default=str)}")
        print(f"Fetched {len(deals)} deals")
        return [{'asin': deal['asin'], 'title': deal.get('title', '-')} for deal in deals]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []
# Chunk 2 ends

# Chunk 3 starts
def fetch_product(asin, days=365, offers=20, rating=1):
    logging.debug(f"Fetching ASIN {asin} for {days} days...")
    print(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&stock=1&buyBox=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        logging.debug(f"Response status: {response.status_code}")
        logging.debug(f"Raw response: {response.text[:500]}...")
        if response.status_code != 200:
            logging.error(f"Request failed: {response.status_code}, {response.text}")
            print(f"Request failed: {response.status_code}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"No product data for ASIN {asin}")
            print(f"No product data for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        product = products[0]
        stats = product.get('stats', {})
        current = stats.get('current', [-1] * 30)
        offers = product.get('offers', [])
        buy_box = product.get('buyBox', {})
        logging.debug(f"Raw stats for ASIN {asin}: current={current[:12]}")
        logging.debug(f"Buy Box for ASIN {asin}: {json.dumps(buy_box, default=str)}")
        logging.debug(f"Offers for ASIN {asin}: {json.dumps([{'price': o.get('price'), 'condition': o.get('condition'), 'isFBA': o.get('isFBA'), 'isBuyBox': o.get('isBuyBox')} for o in offers[:5]], default=str)}")
        print(f"Raw stats.current: {current[:12]}")
        if not stats or len(current) < 19:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            print(f"Incomplete stats for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        if current[2] <= 0:
            logging.warning(f"No Used price for ASIN {asin}: current[2]={current[2]}")
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        print(f"Fetch failed: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def used_current(product):
    stats = product.get('stats', {})
    result = {'Used - Current': get_stat_value(stats, 'current', 2, divisor=100, is_price=True)}
    logging.debug(f"used_current result: {result}")
    print(f"Used - Current for ASIN: {result}")
    return result

def buy_box_used_current(product):
    stats = product.get('stats', {})
    buy_box = product.get('buyBox', {})
    price = buy_box.get('price', -1)
    if price <= 0:
        logging.warning(f"No Buy Box Used price for ASIN {product.get('asin', '-')}: buyBox={buy_box}")
        return {'Buy Box Used - Current': '-'}
    result = {'Buy Box Used - Current': f"${price / 100:.2f}"}
    logging.debug(f"buy_box_used_current result: {result}")
    print(f"Buy Box Used - Current for ASIN: {result}")
    return result
# Chunk 3 ends

# Chunk 4 starts
def write_csv(rows, deals, diagnostic=False):
    with open('Keepa_Deals_Export.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        if diagnostic:
            writer.writerow(['No deals fetched'] + ['-'] * (len(HEADERS) - 1))
        else:
            for row, deal in zip(rows, deals[:len(rows)]):
                row_data = {}
                row_data.update(row)
                logging.debug(f"row_data keys: {row_data.keys()}")
                logging.debug(f"row_data values: {row_data}")
                print(f"Writing row for ASIN {deal['asin']}: {row_data}")
                writer.writerow([get_asin(deal) if header == 'ASIN' else get_title(deal) if header == 'Title' else row_data.get(header, '-') for header in HEADERS])
# Chunk 4 ends

# Chunk 5 starts
def main():
    try:
        logging.info("Starting Keepa_Deals...")
        print("Starting Keepa_Deals...")
        deals = fetch_deals(0)
        rows = []
        if not deals:
            logging.warning("No deals fetched, writing diagnostic CSV")
            print("No deals fetched, writing diagnostic CSV")
            write_csv([], [], diagnostic=True)
            return
        for deal in deals[:3]:
            asin = deal['asin']
            logging.info(f"Fetching ASIN {asin} ({deals.index(deal)+1}/{len(deals)})")
            product = fetch_product(asin)
            row = {}
            row.update(buy_box_used_current(product))
            row.update(sales_rank_current(product))
            row.update(used_current(product))
            rows.append(row)
        write_csv(rows, deals)
        logging.info("Writing CSV...")
        print("Writing CSV...")
        logging.info("Script completed!")
        print("Script completed!")
    except Exception as e:
        logging.error(f"Main failed: {str(e)}")
        print(f"Main failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
# Chunk 5 ends