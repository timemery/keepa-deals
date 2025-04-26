# Keepa_Deals.py
import json, csv, logging, keepa, sys, requests, urllib.parse

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

try:
    # Config & Headers
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
    with open('headers.json') as f:
        HEADERS = json.load(f)
except Exception as e:
    logging.error(f"Startup failed: {str(e)}")
    sys.exit(1)

def fetch_deals(page):
    logging.debug(f"Fetching deals page {page}...")
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
    logging.debug(f"Raw query JSON: {query_json}")
    try:
        response = requests.get(url, headers=headers)
        logging.debug(f"Deal response: {response.text}")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        logging.debug(f"Fetched {len(deals)} deals: {json.dumps([d['asin'] for d in deals], default=str)}")
        return [{'asin': deal['asin'], 'title': deal.get('title', '-')} for deal in deals]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        return []

def fetch_product(asin, days, api):
    logging.debug(f"Fetching ASIN {asin} for {days} days...")
    try:
        product = api.query(asin, stats=days, offers=20, stock=True, rating=True, update=0)[0]
        if not product:
            logging.error(f"No product data for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        stats = product.get('stats', {})
        current = stats.get('current', [-1] * 30)
        logging.debug(f"Raw stats for ASIN {asin}: current={current[:16]}")
        if not stats or len(current) < 11:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        if current[2] <= 0:
            logging.warning(f"No Used price for ASIN {asin}: current[2]={current[2]}")
        if current[10] <= 0:
            logging.warning(f"No Sales Rank for ASIN {asin}: current[10]={current[10]}")
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def get_stat_value(stats, key, index, divisor=1000):
    value = stats.get(key, [-1] * 30)[index]
    if isinstance(value, list):  # Handle tuples [price, timestamp]
        value = value[0] if value else -1
    if value <= 0:
        return '-'
    return f"${value / divisor:.2f}" if index == 10 else str(value)

def sales_rank_current(product):
    stats = product.get('stats', {})
    return {'Sales Rank - Current': get_stat_value(stats, 'current', 2, 1)}

def buy_box_used_current(product):
    stats = product.get('stats', {})
    return {'Buy Box Used - Current': get_stat_value(stats, 'current', 10)}

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
                writer.writerow([deal['asin'] if header == 'ASIN' else deal['title'] if header == 'Title' else row_data.get(header, '-') for header in HEADERS])

def main():
    try:
        logging.info("Starting Keepa_Deals...")
        api = keepa.Keepa(api_key)
        deals = fetch_deals(0)
        rows = []
        if not deals:
            logging.warning("No deals fetched, writing diagnostic CSV")
            write_csv([], [], diagnostic=True)
            return
        for deal in deals[:3]:
            asin = deal['asin']
            product = fetch_product(asin, 365, api)
            row = {}
            row.update(sales_rank_current(product))
            row.update(buy_box_used_current(product))
            rows.append(row)
        write_csv(rows, deals)
        logging.info("Script completed!")
    except Exception as e:
        logging.error(f"Main failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()