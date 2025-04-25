# Keepa_Deals.py
import json, csv, logging, keepa, sys
from retrying import retry

# Config & Headers
with open('config.json') as f:
    config = json.load(f)
    api_key = config['api_key']
with open('headers.json') as f:
    HEADERS = json.load(f)['headers']

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_deals(page):
    # Static parameters, no shared filters
    logging.debug(f"Fetching deals page {page}...")
    return [{'asin': '1954966032', 'title': 'Airick Flies High'}]  # Placeholder for testing

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_product(asin, days, api):
    logging.debug(f"Fetching ASIN {asin} for {days} days...")
    try:
        products = api.query(asin, stats=days, offers=20, rating=1, update=0, force=True)
        if not products or not isinstance(products, list) or len(products) == 0:
            logging.error(f"No product data for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        product = products[0]
        stats = product.get('stats', {})
        logging.debug(f"Raw stats for ASIN {asin}: {json.dumps(stats, default=str)}")
        if not stats or len(stats.get('current', [])) < 16:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def price_now(data):
    stats = data.get('stats', {}).get('current', [-1] * 30)
    value = stats[3] if len(stats) > 3 else -1
    logging.debug(f"Price Now for ASIN {data.get('asin', '-')}: stats.current[3]={value}")
    return f"${value / 100:.2f}" if value > 0 else '-'

def validate_header(asin, header, value, api):
    product = api.query(asin, stats=365, offers=20, rating=1, update=0, force=True)[0]
    expected = product.get('stats', {}).get('current', [-1])[3] / 100
    logging.debug(f"Validate {header} for {asin}: got {value}, expected ${expected:.2f}")
    return value == f"${expected:.2f}"

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
    for deal in deals[:10]:
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