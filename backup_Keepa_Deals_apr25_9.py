# Keepa_Deals.py
import json, csv, logging, keepa, sys
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
    return [
        {'asin': '1954966032', 'title': 'Airick Flies High'},
        {'asin': '1440245010', 'title': 'Gun Digest Book of Modern Gun Values'},
        {'asin': '0914671227', 'title': 'Absolute Solitude: Selected Poems'}
    ]

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_product(asin, days, api):
    logging.debug(f"Fetching ASIN {asin} for {days} days...")
    try:
        product = api.query(asin, stats=days, offers=20, rating=True, update=0)[0]
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
    buy_box_used = data.get('buyBoxUsedPrice', -1)
    logging.debug(f"Price Now for ASIN {asin}: buyBoxUsedPrice={buy_box_used}")
    if buy_box_used > 0 and buy_box_used < 100000:
        return f"${buy_box_used / 100:.2f}"
    offers = data.get('offers', [])
    valid_prices = [offer['price'] for offer in offers if offer.get('price', -1) > 0]
    logging.debug(f"Price Now for ASIN {asin}: valid_offer_prices={valid_prices}")
    if not valid_prices:
        return '-'
    value = min(valid_prices)
    if value > 100000:
        return '-'
    return f"${value / 100:.2f}"

def validate_header(asin, header, value, api):
    product = api.query(asin, stats=365, offers=20, rating=True, update=0)[0]
    buy_box_used = product.get('buyBoxUsedPrice', -1)
    expected = buy_box_used / 100 if buy_box_used > 0 else -1
    if expected <= 0:
        offers = product.get('offers', [])
        valid_prices = [offer['price'] for offer in offers if offer.get('price', -1) > 0]
        expected = min(valid_prices) / 100 if valid_prices else -1
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