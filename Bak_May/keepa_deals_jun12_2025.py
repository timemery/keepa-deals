# Keepa_Deals.py
# Chunk 1 starts
import json, csv, logging, sys, requests, urllib.parse, time, datetime
from retrying import retry
from stable_deals import validate_asin, deal_found
from stable_products import (
    get_asin, get_title, package_quantity,
    sales_rank_current, sales_rank_30_days_avg, sales_rank_90_days_avg,
    sales_rank_180_days_avg, sales_rank_365_days_avg, used_current,
    package_weight, package_height, package_length, package_width,
    used_like_new, used_very_good, used_good, used_acceptable,
    new_3rd_party_fbm_current, new_3rd_party_fbm, list_price,
    get_stat_value
)

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

# Cache headers globally
try:
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        print(f"API key loaded: {api_key[:5]}...")
    with open('headers.json') as f:
        global HEADERS
        HEADERS = json.load(f)
        logging.debug(f"Loaded headers: {len(HEADERS)} fields")
        print(f"Headers loaded: {len(HEADERS)} fields")
except Exception as e:
    logging.error(f"Startup failed: {str(e)}")
    print(f"Startup failed: {str(e)}")
    sys.exit(1)
# Chunk 1 ends

# Chunk 2 starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_deals(page):
    logging.debug(f"Fetching deals page {page}...")
    print(f"Fetching deals page {page}...")
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
    query_json = json.dumps(deal_query, separators=(',', ':'), sort_keys=True)
    encoded_selection = urllib.parse.quote(query_json)
    url = f"https://api.keepa.com/deal?key={api_key}&selection={encoded_selection}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    logging.debug(f"Deal URL: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Deal response: {response.text[:500]}...")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            print(f"Deal fetch failed: {response.status_code}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        date_index = data.get('drDateIndex', {})
        logging.debug(f"Fetched {len(deals)} deals: {[d.get('asin', '-') for d in deals[:5]]}")
        logging.debug(f"fetch_deals raw response keys: {list(data.get('deals', {}).keys())}")
        logging.debug(f"First deal object keys: {[list(d.keys()) for d in deals[:1]]}")
        print(f"Fetched {len(deals)} deals")
        return [{'deal': deal, 'date_index': date_index} for deal in deals[:5]]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []
# Chunk 2 ends

# Chunk 3 starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_product(asin, days=365, offers=20, rating=1, history=1):
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        print(f"Invalid ASIN format: {asin}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}
    logging.debug(f"Fetching ASIN {asin} for {days} days, history={history}, offers={offers}...")
    print(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&stock=1&buyBox=1&history={history}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
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
        logging.debug(f"Stats structure for ASIN {asin}: keys={list(stats.keys())}, current_length={len(current)}")
        csv_field = product.get('csv', [[] for _ in range(11)])
        logging.debug(f"CSV field for ASIN {asin}: {[len(x) if isinstance(x, list) else 'None' for x in csv_field[:11]]}")
        logging.debug(f"CSV raw data for ASIN {asin}: {[x[:10] if isinstance(x, list) else x for x in csv_field[:11]]}")
        logging.debug(f"Sales Rank Drops for ASIN {asin}: drops90={stats.get('salesDrops90', -1)}")
        time.sleep(1)
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        print(f"Fetch failed: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def list_price(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'List Price - 30 days avg.': get_stat_value(stats, 'avg30', 8, divisor=100, is_price=True),
        'List Price - 60 days avg.': get_stat_value(stats, 'avg60', 8, divisor=100, is_price=True),
        'List Price - 90 days avg.': get_stat_value(stats, 'avg90', 8, divisor=100, is_price=True),
        'List Price - 180 days avg.': get_stat_value(stats, 'avg180', 8, divisor=100, is_price=True),
        'List Price - 365 days avg.': get_stat_value(stats, 'avg365', 8, divisor=100, is_price=True),
        'List Price - 90 days OOS': get_stat_value(stats, 'outOfStock90', 8, is_price=False),
        'List Price - Stock': '-'
    }
    logging.debug(f"list_price result for ASIN {asin}: {result}")
    print(f"List Price for ASIN {asin}: {result}")
    return result

def new_3rd_party_fbm(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'New' and not o.get('isFBA', False) and o.get('stock', 0) > 0)
    result = {
        'New, 3rd Party FBM - 30 days avg.': get_stat_value(stats, 'avg30', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 60 days avg.': get_stat_value(stats, 'avg60', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 90 days avg.': get_stat_value(stats, 'avg90', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 180 days avg.': get_stat_value(stats, 'avg180', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 365 days avg.': get_stat_value(stats, 'avg365', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"new_3rd_party_fbm result for ASIN {asin}: {result}")
    print(f"New, 3rd Party FBM for ASIN {asin}: {result}")
    return result

def used_like_new(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Like New' and o.get('stock', 0) > 0)
    result = {
        'Used, like new - 30 days avg.': get_stat_value(stats, 'avg30', 4, divisor=100, is_price=True),
        'Used, like new - 60 days avg.': get_stat_value(stats, 'avg60', 4, divisor=100, is_price=True),
        'Used, like new - 90 days avg.': get_stat_value(stats, 'avg90', 4, divisor=100, is_price=True),
        'Used, like new - 180 days avg.': get_stat_value(stats, 'avg180', 4, divisor=100, is_price=True),
        'Used, like new - 365 days avg.': get_stat_value(stats, 'avg365', 4, divisor=100, is_price=True),
        'Used, like new - 90 days OOS': get_stat_value(stats, 'outOfStock90', 4, is_price=False),
        'Used, like new - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_like_new result for ASIN {asin}: {result}")
    print(f"Used, like new for ASIN {asin}: {result}")
    return result

def used_very_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Very Good' and o.get('stock', 0) > 0)
    result = {
        'Used, very good - 30 days avg.': get_stat_value(stats, 'avg30', 5, divisor=100, is_price=True),
        'Used, very good - 60 days avg.': get_stat_value(stats, 'avg60', 5, divisor=100, is_price=True),
        'Used, very good - 90 days avg.': get_stat_value(stats, 'avg90', 5, divisor=100, is_price=True),
        'Used, very good - 180 days avg.': get_stat_value(stats, 'avg180', 5, divisor=100, is_price=True),
        'Used, very good - 365 days avg.': get_stat_value(stats, 'avg365', 5, divisor=100, is_price=True),
        'Used, very good - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_very_good result for ASIN {asin}: {result}")
    print(f"Used, very good for ASIN {asin}: {result}")
    return result

def used_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Good' and o.get('stock', 0) > 0)
    result = {
        'Used, good - 30 days avg.': get_stat_value(stats, 'avg30', 6, divisor=100, is_price=True),
        'Used, good - 60 days avg.': get_stat_value(stats, 'avg60', 6, divisor=100, is_price=True),
        'Used, good - 90 days avg.': get_stat_value(stats, 'avg90', 6, divisor=100, is_price=True),
        'Used, good - 180 days avg.': get_stat_value(stats, 'avg180', 6, divisor=100, is_price=True),
        'Used, good - 365 days avg.': get_stat_value(stats, 'avg365', 6, divisor=100, is_price=True),
        'Used, good - 90 days OOS': get_stat_value(stats, 'outOfStock90', 6, is_price=False),
        'Used, good - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_good result for ASIN {asin}: {result}")
    print(f"Used, good for ASIN {asin}: {result}")
    return result

def used_acceptable(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Acceptable' and o.get('stock', 0) > 0)
    result = {
        'Used, acceptable - 30 days avg.': get_stat_value(stats, 'avg30', 7, divisor=100, is_price=True),
        'Used, acceptable - 60 days avg.': get_stat_value(stats, 'avg60', 7, divisor=100, is_price=True),
        'Used, acceptable - 90 days avg.': get_stat_value(stats, 'avg90', 7, divisor=100, is_price=True),
        'Used, acceptable - 180 days avg.': get_stat_value(stats, 'avg180', 7, divisor=100, is_price=True),
        'Used, acceptable - 365 days avg.': get_stat_value(stats, 'avg365', 7, divisor=100, is_price=True),
        'Used, acceptable - 90 days OOS': get_stat_value(stats, 'outOfStock90', 7, is_price=False),
        'Used, acceptable - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_acceptable result for ASIN {asin}: {result}")
    print(f"Used, acceptable for ASIN {asin}: {result}")
    return result
# Chunk 3 ends

# Chunk 4 starts
def write_csv(rows, deals, diagnostic=False):
    try:
        with open('Keepa_Deals_Export.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            if diagnostic:
                writer.writerow(['No deals fetched'] + ['-'] * (len(HEADERS) - 1))
                logging.info(f"Diagnostic CSV written: Keepa_Deals_Export.csv")
                print(f"Diagnostic CSV written: Keepa_Deals_Export.csv")
            else:
                for deal_data, row in zip(deals[:len(rows)], rows):
                    deal = deal_data['deal']
                    try:
                        row_data = {
                            'ASIN': row.get('ASIN', '-'),
                            'Title': row.get('Title', '-'),
                            'Package - Quantity': row.get('Package - Quantity', '-')
                        }
                        row_data.update(row)
                        missing_headers = [h for h in HEADERS if h not in row_data]
                        if missing_headers:
                            logging.warning(f"Missing headers for ASIN {deal.get('asin', '-')}: {missing_headers[:5]}")
                        logging.debug(f"row_data for ASIN {deal.get('asin', '-')}: {list(row_data.keys())[:10]}")
                        print(f"Writing row for ASIN {deal.get('asin', '-')}...")
                        writer.writerow([row_data.get(header, '-') for header in HEADERS])
                        logging.debug(f"Wrote row for ASIN {deal.get('asin', '-')}")
                    except Exception as e:
                        logging.error(f"Failed to write row for ASIN {deal.get('asin', '-')}: {str(e)}")
                        print(f"Failed to write row for ASIN {deal.get('asin', '-')}: {str(e)}")
        logging.info(f"CSV written: Keepa_Deals_Export.csv")
        print(f"CSV written: Keepa_Deals_Export.csv")
    except Exception as e:
        logging.error(f"Failed to write CSV Keepa_Deals_Export.csv: {str(e)}")
        print(f"Failed to write CSV Keepa_Deals_Export.csv: {str(e)}")
# Chunk 4 ends

# Chunk 5 starts
def main():
    try:
        logging.info("Starting Keepa_Deals...")
        print("Starting Keepa_Deals...")
        time.sleep(2)
        deals = fetch_deals(0)
        rows = []
        if not deals:
            logging.warning("No deals fetched, writing diagnostic CSV")
            print("No deals fetched, writing diagnostic CSV")
            write_csv([], [], diagnostic=True)
            return
        logging.debug(f"Deals ASINs: {[d['deal'].get('asin', '-') for d in deals[:5]]}")
        print(f"Deals ASINs: {[d['deal'].get('asin', '-') for d in deals[:5]]}")
        for deal_data in deals:
            deal = deal_data['deal']
            date_index = deal_data['date_index']
            asin = deal.get('asin', '-')
            if not validate_asin(asin):
                logging.warning(f"Skipping invalid ASIN for deal {deals.index(deal_data)+1}")
                continue
            logging.info(f"Fetching ASIN {asin} ({deals.index(deal_data)+1}/{len(deals)})")
            product = fetch_product(asin)
            row = {}
            row.update({'Deal found': deal_found(deal, date_index)['Deal found']})
            row.update({'ASIN': get_asin(asin, api_key)['ASIN']})
            row.update({'Title': get_title(asin, api_key)['Title']})
            row.update({'Package - Quantity': package_quantity(asin, api_key)['Package - Quantity']})
            functions = [
                sales_rank_current, sales_rank_30_days_avg, sales_rank_90_days_avg,
                sales_rank_180_days_avg, sales_rank_365_days_avg, used_current,
                package_weight, package_height, package_length, package_width,
                used_like_new, used_very_good, used_good, used_acceptable,
                new_3rd_party_fbm_current, new_3rd_party_fbm, list_price
            ]
            for func in functions:
                row.update(func(product))
            rows.append(row)
        write_csv(rows, deals)
        logging.info("Writing CSV...")
        print("Writing CSV...")
        logging.info("Script completed!")
        print("Script completed!")
        print(f"Processed ASINs: {[row.get('ASIN', '-') for row in rows]}")
    except Exception as e:
        logging.error(f"Main failed: {str(e)}")
        print(f"Main failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
# Chunk 5 ends