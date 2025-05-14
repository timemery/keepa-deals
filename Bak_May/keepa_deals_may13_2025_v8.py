# Keepa_Deals.py
# Chunk 1 starts
import json, csv, logging, sys, requests, urllib.parse, time, datetime
from retrying import retry
from stable_deals import validate_asin, fetch_deals_for_deals, deal_found, last_update, last_price_change
from stable_products import (
    get_asin, get_title, package_quantity,
    sales_rank_current, sales_rank_30_days_avg, sales_rank_90_days_avg,
    sales_rank_180_days_avg, sales_rank_365_days_avg, used_current,
    package_weight, package_height, package_length, package_width,
    used_like_new, used_very_good, used_good, used_acceptable,
    new_3rd_party_fbm_current, new_3rd_party_fbm, list_price,
    get_stat_value, percent_down_90, amz_link, keepa_link,
    categories_root, categories_sub, categories_tree, tracking_since,
    manufacturer, brand, product_group, author, contributors, binding
)
from stable_calculations import *  # Empty import

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
        "dateRange": "3"
    }
    query_json = json.dumps(deal_query, separators=(',', ':'), sort_keys=True)
    logging.debug(f"Raw query JSON: {query_json}")
    encoded_selection = urllib.parse.quote(query_json)
    url = f"https://api.keepa.com/deal?key={api_key}&selection={encoded_selection}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    logging.debug(f"Deal URL: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Full deal response: {response.text}")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            print(f"Deal fetch failed: {response.status_code}, {response.text}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        logging.debug(f"Fetched {len(deals)} deals: {[d.get('asin', '-') for d in deals]}")
        logging.debug(f"Deal response structure: {list(data.get('deals', {}).keys())}")
        logging.debug(f"All deal keys: {[list(d.keys()) for d in deals]}")
        logging.debug(f"Full deals: {deals}")
        print(f"Fetched {len(deals)} deals")
        return deals[:5]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []
# Chunk 2 ends

# Chunk 3 starts
# Fetch Product Logging - start
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def fetch_product(asin, api_key, days=365, offers=20, rating=1, history=1):
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        print(f"Invalid ASIN format: {asin}")
        return None
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&stock=1&buyBox=1&history={history}"
    logging.debug(f"Fetching ASIN {asin} for {days} days, history={history}, offers={offers}, url={url}")
    print(f"Fetching ASIN {asin}...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=60)
        logging.debug(f"Response status: {response.status_code}, headers={response.headers}, response_text={response.text[:1000]}")
        if response.status_code != 200:
            logging.error(f"Request failed: {response.status_code}, headers={response.headers}, response_text={response.text[:500]}")
            print(f"Request failed: {response.status_code}")
            return None
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"No product data for ASIN {asin}, data_keys={list(data.keys())}")
            print(f"No product data for ASIN {asin}")
            return None
        product = products[0]
        stats = product.get('stats', {})
        current = stats.get('current', [-1] * 30)
        logging.debug(f"Stats structure for ASIN {asin}: keys={list(stats.keys())}, current_length={len(current)}, product_keys={list(product.keys())[:20]}")
        csv_field = product.get('csv', [[] for _ in range(11)])
        logging.debug(f"CSV field for ASIN {asin}: {[len(x) if isinstance(x, list) else 'None' for x in csv_field[:11]]}")
        logging.debug(f"CSV raw data for ASIN {asin}: {[x[:10] if isinstance(x, list) else x for x in csv_field[:11]]}")
        logging.debug(f"Sales Rank Drops for ASIN {asin}: drops90={stats.get('salesDrops90', -1)}")
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}, url={url}")
        print(f"Fetch failed: {str(e)}")
        return None
# Fetch Product Logging - end

# Removed conflicting multi-field definitions of list_price, new_3rd_party_fbm, used_like_new, used_very_good, used_good, used_acceptable to restore stable_products.py logic

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
                for deal, row in zip(deals[:len(rows)], rows):
                    try:
                        row_data = {
                            'ASIN': row.get('ASIN', '-'),
                            'Title': row.get('Title', '-'),
                            'Package - Quantity': row.get('Package - Quantity', '-')
                        }
                        row_data.update(row)
                        missing_headers = [h for h in HEADERS if h not in row_data]
                        extra_headers = [k for k in row_data if k not in HEADERS]
                        if missing_headers or extra_headers:
                            logging.warning(f"Header mismatch for ASIN {deal.get('asin', '-')}: Missing={missing_headers[:5]}, Extra={extra_headers[:5]}")
                        logging.debug(f"row_data for ASIN {deal.get('asin', '-')}: keys={list(row_data.keys())[:20]}, values={list(row_data.values())[:20]}")
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
# Local simple field functions - start
# removed this for some reason... 
# Local simple field functions - end    
def main():
    try:
        logging.info("Starting Keepa_Deals...")
        print("Starting Keepa_Deals...")
        time.sleep(2)
        deals = fetch_deals_for_deals(0)  # Fixed: Removed api_key argument
        rows = []
        if not deals:
            logging.warning("No deals fetched, writing diagnostic CSV")
            print("No deals fetched, writing diagnostic CSV")
            write_csv([], [], diagnostic=True)
            return
        logging.debug(f"Deals ASINs: {[d.get('asin', '-') for d in deals[:5]]}")
        print(f"Deals ASINs: {[d.get('asin', '-') for d in deals[:5]]}")
        for deal in deals:
            asin = deal.get('asin', '-')
            if not validate_asin(asin):
                logging.warning(f"Skipping invalid ASIN for deal {deals.index(deal)+1}")
                continue
            logging.info(f"Fetching ASIN {asin} ({deals.index(deal)+1}/{len(deals)})")
# or maybe this is fetch product - start
            product = fetch_product(asin, api_key)
            if not product or not isinstance(product, dict) or 'asin' not in product or 'stats' not in product:
                logging.error(f"Invalid or incomplete product data for ASIN {asin}: {product}")
                continue
            row = {}
            try:
                logging.debug(f"Processing deal: {deal}")
                logging.debug(f"Processing product: {product}")
                row.update({'Percent Down 90': percent_down_90(product)['Percent Down 90']})
                row.update({'Deal found': deal_found(deal)['Deal found']})
                row.update({'last update': last_update(deal)['last update']})
                row.update({'last price change': last_price_change(deal)['last price change']})
                row.update({'Tracking since': tracking_since(product)['Tracking since']})
                row.update({'ASIN': get_asin(asin, api_key)['ASIN']})
                row.update({'Title': get_title(asin, api_key)['Title']})
                row.update({'Package - Quantity': package_quantity(asin, api_key)['Package - Quantity']})
#                row.update({'ASIN': get_asin(asin, api_key)['ASIN']})
#                row.update({'Title': get_title(asin, api_key)['Title']})
#                row.update({'Package - Quantity': package_quantity(asin, api_key)['Package - Quantity']})
# or maybe this is fetch product - end
                functions = [
                    sales_rank_current, sales_rank_30_days_avg, sales_rank_90_days_avg,
                    sales_rank_180_days_avg, sales_rank_365_days_avg, used_current,
                    package_weight, package_height, package_length, package_width,
                    used_like_new, used_very_good, used_good, used_acceptable,
                    new_3rd_party_fbm_current, list_price,
                    amz_link, keepa_link, categories_root, categories_sub, categories_tree,
                    tracking_since, manufacturer, brand, product_group,
                    author, contributors, binding
                ]
                for func in functions:
                    row.update(func(product))
                rows.append(row)
            except Exception as e:
                logging.error(f"Error processing ASIN {asin}: {str(e)}, row_keys={list(row.keys())[:10]}, product_keys={list(product.keys())[:20] if product else 'None'}")
                continue
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