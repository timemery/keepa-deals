# Keepa_Deals.py force change window
# Chunk 1 starts
import json, csv, logging, sys, requests, urllib.parse, time
from retrying import retry
from stable_deals import validate_asin, fetch_deals_for_deals
from field_mappings import FUNCTION_LIST

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

# Cache config and headers
try:
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        print(f"API key loaded: {api_key[:5]}...")
    with open('headers.json') as f:
        HEADERS = json.load(f)
        logging.debug(f"Loaded headers: {len(HEADERS)} fields")
        print(f"Headers loaded: {len(HEADERS)} fields")
except Exception as e:
    logging.error(f"Startup failed: {str(e)}")
    print(f"Startup failed: {str(e)}")
    sys.exit(1)
# Chunk 1 ends

# Chunk 2 starts
# 2025-05-20: Removed &buyBox=1 from fetch_product URL (commit 95aac66e) to fix Amazon - Current, but stats.current[10] still -1 for ASIN 150137012X despite $6.26 offer. Reverted to commit 31cb7bee setup. Pivoted to New - Current.
# 2025-05-22: Updated offers=100, enhanced logging (commit a03ceb87).
# 2025-05-22: Switched to Python client, offers=100 (commit 69d2801d).
# 2025-05-22: Reverted to HTTP, offers=100, added Python client fallback (commit e1f6f52e).
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_product(asin, days=365, history=1):
    if not validate_asin(asin):
        logging.error(f"Invalid ASIN format: {asin}")
        print(f"Invalid ASIN format: {asin}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}
    logging.debug(f"Fetching ASIN {asin} for {days} days, history={history}...")
    print(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&buybox=1&history={history}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Response status: {response.status_code}")
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
        logging.debug(f"HTTP Stats for ASIN {asin}: keys={list(stats.keys())}, current={current}, offers_count={len(offers)}, stats_raw={stats}")
        time.sleep(1)
        return product
    except Exception as e:
        logging.error(f"HTTP Fetch failed for ASIN {asin}: {str(e)}")
        print(f"HTTP Fetch failed: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}
# Chunk 2 ends

# Chunk 3 starts
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
                        row_data = row.copy()
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
# Chunk 3 ends

# Chunk 4 starts
def main():
    try:
        logging.info("Starting Keepa_Deals...")
        print("Starting Keepa_Deals...")
        time.sleep(2)
        deals = fetch_deals_for_deals(0)
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
            product = fetch_product(asin)
            if not product or 'stats' not in product:
                logging.error(f"Incomplete product data for ASIN {asin}")
                continue
            row = {}
            try:
                # Process all functions using FUNCTION_LIST
                for header, func in zip(HEADERS, FUNCTION_LIST):
                    if func:
                        try:
                            # Pass deal for stable_deals functions, product for stable_products
                            input_data = deal if header in ['Deal found', 'last update', 'last price change'] else product
                            result = func(input_data)
                            row.update(result)
                        except Exception as e:
                            logging.error(f"Function {func.__name__} failed for ASIN {asin}: {str(e)}")
                            row[header] = '-'
                rows.append(row)
            except Exception as e:
                logging.error(f"Error processing ASIN {asin}: {str(e)}")
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
# Chunk 4 ends

if __name__ == "__main__":
    main()

#### END OF FILE ####