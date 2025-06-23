# Keepa_Deals.py 
# (Last update: Version 5.1)

# Chunk 1 starts: 
# Added argparse
import json, csv, logging, sys, requests, urllib.parse, time, argparse
from retrying import retry
from stable_deals import validate_asin, fetch_deals_for_deals
from field_mappings import FUNCTION_LIST
import os
CSV_PATH = os.path.join(os.path.dirname(__file__), "Keepa_Deals_Export.csv")

# Logging for terminal and file output - starts
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Terminal output
        logging.FileHandler('debug_log.txt')  # File output
    ]
)
# Force flush for real-time output
logging.getLogger().handlers[0].flush = sys.stdout.flush

#kept but commented out in case the new one above doesn't work.
#logging.basicConfig(
#    level=logging.INFO,
#    format='%(asctime)s - %(message)s',
#    handlers=[
#        logging.StreamHandler(),  # Terminal output
#        logging.FileHandler('debug_log.txt')  # File output
#    ]
#)
#logger = logging.getLogger(__name__)
# Logging for terminal and file output - ends

# Command-line arguments
parser = argparse.ArgumentParser(description="Keepa Deals Script")
parser.add_argument("--no-cache", action="store_true", help="Force fresh Keepa API calls")
# args = parser.parse_args() # Moved to main()

# Logging - removed this one since we have a new/better one above
#logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

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
# 2025-05-22: Increased timeout=60, wait_fixed=10000, sleep=2 to fix timeouts for ASINs 1848638930, B0CS6RL7D6, B0C1VSRNNH.
# 2025-05-26: Added --no-cache flag to force fresh API calls.
@retry(stop_max_attempt_number=3, wait_fixed=10000)
def fetch_product(asin, days=365, offers=100, rating=1, history=1):
    if not validate_asin(asin):
        logging.error(f"Invalid ASIN format: {asin}")
        print(f"Invalid ASIN format: {asin}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}
    logging.debug(f"Fetching ASIN {asin} for {days} days, history={history}, offers={offers}, no_cache={args.no_cache}")
    print(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&history={history}&stock=1&buybox=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=60)
        logging.debug(f"Response status: {response.status_code}, url={url}")
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
        offers = product.get('offers', []) if product.get('offers') is not None else []
        logging.info(f"HTTP Stats for ASIN {asin}: keys={list(stats.keys())}, current={current}, current_length={len(current)}, offers_count={len(offers)}, stats_raw={stats}")
        if len(current) < 11:
            logging.warning(f"Short current array for ASIN {asin}: {current}")
        if current[1] == -1:
            logging.warning(f"Invalid Amazon - Current price for ASIN {asin}: current[1]={current[1]}")
        time.sleep(2)  # Restore delay to avoid rate limits
        return product
    except Exception as e:
        logging.error(f"HTTP Fetch failed for ASIN {asin}: {str(e)}")
        print(f"HTTP Fetch failed for ASIN {asin}: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}
# Chunk 2 ends

# Global args variable, to be initialized in main
args = None

# Chunk 3 starts
def write_csv(rows, deals, diagnostic=False):
    try:
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
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
    global args
    args = parser.parse_args() # Initialize global args
    logger = logging.getLogger('KeepaDeals') # Obtain logger instance
    try:
# Logging stuff - starts
        logger.info("Starting Keepa_Deals...") # Use logger instance
        print("Starting Keepa_Deals...", flush=True)
        time.sleep(2)
        deals = fetch_deals_for_deals(0) # Consider passing args.no_cache if fetch_deals_for_deals needs it
        rows = []
        if not deals:
            logger.warning("No deals fetched, writing diagnostic CSV") # Use logger instance
            print("No deals fetched, writing diagnostic CSV", flush=True)
            write_csv([], [], diagnostic=True)
            return
        logger.debug(f"Deals ASINs: {[d.get('asin', '-') for d in deals[:5]]}") # Use logger instance
        print(f"Deals ASINs: {[d.get('asin', '-') for d in deals[:5]]}", flush=True)
        logger.info(f"Starting ASIN processing, found {len(deals)} deals") # Use logger instance
        print(f"Starting ASIN processing, found {len(deals)} deals", flush=True)
# Logging stuff - ends
        for deal in deals:
            asin = deal.get('asin', '-')
            if not validate_asin(asin):
                logger.warning(f"Skipping invalid ASIN for deal {deals.index(deal)+1}") # Use logger instance
                continue
            logger.info(f"Processing ASIN {asin} ({deals.index(deal)+1}/{len(deals)})") # Use logger instance
            print(f"Processing ASIN {asin} ({deals.index(deal)+1}/{len(deals)})", flush=True)
            logger.info(f"Fetching ASIN {asin} ({deals.index(deal)+1}/{len(deals)})") # Use logger instance
            print(f"Fetching ASIN {asin} ({deals.index(deal)+1}/{len(deals)})", flush=True)
            product = fetch_product(asin)
            if not product or 'stats' not in product:
                logger.error(f"Incomplete product data for ASIN {asin}") # Use logger instance
                continue

            # Logging for Last Used price update from product_data
            try:
                if product and isinstance(product, dict) and \
                   product.get('products') and isinstance(product['products'], list) and \
                   len(product['products']) > 0 and isinstance(product['products'][0], dict) and \
                   'csv' in product['products'][0] and isinstance(product['products'][0]['csv'], list) and \
                   len(product['products'][0]['csv']) > 2 and \
                   isinstance(product['products'][0]['csv'][2], list) and \
                   len(product['products'][0]['csv'][2]) > 0:
                    
                    # Get the last entry from the "Used" price history (index 2 for USED)
                    last_used_entry = product['products'][0]['csv'][2][-1]
                    if isinstance(last_used_entry, list) and len(last_used_entry) > 0:
                        last_used_price_ts_minutes = last_used_entry[0]
                        logger.info(f"ASIN: {asin} - Last Used price update from product_data.csv[2]: {last_used_price_ts_minutes}")
                    else:
                        logger.warning(f"ASIN: {asin} - Last Used price entry in product_data.csv[2] is not a valid list or is empty.")
                else:
                    logger.warning(f"ASIN: {asin} - Could not retrieve valid product_data.csv[2] path for Used price history.")
            except (KeyError, IndexError, TypeError) as e:
                logger.warning(f"ASIN: {asin} - Could not retrieve last Used price update from product_data.csv[2]. Error: {type(e).__name__} - {e}")
            
            row = {}
            try:
                # Process all functions using FUNCTION_LIST
                for header, func in zip(HEADERS, FUNCTION_LIST):
                    if func:
                        try:
                            # Determine input_data based on header
                            if header in ['Deal found', 'last update', 'last price change']:
                                input_data = deal
                            else:
                                input_data = product

                            # Call func with appropriate arguments
                            if header == 'last update':
                                # input_data is 'deal', 'product' is the fetched product data
                                result = func(input_data, config, logger, product)
                            elif header == 'Deal found' or header == 'last price change':
                                # input_data is 'deal'
                                result = func(input_data, config, logger) 
                            else: # For all other functions, input_data is 'product'
                                result = func(input_data)
                                
                            logger.debug(f"Header: {header}, Function: {func.__name__}, Result: {result}, Row before: {row}") # Use logger instance
                            row.update(result)
                            logger.debug(f"Row after update for {header}: {row}") # Use logger instance
                        except Exception as e:
                            logger.error(f"Function {func.__name__} failed for ASIN {asin}: {str(e)}") # Use logger instance
                            row[header] = '-'
                rows.append(row)
            except Exception as e:
                logger.error(f"Error processing ASIN {asin}: {str(e)}") # Use logger instance
                continue
        write_csv(rows, deals)
        logger.info("Writing CSV...") # Use logger instance
        print("Writing CSV...")
        logger.info("Script completed!") # Use logger instance
        print("Script completed!")
        print(f"Processed ASINs: {[row.get('ASIN', '-') for row in rows]}")
    except Exception as e:
        logger.error(f"Main failed: {str(e)}") # Use logger instance
        print(f"Main failed: {str(e)}")
        sys.exit(1)
# Chunk 4 ends

if __name__ == "__main__":
    main()

#### END of Keepa_Deals.py ####