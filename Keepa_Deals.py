# Keepa_Deals.py force it to Git change window
# Environment: Python 3.11 in /home/timscripts/keepa_venv/, activate with 'source /home/timscripts/keepa_venv/bin/activate'
# Dependencies: Install with 'pip install -r /home/timscripts/keepa_api/keepa-deals/requirements.txt'
# Chunk 1 starts
print("Attempting script start...")
try:
    import json, csv, logging, sys, requests, urllib.parse, time
    print("Standard modules loaded")
    from retrying import retry
    print("Imported retrying")
    from stable_deals import validate_asin, fetch_deals_for_deals
    print("Imported stable_deals")
    from field_mappings import FUNCTION_LIST
    print("Imported FUNCTION_LIST")
except Exception as e:
    with open('early_error.txt', 'w') as f:
        f.write(f"Import failure: {str(e)}\n")
    print(f"Import failure: {str(e)}")
    sys.exit(1)

with open('startup.txt', 'w') as f:
    f.write("Script invoked at " + time.ctime() + "\n")
print("Wrote startup.txt")

# Logging
try:
    logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
    print("Logging configured")
    logging.debug("Keepa_Deals.py started")
except Exception as e:
    with open('early_error.txt', 'a') as f:
        f.write(f"Logging failure: {str(e)}\n")
    print(f"Logging failure: {str(e)}")
    sys.exit(1)
# Chunk 1 ends

# Cache config and headers
try:
    print("Loading config.json...")
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        print(f"API key loaded: {api_key[:5]}...")
    print("Loading headers.json...")
    with open('headers.json') as f:
        HEADERS = json.load(f)
        print(f"Headers loaded: {len(HEADERS)} fields")
        logging.debug(f"Loaded headers: {len(HEADERS)} fields")
except Exception as e:
    with open('early_error.txt', 'a') as f:
        f.write(f"Config failure: {str(e)}\n")
    print(f"Config failure: {str(e)}")
    logging.error(f"Config failure: {str(e)}")
    sys.exit(1)
# Chunk 1 ends

# Chunk 2 starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_product(asin, days=365, offers=20, rating=1, history=1):
    if not validate_asin(asin):
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
        time.sleep(1)
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        print(f"Fetch failed: {str(e)}")
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
                        # Ensure Title is populated directly if missing
                        if 'Title' not in row_data or row_data['Title'] == '-':
                            row_data['Title'] = deal.get('title', '-') if deal.get('title') else '-'
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
                for header, func in FUNCTION_LIST:
                    if func:
                        try:
                            # Pass deal for stable_deals functions, product for stable_products
                            input_data = deal if header in ['Deal found', 'last update', 'last price change'] else product
                            result = func(input_data)
                            row[header] = str(result) if result else '-'
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
# End of keepa_deals.py