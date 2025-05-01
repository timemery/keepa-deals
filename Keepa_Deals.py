# Chunk 1 starts
import json, csv, logging, sys, requests, urllib.parse, time
from retrying import retry
from stable import get_stat_value, get_title, get_asin, sales_rank_current, used_current, sales_rank_30_days_avg, sales_rank_90_days_avg, sales_rank_180_days_avg, sales_rank_365_days_avg, package_quantity, package_weight, package_height, package_length, package_width, list_price, new_3rd_party_fbm, used_like_new

# Logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def load_headers():
    try:
        with open('headers.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load headers: {str(e)}")
        print(f"Failed to load headers: {str(e)}")
        return []

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
def fetch_deals(api_key, per_page=100, max_pages=2):
    asins = []
    price_data_map = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212', 'Accept-Encoding': 'gzip'}
    # Check tokens
    token_url = f"https://api.keepa.com/token?key={api_key}"
    try:
        token_response = requests.get(token_url, headers=headers, timeout=30)
        token_data = token_response.json()
        logging.debug(f"Token status: {token_response.status_code}, tokens left: {token_data.get('tokensLeft', -1)}")
    except Exception as e:
        logging.error(f"Token check failed: {str(e)}")
        print(f"Token check failed: {str(e)}")
    for page in range(max_pages):
        selection = json.dumps({
            "page": page, "domainId": "1", "priceTypes": [2], "salesRankRange": [50000, 1500000],
            "deltaPercentRange": [50, 100], "isFilterEnabled": True, "isRangeEnabled": True,
            "sortType": 0, "includeCategories": [283155]
        })
        url = f"https://api.keepa.com/deal?key={api_key}&selection={urllib.parse.quote(selection)}&perPage={per_page}"
        logging.debug(f"Fetching deals page {page}: {url}")
        logging.debug(f"Selection JSON: {selection}")
        try:
            response = requests.get(url, headers=headers, timeout=30)
            logging.debug(f"Deals response status: {response.status_code}")
            logging.debug(f"Deals raw response: {response.text}")
            if response.status_code != 200:
                logging.error(f"Deals request failed: {response.status_code} - {response.text}")
                print(f"Deals request failed: {response.status_code}")
                continue
            data = response.json()
            logging.debug(f"Deals response data: {json.dumps(data, indent=2)}")
            if 'error' in data:
                logging.error(f"Deals response error: {data['error']}")
                print(f"Deals response error: {data['error']}")
            deals = data.get('dr', [])
            logging.debug(f"Deals count: {len(deals)}")
            for deal in deals:
                asin = deal.get('asin')
                if asin:
                    asins.append(asin)
                    price_data = deal.get('priceData', {}).get('USED_ACCEPTABLE_SHIPPING', {})
                    price_data_map[asin] = {
                        'lowest': price_data.get('lowest', -1),
                        'highest': price_data.get('highest', -1),
                        'lowest365': price_data.get('lowest365', -1),
                        'highest365': price_data.get('highest365', -1)
                    }
            time.sleep(1)
        except Exception as e:
            logging.error(f"Deals fetch failed: {str(e)}")
            print(f"Deals fetch failed: {str(e)}")
    print(f"Total ASINs fetched: {len(asins)}")
    return asins[:150], price_data_map
# Chunk 2 ends

# Chunk 3 starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_product(asin, days=365, offers=20, rating=1, history=1):
    global api_key
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        print(f"Invalid ASIN format: {asin}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}
    logging.debug(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&stock=1&buyBox=1&history={history}&update=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Product response status: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"Product request failed: {response.status_code} - {response.text}")
            print(f"Product request failed: {response.status_code}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        data = response.json()
        logging.debug(f"Product response data: {json.dumps(data, indent=2)[:1000]}...")
        products = data.get('products', [])
        if not products:
            logging.error(f"No product data for ASIN {asin}")
            print(f"No product data for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        product = products[0]
        time.sleep(1)
        return product
    except Exception as e:
        logging.error(f"Product fetch failed: {str(e)}")
        print(f"Product fetch failed: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def used_very_good(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 5 or csv_field[5] is None or not isinstance(csv_field[5], list) or not csv_field[5]:
        logging.warning(f"No valid CSV data for Used, very good, ASIN {asin}: {csv_field[:6] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[5]
    logging.debug(f"CSV data length for Used, very good, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for Used, very good, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
              if isinstance(price, (int, float)) and price >= 1 and price <= 1000000 and 
                 isinstance(timestamp, (int, float)) and timestamp > 0] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
                  if isinstance(price, (int, float)) and price >= 1 and price <= 1000000 and 
                     isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Very Good' and o.get('stock', 0) > 0)
    result = {
        'Used, very good - Current': get_stat_value(stats, 'current', 5, divisor=100, is_price=True),
        'Used, very good - 30 days avg.': get_stat_value(stats, 'avg30', 5, divisor=100, is_price=True),
        'Used, very good - 60 days avg.': get_stat_value(stats, 'avg60', 5, divisor=100, is_price=True),
        'Used, very good - 90 days avg.': get_stat_value(stats, 'avg90', 5, divisor=100, is_price=True),
        'Used, very good - 180 days avg.': get_stat_value(stats, 'avg180', 5, divisor=100, is_price=True),
        'Used, very good - 365 days avg.': get_stat_value(stats, 'avg365', 5, divisor=100, is_price=True),
        'Used, very good - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'Used, very good - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, very good - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'Used, very good - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, very good - 90 days OOS': get_stat_value(stats, 'outOfStock90', 5, is_price=False),
        'Used, very good - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_very_good stats.current[5] for ASIN {asin}: {stats.get('current', [-1]*30)[5]}")
    logging.debug(f"used_very_good result for ASIN {asin}: {result}")
    print(f"Used, very good for ASIN {asin}: {result}")
    return result

def used_good(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 6 or csv_field[6] is None or not isinstance(csv_field[6], list) or not csv_field[6]:
        logging.warning(f"No valid CSV data for Used, good, ASIN {asin}: {csv_field[:7] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[6]
    logging.debug(f"CSV data length for Used, good, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for Used, good, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
              if isinstance(price, (int, float)) and price >= 1 and price <= 1000000 and 
                 isinstance(timestamp, (int, float)) and timestamp > 0] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
                  if isinstance(price, (int, float)) and price >= 1 and price <= 1000000 and 
                     isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Good' and o.get('stock', 0) > 0)
    result = {
        'Used, good - Current': get_stat_value(stats, 'current', 6, divisor=100, is_price=True),
        'Used, good - 30 days avg.': get_stat_value(stats, 'avg30', 6, divisor=100, is_price=True),
        'Used, good - 60 days avg.': get_stat_value(stats, 'avg60', 6, divisor=100, is_price=True),
        'Used, good - 90 days avg.': get_stat_value(stats, 'avg90', 6, divisor=100, is_price=True),
        'Used, good - 180 days avg.': get_stat_value(stats, 'avg180', 6, divisor=100, is_price=True),
        'Used, good - 365 days avg.': get_stat_value(stats, 'avg365', 6, divisor=100, is_price=True),
        'Used, good - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'Used, good - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, good - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'Used, good - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, good - 90 days OOS': get_stat_value(stats, 'outOfStock90', 6, is_price=False),
        'Used, good - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_good stats.current[6] for ASIN {asin}: {stats.get('current', [-1]*30)[6]}")
    logging.debug(f"used_good result for ASIN {asin}: {result}")
    print(f"Used, good for ASIN {asin}: {result}")
    return result

def used_acceptable(product, price_data_map):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    offers = product.get('offers', [])
    used_acc_offers = [o for o in offers if o.get('condition') == 'Used - Acceptable']
    stock = sum(1 for o in used_acc_offers if o.get('stock', 0) > 0)
    price_data = price_data_map.get(asin, {})
    result = {
        'Used, acceptable - Current': get_stat_value(stats, 'current', 7, divisor=100, is_price=True),
        'Used, acceptable - 30 days avg.': get_stat_value(stats, 'avg30', 7, divisor=100, is_price=True),
        'Used, acceptable - 90 days avg.': get_stat_value(stats, 'avg90', 7, divisor=100, is_price=True),
        'Used, acceptable - 180 days avg.': get_stat_value(stats, 'avg180', 7, divisor=100, is_price=True),
        'Used, acceptable - 365 days avg.': get_stat_value(stats, 'avg365', 7, divisor=100, is_price=True),
        'Used, acceptable - Lowest': f"${price_data['lowest'] / 100:.2f}" if price_data.get('lowest', -1) != -1 else '-',
        'Used, acceptable - Lowest 365 days': f"${price_data['lowest365'] / 100:.2f}" if price_data.get('lowest365', -1) != -1 else '-',
        'Used, acceptable - Highest': f"${price_data['highest'] / 100:.2f}" if price_data.get('highest', -1) != -1 else '-',
        'Used, acceptable - Highest 365 days': f"${price_data['highest365'] / 100:.2f}" if price_data.get('highest365', -1) != -1 else '-',
        'Used, acceptable - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_acceptable result for ASIN {asin}: {result}")
    print(f"Used, acceptable for ASIN {asin}: {result}")
    return result
# Chunk 3 ends

# Chunk 4 starts
def main():
    logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
    print("Starting Keepa Deals script...")
    logging.debug("Starting Keepa Deals script...")
    config_path = 'config.json'
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            api_key = config.get('api_key')  # Fixed key name
            if not api_key:
                logging.error("No API key found in config.json")
                print("No API key found in config.json")
                return
    except Exception as e:
        logging.error(f"Failed to load config: {str(e)}")
        print(f"Failed to load config: {str(e)}")
        return
    asins, price_data_map = fetch_deals(api_key, per_page=100, max_pages=2)
    if not asins:
        logging.error("No ASINs fetched from deals")
        print("No ASINs fetched from deals")
        return
    headers = load_headers()
    if not headers:
        logging.error("No headers loaded")
        print("No headers loaded")
        return
    output_file = 'Keepa_Deals_Export.csv'
    results = []
    for i, asin in enumerate(asins[:5]):  # Process first 5 ASINs
        logging.debug(f"Processing ASIN {asin} ({i+1}/{min(5, len(asins))})")
        print(f"Processing ASIN {asin} ({i+1}/{min(5, len(asins))})")
        product = fetch_product(asin)
        row = {
            'ASIN': asin,
            **list_price(product),
            **used_acceptable(product, price_data_map),
            **used_very_good(product),
            **used_good(product),
            **sales_rank_drops(product),
            **product_package(product)
        }
        results.append(row)
        time.sleep(1)
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)
        logging.debug(f"CSV written successfully: {output_file}")
        print(f"CSV written successfully: {output_file}")
    except Exception as e:
        logging.error(f"Failed to write CSV: {str(e)}")
        print(f"Failed to write CSV: {str(e)}")
    logging.debug("Script completed!")
    print("Script completed!")
# Chunk 4 ends

# Chunk 5 starts
def load_headers():
    try:
        with open('headers.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load headers: {str(e)}")
        print(f"Failed to load headers: {str(e)}")
        return []

def main():
    logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
    print("Starting Keepa Deals script...")
    logging.debug("Starting Keepa Deals script...")
    config_path = 'config.json'
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            api_key = config.get('api_key')  # Fixed key name
            if not api_key:
                logging.error("No API key found in config.json")
                print("No API key found in config.json")
                return
    except Exception as e:
        logging.error(f"Failed to load config: {str(e)}")
        print(f"Failed to load config: {str(e)}")
        return
    asins, price_data_map = fetch_deals(api_key, per_page=100, max_pages=2)
    if not asins:
        logging.error("No ASINs fetched from deals")
        print("No ASINs fetched from deals")
        return
    headers = load_headers()
    if not headers:
        logging.error("No headers loaded")
        print("No headers loaded")
        return
    output_file = 'Keepa_Deals_Export.csv'
    results = []
    for i, asin in enumerate(asins[:5]):  # Process first 5 ASINs
        logging.debug(f"Processing ASIN {asin} ({i+1}/{min(5, len(asins))})")
        print(f"Processing ASIN {asin} ({i+1}/{min(5, len(asins))})")
        product = fetch_product(asin)
        row = {
            'ASIN': asin,
            **list_price(product),
            **used_acceptable(product, price_data_map),
            **used_very_good(product),
            **used_good(product),
            **sales_rank_drops(product),
            **product_package(product)
        }
        results.append(row)
        time.sleep(1)
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)
        logging.debug(f"CSV written successfully: {output_file}")
        print(f"CSV written successfully: {output_file}")
    except Exception as e:
        logging.error(f"Failed to write CSV: {str(e)}")
        print(f"Failed to write CSV: {str(e)}")
    logging.debug("Script completed!")
    print("Script completed!")

if __name__ == "__main__":
    main()
# Chunk 5 ends