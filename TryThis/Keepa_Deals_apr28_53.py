# Chunk 1 starts
# Keepa_Deals.py
import json, csv, logging, sys, requests, urllib.parse, time
from retrying import retry
from stable import get_stat_value, get_title, get_asin, sales_rank_current, used_current, sales_rank_30_days_avg, sales_rank_90_days_avg, sales_rank_180_days_avg, sales_rank_365_days_avg, package_quantity, package_weight, package_height, package_length, package_width

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
@retry(stop_max_attempt_number=3, wait_fixed=5000)
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
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Deal response: {response.text}")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            print(f"Deal fetch failed: {response.status_code}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        logging.debug(f"Fetched {len(deals)} deals: {json.dumps([d['asin'] for d in deals], default=str)}")
        print(f"Fetched {len(deals)} deals")
        return [{'asin': deal['asin'], 'title': deal.get('title', '-')} for deal in deals[:5]]
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
    logging.debug(f"Fetching ASIN {asin} for {days} days, history={history}...")
    print(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&stock=1&buyBox=1&history={history}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Response status: {response.status_code}")
        logging.debug(f"Response time: {response.headers.get('Date', 'N/A')}")
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
        csv_field = product.get('csv', [])
        if not isinstance(csv_field, list) or not csv_field or len(csv_field) < 11:
            logging.warning(f"CSV field invalid for ASIN {asin}: {csv_field}")
            csv_field = [[] for _ in range(11)]  # Ensure 11 indices
        csv_lengths = [len(x) if isinstance(x, list) else 'None' for x in csv_field[:11]]
        logging.debug(f"Raw stats for ASIN {asin}: current={current[:20]}")
        logging.debug(f"CSV field for ASIN {asin}: {csv_lengths}")
        logging.debug(f"CSV raw data for ASIN {asin}: {[x[:10] if isinstance(x, list) else x for x in csv_field[:11]]}")
        logging.debug(f"Buy Box for ASIN {asin}: {json.dumps(buy_box, default=str)}")
        logging.debug(f"Offers for ASIN {asin}: {json.dumps([{'price': o.get('price'), 'condition': o.get('condition'), 'isFBA': o.get('isFBA'), 'isBuyBox': o.get('isBuyBox')} for o in offers], default=str)}")
        logging.debug(f"Stats avg for ASIN {asin}: avg={stats.get('avg', [-1] * 30)[:10]}, avg30={stats.get('avg30', [-1] * 30)[:10]}, avg90={stats.get('avg90', [-1] * 30)[:10]}, avg180={stats.get('avg180', [-1] * 30)[:10]}, avg365={stats.get('avg365', [-1] * 30)[:10]}")
        logging.debug(f"Package dimensions for ASIN {asin}: weight={product.get('packageWeight', -1)}, height={product.get('packageHeight', -1)}, length={product.get('packageLength', -1)}, width={product.get('packageWidth', -1)}, quantity={product.get('packageQuantity', -1)}")
        print(f"Raw stats.current: {current[:20]}")
        if not stats or len(current) < 19:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            print(f"Incomplete stats for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}, 'asin': asin}
        if current[2] <= 0:
            logging.warning(f"No Used price for ASIN {asin}: current[2]={current[2]}")
        if current[3] <= 0:
            logging.warning(f"No Sales Rank for ASIN {asin}: current[3]={current[3]}")
        if current[18] <= 0:
            logging.warning(f"No Buy Box price for ASIN {asin}: current[18]={current[18]}")
        time.sleep(1)  # Rate limiting
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        print(f"Fetch failed: {str(e)}")
        return {'stats': {'current': [-1] * 30}, 'asin': asin}

def used_like_new(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 4 or csv_field[4] is None or not isinstance(csv_field[4], list) or not csv_field[4]:
        logging.warning(f"No valid CSV data for Used, like new, ASIN {asin}: {csv_field[:5] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[4]
    logging.debug(f"CSV data length for Used, like new, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for Used, like new, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float))] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Like New' and o.get('stock', 0) > 0)
    result = {
        'Used, like new - Current': get_stat_value(stats, 'current', 4, divisor=100, is_price=True),
        'Used, like new - 30 days avg.': get_stat_value(stats, 'avg30', 4, divisor=100, is_price=True),
        'Used, like new - 60 days avg.': get_stat_value(stats, 'avg60', 4, divisor=100, is_price=True),
        'Used, like new - 90 days avg.': get_stat_value(stats, 'avg90', 4, divisor=100, is_price=True),
        'Used, like new - 180 days avg.': get_stat_value(stats, 'avg180', 4, divisor=100, is_price=True),
        'Used, like new - 365 days avg.': get_stat_value(stats, 'avg365', 4, divisor=100, is_price=True),
        'Used, like new - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'Used, like new - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, like new - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'Used, like new - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, like new - 90 days OOS': get_stat_value(stats, 'outOfStock90', 4, is_price=False),
        'Used, like new - Stock': str(stock) if stock > 0 else '-'
    }
    logging.debug(f"used_like_new result for ASIN {asin}: {result}")
    print(f"Used, like new for ASIN {asin}: {result}")
    return result

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
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float))] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
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
        'Used, very good - Stock': str(stock) if stock > 0 else '-'
    }
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
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float))] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
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
        'Used, very good - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, good - 90 days OOS': get_stat_value(stats, 'outOfStock90', 6, is_price=False),
        'Used, good - Stock': str(stock) if stock > 0 else '-'
    }
    logging.debug(f"used_good result for ASIN {asin}: {result}")
    print(f"Used, good for ASIN {asin}: {result}")
    return result

def used_acceptable(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 7 or csv_field[7] is None or not isinstance(csv_field[7], list) or not csv_field[7]:
        logging.warning(f"No valid CSV data for Used, acceptable, ASIN {asin}: {csv_field[:8] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[7]
    logging.debug(f"CSV data length for Used, acceptable, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for Used, acceptable, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float))] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) if price > 0 and isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Acceptable' and o.get('stock', 0) > 0)
    result = {
        'Used, acceptable - Current': get_stat_value(stats, 'current', 7, divisor=100, is_price=True),
        'Used, acceptable - 30 days avg.': get_stat_value(stats, 'avg30', 7, divisor=100, is_price=True),
        'Used, acceptable - 60 days avg.': get_stat_value(stats, 'avg60', 7, divisor=100, is_price=True),
        'Used, acceptable - 90 days avg.': get_stat_value(stats, 'avg90', 7, divisor=100, is_price=True),
        'Used, acceptable - 180 days avg.': get_stat_value(stats, 'avg180', 7, divisor=100, is_price=True),
        'Used, acceptable - 365 days avg.': get_stat_value(stats, 'avg365', 7, divisor=100, is_price=True),
        'Used, acceptable - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'Used, acceptable - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, acceptable - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'Used, acceptable - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, acceptable - 90 days OOS': get_stat_value(stats, 'outOfStock90', 7, is_price=False),
        'Used, acceptable - Stock': str(stock) if stock > 0 else '-'
    }
    logging.debug(f"used_acceptable result for ASIN {asin}: {result}")
    print(f"Used, acceptable for ASIN {asin}: {result}")
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
                logging.debug(f"row_data keys for ASIN {deal['asin']}: {row_data.keys()}")
                logging.debug(f"row_data values for ASIN {deal['asin']}: {row_data}")
                print(f"Writing row for ASIN {deal['asin']}: {row_data}")
                writer.writerow([get_asin(deal) if header == 'ASIN' else get_title(deal) if header == 'Title' else row_data.get(header, '-') for header in HEADERS])
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
        logging.debug(f"Deals ASINs: {[d['asin'] for d in deals[:5]]}")
        for deal in deals[:5]:
            asin = deal['asin']
            logging.info(f"Fetching ASIN {asin} ({deals.index(deal)+1}/{len(deals)})")
            product = fetch_product(asin)
            row = {}
            row.update(sales_rank_current(product))
            row.update(sales_rank_30_days_avg(product))
            row.update(sales_rank_90_days_avg(product))
            row.update(sales_rank_180_days_avg(product))
            row.update(sales_rank_365_days_avg(product))
            row.update(used_current(product))
            row.update(package_quantity(product))
            row.update(package_weight(product))
            row.update(package_height(product))
            row.update(package_length(product))
            row.update(package_width(product))
            row.update(used_like_new(product))
            row.update(used_very_good(product))
            row.update(used_good(product))
            row.update(used_acceptable(product))
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