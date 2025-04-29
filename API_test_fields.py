import json, csv, logging, sys, requests, time

# Setup logging
logging.basicConfig(filename='api_test_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

# Load API key
try:
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        print(f"API key loaded: {api_key[:5]}...")
except Exception as e:
    logging.error(f"Failed to load config: {str(e)}")
    print(f"Failed to load config: {str(e)}")
    sys.exit(1)

# Keepa field descriptions (from https://keepa.com/#!api)
FIELD_DESCRIPTIONS = {
    'csv': {
        0: 'Sales Rank',
        1: 'New, 3rd Party FBM',
        2: 'Amazon',
        3: 'New',
        4: 'Used - Like New',
        5: 'Used - Very Good',
        6: 'Used - Good',
        7: 'Used - Acceptable',
        8: 'List Price',
        9: 'Collectible',
        10: 'Refurbished',
        11: 'New, 3rd Party FBA',
        12: 'Lightning Deal',
        13: 'Warehouse Deals',
        14: 'Rental',
        15: 'Used, 3rd Party FBA',
        16: 'Digital',
        17: 'Trade-In',
        18: 'Alternative Platform'
    },
    'stats': {
        'current': 'Current Prices',
        'avg30': '30-Day Average',
        'avg90': '90-Day Average',
        'avg180': '180-Day Average',
        'avg365': '365-Day Average',
        'outOfStock90': 'Out of Stock % (90 Days)'
    },
    'offers': 'Offer Details (Price, Condition, Stock)',
    'metadata': [
        'title', 'asin', 'packageWeight', 'packageHeight', 'packageLength',
        'packageWidth', 'packageQuantity', 'brand', 'manufacturer', 'categories',
        'lastUpdate', 'lastPriceChange', 'salesDrops90'
    ]
}

def fetch_product(asin):
    logging.debug(f"Fetching ASIN {asin}...")
    print(f"Fetching ASIN {asin}...")
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats=365&offers=20&rating=1&stock=1&buyBox=1&history=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            logging.error(f"Request failed: {response.status_code}, {response.text}")
            print(f"Request failed: {response.status_code}")
            return None
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"No product data for ASIN {asin}")
            print(f"No product data for ASIN {asin}")
            return None
        product = products[0]
        logging.debug(f"Fetched product for ASIN {asin}: {list(product.keys())[:10]}")
        time.sleep(1)
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        print(f"Fetch failed: {str(e)}")
        return None

def extract_all_fields(product):
    if not product:
        return []
    asin = product.get('asin', 'unknown')
    rows = []

    # CSV fields (price history)
    csv_field = product.get('csv', [[] for _ in range(19)])
    for i in range(len(csv_field)):
        desc = FIELD_DESCRIPTIONS['csv'].get(i, f'Unknown csv[{i}]')
        csv_data = csv_field[i] if isinstance(csv_field[i], list) else []
        prices = [f"${price / 100:.2f}" for timestamp, price in zip(csv_data[0::2], csv_data[1::2])
                  if isinstance(price, (int, float)) and price > 0] if csv_data else []
        rows.append({
            'Field': f"csv[{i}]",
            'Description': desc,
            'Data': prices[:20] if prices else 'None',
            'Raw Length': len(csv_data)
        })
        logging.debug(f"csv[{i}] ({desc}) for ASIN {asin}: {prices[:10]}")

    # Stats fields
    stats = product.get('stats', {})
    for stat_key, stat_desc in FIELD_DESCRIPTIONS['stats'].items():
        stat_data = stats.get(stat_key, [-1] * 30)
        formatted = [f"${x / 100:.2f}" if isinstance(x, (int, float)) and x > 0 else str(x)
                     for x in stat_data[:19]]
        rows.append({
            'Field': f"stats.{stat_key}",
            'Description': stat_desc,
            'Data': formatted,
            'Raw Length': len(stat_data)
        })
        logging.debug(f"stats.{stat_key} ({stat_desc}) for ASIN {asin}: {formatted[:10]}")

    # Offers
    offers = product.get('offers', [])
    offer_data = [
        {
            'Price': f"${o.get('price', -1) / 100:.2f}" if o.get('price', -1) > 0 else '-',
            'Condition': o.get('condition', 'Unknown'),
            'Stock': o.get('stock', 0),
            'IsFBA': o.get('isFBA', False)
        } for o in offers
    ]
    rows.append({
        'Field': 'offers',
        'Description': FIELD_DESCRIPTIONS['offers'],
        'Data': offer_data[:20] if offer_data else 'None',
        'Raw Length': len(offers)
    })
    logging.debug(f"offers for ASIN {asin}: {offer_data[:5]}")

    # Metadata
    for key in FIELD_DESCRIPTIONS['metadata']:
        value = product.get(key, '-')
        rows.append({
            'Field': key,
            'Description': key.replace('_', ' ').title(),
            'Data': str(value)[:500],
            'Raw Length': len(str(value)) if isinstance(value, (str, list)) else 1
        })
        logging.debug(f"{key} for ASIN {asin}: {str(value)[:100]}")

    return rows

def write_csv(rows):
    with open('API_validation.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Field', 'Description', 'Data', 'Raw Length'])
        for row in rows:
            writer.writerow([
                row['Field'],
                row['Description'],
                str(row['Data'])[:1000],  # Truncate for CSV readability
                row['Raw Length']
            ])
    logging.info("Wrote API_validation.csv")
    print("Wrote API_validation.csv")

def main():
    try:
        logging.info("Starting API_test_fields...")
        print("Starting API_test_fields...")
        asin = "0914671227"
        product = fetch_product(asin)
        if not product:
            logging.error("No product data, exiting")
            print("No product data, exiting")
            sys.exit(1)
        rows = extract_all_fields(product)
        write_csv(rows)
        logging.info("Script completed!")
        print("Script completed!")
    except Exception as e:
        logging.error(f"Main failed: {str(e)}")
        print(f"Main failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()