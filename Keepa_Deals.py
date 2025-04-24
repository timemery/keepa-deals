# Chunk 1 starts
# Dev Log: Loads configuration, API key, deal filters, headers, and field mappings from JSON files (2025-04-27).
# Defines constants for Keepa API base URL, headers, epoch (2011-01-01), Toronto timezone, and product types (Books, Music).
# Uses logging format with level for debugging (2025-04-27).
import requests
import json
import csv
import os
import urllib.parse
from datetime import datetime, timedelta
from pytz import timezone
import logging

logging.basicConfig(
    filename='debug_log.txt',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)

with open(os.path.join(os.path.dirname(__file__), 'config.json')) as file:
    config = json.load(file)
    api_key = config['api_key']

with open(os.path.join(os.path.dirname(__file__), 'deal_filters.json')) as file:
    DEAL_FILTERS = json.load(file)

with open(os.path.join(os.path.dirname(__file__), 'headers.json')) as file:
    HEADERS = json.load(file)

with open(os.path.join(os.path.dirname(__file__), 'field_mapping.json')) as file:
    FIELD_MAPPING = json.load(file)

BASE_URL = 'https://api.keepa.com'
headers = {'accept': 'application/json', 'accept-encoding': 'identity'}
KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')
PRODUCT_TYPES = {0: 'ABIS_BOOK', 1: 'ABIS_MUSIC'}
# Chunk 1 ends

# Chunk 2 starts
# Dev Log: Implements fetch_deals to retrieve deals from Keepa API using JSON selection and pagination.
# fetch_product retrieves product data for a given ASIN and stats period, handles rate limiting (429 status, low tokens),
# and calculates buy_box_used for stats.current[11]. Called by main() for 90 and 365-day stats (2025-04-21).
# Adds timeout to requests to prevent stalls (2025-04-21).
import time

def fetch_deals(page):
    selection = DEAL_FILTERS.copy()
    selection["page"] = page
    selection_str = json.dumps(selection)
    url = f'{BASE_URL}/deal?key={api_key}&selection={urllib.parse.quote(selection_str)}'
    try:
        response = requests.get(url, headers=headers, timeout=30)
        deals = response.json().get('deals', {}).get('dr', [])
        logging.debug(f"fetch_deals - page={page}, deals_count={len(deals)}")
        return deals
    except requests.Timeout:
        logging.error(f"fetch_deals timeout after 30s - page={page}")
        return []
    except requests.RequestException as e:
        logging.error(f"fetch_deals failed: {str(e)}")
        return []

def fetch_product(asin, stats_period):
    url = f'{BASE_URL}/product?key={api_key}&domain=1&asin={asin}&stats={stats_period}&offers=20&rating=1'
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 429:
            logging.warning("Rate limit hit - sleeping for 5 seconds")
            time.sleep(5)
            return fetch_product(asin, stats_period)  # Retry
        tokens_left = response.json().get('tokensLeft', -1)
        if tokens_left < 100:
            sleep_time = max(5, (100 - tokens_left) // 5)
            logging.warning(f"Low tokens ({tokens_left}) - sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
        product = response.json().get('products', [])[0] if response.json().get('products') else {}
        offers = product.get('offers', [])
        buy_box_used = -1
        for offer in offers:
            is_bb = offer.get('isBuyBox', False)
            cond = offer.get('condition', -1)
            price = offer.get('price', -1)
            if is_bb and cond in [2, 3, 4, 5]:
                buy_box_used = price
                break
        if 'stats' not in product:
            product['stats'] = {'current': [-1] * 20}
        product['stats']['current'][11] = buy_box_used
        logging.debug(f"fetch_product - ASIN={asin}, stats_period={stats_period}, buy_box_used={buy_box_used}")
        return product
    except requests.Timeout:
        logging.error(f"fetch_product timeout after 30s - ASIN={asin}, stats_period={stats_period}")
        return {}
    except requests.RequestException as e:
        logging.error(f"fetch_product failed: {str(e)}")
        return {}
# Chunk 2 ends

# Chunk 3 starts
# Dev Log: Implements calculate_fba_fee with 2025 FBA rates ($3.15, $3.53, $6.21, $7.45, $10.53), dimensional weight
# (divisor 166), and $1.80 closing fee for non-book bindings (Audio CD, DVD, Vinyl) (2025-04-21). Handles missing
# dimensions by defaulting to zero. calculate_referral_fee sets 15% for books, with warnings for non-book bindings
# (2025-04-19).
def calculate_fba_fee(weight_g, height_mm, length_mm, width_mm, binding):
    weight_g = weight_g or 0
    height_mm = height_mm or 0
    length_mm = length_mm or 0
    width_mm = width_mm or 0
    weight_lb = weight_g / 453.59237
    dims_in = [x / 25.4 for x in [height_mm, length_mm, width_mm]]
    dims_in.sort()
    volume_cm3 = (height_mm * length_mm * width_mm) / 1000
    dim_weight_lb = (volume_cm3 / 166) / 453.59237 if volume_cm3 > 0 else 0
    effective_weight = max(weight_lb, dim_weight_lb) if dim_weight_lb > 0 else weight_lb
    logging.debug(f"FBA Calc: weight={weight_lb:.2f} lb, dims={dims_in}, dim_weight={dim_weight_lb:.2f} lb, effective_weight={effective_weight:.2f} lb, volume={volume_cm3:.2f} cm³")
    
    if effective_weight <= 15 and dims_in[2] <= 18 and dims_in[1] <= 14 and dims_in[0] <= 8:
        if effective_weight <= 0.25:
            base_fee = 3.15
            size_tier = "Small Standard (<=4 oz)"
        elif effective_weight <= 1:
            base_fee = 3.53
            size_tier = "Small Standard (4-16 oz)"
        elif effective_weight <= 3:
            base_fee = 6.21
            size_tier = "Large Standard (<=3 lb)"
        else:
            base_fee = 7.45 + (effective_weight - 3) * 0.15
            size_tier = "Large Standard (3-15 lb)"
    else:
        base_fee = 10.53
        size_tier = "Oversize"
    
    closing_fee = 1.80 if binding in ["Audio CD", "DVD", "Vinyl"] else 0.0
    total_fee = base_fee + closing_fee
    logging.debug(f"FBA Fee: size_tier={size_tier}, base_fee=${base_fee:.2f}, closing_fee=${closing_fee:.2f}, total=${total_fee:.2f}")
    return f"${total_fee:.2f}"

def calculate_referral_fee(asin, product_type, binding):
    if product_type == "ABIS_BOOK" and binding not in ["Audio CD", "DVD", "Vinyl"]:
        logging.debug(f"ASIN {asin}: Referral fee set to 15.0% (Book)")
        return "15.00%"
    logging.warning(f"ASIN {asin}: Non-book binding ({binding}), verify category")
    return "15.00%"
# Chunk 3 ends

# Chunk 4 starts
# Dev Log: Formatting functions for categories, frequently bought together, contributors, languages, and dates (2025-04-19).
# Handles edge cases like empty lists, invalid dates, and ensures consistent output formats.
def format_categories_tree(category_tree):
    return ', '.join(cat['name'] for cat in category_tree) if category_tree else '-'

def format_freq_bought_together(asins):
    return '-' if not asins or asins == 'None' else ', '.join(f"https://keepa.com/#!product/1-{asin}" for asin in asins)

def format_contributors(contributors):
    return '-' if not contributors else ', '.join(f"{name} ({role})" for name, role in contributors)

def format_languages(languages):
    return '-' if not languages else ', '.join(f"{lang} ({', '.join(roles)})" for lang, *roles in languages)

def format_date(value, full_date=False):
    if not value or value in [-1, 'None', '']: return '-'
    try:
        value = str(value)
        if len(value) == 8:
            dt = datetime.strptime(value, '%Y%m%d')
            return dt.strftime('%Y-%m-%d') if full_date else dt.strftime('%Y')
        return value if full_date else value[:4]
    except ValueError:
        return value
# Chunk 4 ends

# Chunk 5 starts
import logging
import json
import csv
from typing import Any, Dict, List
from keepa import Keepa
import config  # Assuming config.py has API key

# Setup logging
logging.basicConfig(
    filename='debug_log.txt',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)

def get_field(product: Dict[str, Any], field_map: str, default: str = '-') -> Any:
    """Extract field value from product data using field_map expression."""
    try:
        # Handle simple attributes (e.g., 'asin', 'title')
        if '.' not in field_map and '[' not in field_map:
            value = product.get(field_map, default)
            logging.debug(f"Field {field_map}: value={value}")
            return value if value != default else default

        # Handle nested attributes and calculations
        parts = field_map.replace(']', '').split('[')
        base = parts[0].split('.')
        value = product

        # Navigate nested dictionary
        for part in base:
            value = value.get(part, default)
            if value == default:
                logging.debug(f"Field {field_map}: missing key {part}")
                return default

        # Handle array indices
        for part in parts[1:]:
            try:
                index = int(part)
                value = value[index] if isinstance(value, list) and index < len(value) else default
                if value == default:
                    logging.debug(f"Field {field_map}: invalid index {index}")
                    return default
            except (ValueError, TypeError):
                logging.debug(f"Field {field_map}: error parsing index {part}")
                return default

        # Handle calculations (e.g., Percent Down 90)
        if 'stats' in field_map and '(' in field_map:
            try:
                # Replace stats expressions with evaluated values
                expr = field_map
                for key in ['stats.current', 'stats.avg', 'stats.avg365']:
                    if key in expr:
                        keys = key.split('.')
                        val = product.get(keys[0], {}).get(keys[1], [default])[int(expr[expr.index(key)+len(key)+1])]
                        expr = expr.replace(f"{key}[{expr[expr.index(key)+len(key)+1]}]", str(val))
                value = eval(expr, {}, {})
                logging.debug(f"Field {field_map}: evaluated value={value}")
                return value
            except Exception as e:
                logging.debug(f"Field {field_map}: eval error {str(e)}")
                return default

        logging.debug(f"Field {field_map}: value={value}")
        return value if value != default else default

    except Exception as e:
        logging.error(f"Field {field_map}: error {str(e)}")
        return default

def write_csv(products: List[Dict[str, Any]], field_mapping: Dict[str, Any], output_file: str = 'Keepa_Deals_Export.csv'):
    """Write product data to CSV."""
    headers = []
    for section in field_mapping.values():
        headers.extend(section.keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for product in products:
            row = {}
            asin = product.get('asin', 'unknown')
            logging.debug(f"Processing ASIN {asin}")

            for section_name, section in field_mapping.items():
                for field_name, field_map in section.items():
                    # Handle URL formatting for AMZ link and Keepa Link
                    if '{asin}' in field_map:
                        value = field_map.format(asin=asin) if asin != 'unknown' else '-'
                    else:
                        value = get_field(product, field_map)

                    # Handle Price Now Source and condition prices
                    if field_name == 'Price Now Source':
                        conditions = [
                            ('Used, like new - Current', 'stats.current[20]'),
                            ('Used, very good - Current', 'stats.current[19]'),
                            ('Used, good - Current', 'stats.current[18]'),
                            ('Used, acceptable - Current', 'stats.current[15]'),
                            ('Used - Current', 'stats.current[3]')
                        ]
                        for cond_name, cond_map in conditions:
                            cond_value = get_field(product, cond_map)
                            if cond_value != '-' and float(cond_value) == float(row.get('Price Now', '-')):
                                value = cond_name.replace(' - Current', '')
                                break
                        else:
                            value = 'Used' if row.get('Price Now') != '-' else '-'

                    row[field_name] = value
                    logging.debug(f"Field {field_name} (ASIN {asin}): field_map={field_map}, value={value}")

            writer.writerow(row)

# Example usage (adjust based on your script)
def main():
    api = Keepa(config.KEEPA_API_KEY)
    # Example: Fetch products (replace with your product fetching logic)
    products = api.query(['B08J4FFK38'], domain='US', stats=90)  # Adjust ASINs and params
    with open('field_mapping.json', 'r') as file:
        field_mapping = json.load(file)
    write_csv(products, field_mapping)

if __name__ == '__main__':
    main()

# Chunk 5 ends

# Chunk 6 starts
# Dev Log: Implements extract_fields to generate CSV rows by mapping all headers from headers.json using get_field.
# Uses nested field_mapping.json for field lookups (2025-04-19).
def extract_fields(deal, product_90, product_365):
    row = [get_field(product_365, deal, product_90, h, FIELD_MAPPING.get(h, h)) for h in HEADERS]
    logging.debug(f"Extracted ASIN {deal.get('asin')}: {row}")
    return row
# Chunk 6 ends

# Chunk 7 starts
# Dev Log: Writes CSV with headers and rows, deletes existing file to avoid appending, logs file size (2025-04-19).
def write_csv(data, filename=os.path.join(os.path.dirname(__file__), 'Keepa_Deals_Export.csv')):
    logging.debug(f"CSV path = {filename}")
    try:
        if os.path.exists(filename):
            os.remove(filename)
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(HEADERS)
            writer.writerows(data)
        logging.info(f"File size: {os.path.getsize(filename)} bytes")
    except Exception as e:
        logging.error(f"Error writing CSV {filename}: {e}")
        raise
# Chunk 7 ends

# Chunk 8 starts
# Dev Log: Implements main() to fetch deals, retrieve product data for 90 and 365-day stats, process up to 50 deals,
# and write CSV using headers from headers.json (2025-04-21). Adds timeout to fetch_deals and verbose logging to diagnose stalls (2025-04-21).
import sys

def main():
    logging.info("Starting Keepa_Deals...")
    print("Initializing script...")
    sys.stdout.flush()
    try:
        logging.info("Fetching deals (page 0)...")
        print("Fetching deals...")
        sys.stdout.flush()
        deals = fetch_deals(0)
        logging.info(f"Deals returned: {len(deals)}")
        print(f"Deals returned: {len(deals)}")
        sys.stdout.flush()
        if deals:
            logging.info("Fetching product data...")
            print("Fetching product data...")
            sys.stdout.flush()
            max_deals = min(10, len(deals))
            products_90 = {}
            products_365 = {}
            for i, deal in enumerate(deals[:max_deals]):
                asin = deal['asin']
                logging.info(f"Fetching ASIN {asin} ({i+1}/{max_deals})...")
                print(f"Fetching ASIN {asin} ({i+1}/{max_deals})...")
                sys.stdout.flush()
                products_90[asin] = fetch_product(asin, 90)
                products_365[asin] = fetch_product(asin, 365)
            logging.info("Processing deals...")
            print("Processing deals...")
            sys.stdout.flush()
            rows = []
            for i, deal in enumerate(deals[:max_deals]):
                asin = deal['asin']
                logging.info(f"Extracting ASIN {asin} ({i+1}/{max_deals})...")
                print(f"Extracting ASIN {asin} ({i+1}/{max_deals})...")
                sys.stdout.flush()
                row = extract_fields(deal, products_90.get(asin, {}), products_365.get(asin, {}))
                rows.append(row)
            logging.info(f"Rows generated: {len(rows)}")
            print(f"Rows generated: {len(rows)}")
            sys.stdout.flush()
            logging.info("Writing CSV...")
            print("Writing CSV...")
            sys.stdout.flush()
            write_csv(rows)
            logging.info("Script completed successfully!")
            print("Script completed successfully!")
        else:
            logging.warning("No deals fetched!")
            print("No deals fetched!")
        sys.stdout.flush()
    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
        print(f"Error: {str(e)}")
        sys.stdout.flush()
        raise

if __name__ == "__main__":
    main()
# Chunk 8 ends