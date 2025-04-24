# Chunk 1 starts
# Dev Log: Loads configuration, API key, deal filters, headers, and field mappings from JSON files (2025-04-19).
# Defines constants for Keepa API base URL, headers, epoch (2011-01-01), Toronto timezone, and product types (Books, Music).
import requests
import json
import csv
import os
import urllib.parse
from datetime import datetime, timedelta
from pytz import timezone
import logging

logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(message)s')

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
# Dev Log: Processes header fields for CSV output (2025-04-25).
# Integrated stable_fields.py logic for Price Now, Deal found, Percent Down 90.
# Refined Price Now Source to match stats.current[2] against condition indices.
# Added correct indices for condition prices and logging for stats.current[18, 19, 20, 1, 10, 8, 9].
def get_field(header, data, deal, product_90, stats_90, field_map, deal_filters, keepa_epoch, toronto_tz):
    import logging
    from datetime import datetime, timedelta

    if header == "Price Now":
        current = stats_90.get('current', [-1] * 20)[2] if len(stats_90.get('current', [-1] * 20)) > 2 else -1
        logging.debug(f"Price Now (ASIN {product_90.get('asin')}): current[2]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"  # Minimum $1.00 to avoid offer counts
    elif header == "Price Now Source":
        current = stats_90.get('current', [-1] * 20)[2] if len(stats_90.get('current', [-1] * 20)) > 2 else -1
        prices = {
            18: stats_90.get('current', [-1] * 20)[18],  # Used - Good
            19: stats_90.get('current', [-1] * 20)[19],  # Used - Very Good
            20: stats_90.get('current', [-1] * 20)[20],  # Used - Like New
            1: stats_90.get('current', [-1] * 20)[1],    # New
            10: stats_90.get('current', [-1] * 20)[10],  # Collectible
            8: stats_90.get('current', [-1] * 20)[8],    # New, 3rd Party FBA
            9: stats_90.get('current', [-1] * 20)[9]     # New, 3rd Party FBM
        }
        source_map = {
            18: "Used - Good",
            19: "Used - Very Good",
            20: "Used - Like New",
            1: "New",
            10: "Collectible",
            8: "New, 3rd Party FBA",
            9: "New, 3rd Party FBM"
        }
        logging.debug(f"Price Now Source (ASIN {product_90.get('asin')}): current[2]={current}, prices={prices}")
        if current > 100:
            for idx, price in prices.items():
                if price == current:
                    return source_map[idx]
        return "Used" if current > 100 else "-"  # Fallback to Used if no match
    elif header == "Deal found":
        ts = deal.get('creationDate', 0)
        dt = (keepa_epoch + timedelta(minutes=ts)) if ts > 100000 else None
        return toronto_tz.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else "-"
    elif header == "Percent Down 90":
        avg = stats_90.get('avg', [-1] * 20)[2] if len(stats_90.get('avg', [-1] * 20)) > 2 else -1
        current = stats_90.get('current', [-1] * 20)[2] if len(stats_90.get('current', [-1] * 20)) > 2 else -1
        value = ((avg - current) / avg * 100) if avg > 0 and current > 0 else -1
        logging.debug(f"Percent Down 90 (ASIN {product_90.get('asin')}): avg[2]={avg}, current[2]={current}, value={value}")
        return f"{value:.0f}%" if value > 0 else "-"
    elif header == "Used, good - Current":
        current = stats_90.get('current', [-1] * 20)[18] if len(stats_90.get('current', [-1] * 20)) > 18 else -1
        logging.debug(f"Used, good - Current (ASIN {product_90.get('asin')}): current[18]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "Used, very good - Current":
        current = stats_90.get('current', [-1] * 20)[19] if len(stats_90.get('current', [-1] * 20)) > 19 else -1
        logging.debug(f"Used, very good - Current (ASIN {product_90.get('asin')}): current[19]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "Used, like new - Current":
        current = stats_90.get('current', [-1] * 20)[20] if len(stats_90.get('current', [-1] * 20)) > 20 else -1
        logging.debug(f"Used, like new - Current (ASIN {product_90.get('asin')}): current[20]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "New - Current":
        current = stats_90.get('current', [-1] * 20)[1] if len(stats_90.get('current', [-1] * 20)) > 1 else -1
        logging.debug(f"New - Current (ASIN {product_90.get('asin')}): current[1]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "New, 3rd Party FBA - Current":
        current = stats_90.get('current', [-1] * 20)[8] if len(stats_90.get('current', [-1] * 20)) > 8 else -1
        logging.debug(f"New, 3rd Party FBA - Current (ASIN {product_90.get('asin')}): current[8]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "New, 3rd Party FBM - Current":
        current = stats_90.get('current', [-1] * 20)[9] if len(stats_90.get('current', [-1] * 20)) > 9 else -1
        logging.debug(f"New, 3rd Party FBM - Current (ASIN {product_90.get('asin')}): current[9]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "Collectible - Current":
        current = stats_90.get('current', [-1] * 20)[10] if len(stats_90.get('current', [-1] * 20)) > 10 else -1
        logging.debug(f"Collectible - Current (ASIN {product_90.get('asin')}): current[10]={current}")
        return f"${current / 100:.2f}" if current > 100 else "-"
    elif header == "Avg. Price 90":
        avg = stats_90.get('avg', [-1] * 20)[2] if len(stats_90.get('avg', [-1] * 20)) > 2 else -1
        return f"${avg / 100:.2f}" if avg > 0 else "-"
    else:
        # Existing logic for other fields (e.g., ASIN, Title)
        product = product_90.get('data', {})
        stats = stats_90
        stats_current = stats.get('current', [-1] * 20)
        stats_avg90 = stats.get('avg', [-1] * 20)
        stats_avg30 = stats.get('avg30', [-1] * 20)
        value = "-"
        try:
            if header in field_map:
                field_info = field_map[header]
                if field_info["source"] == "stats_current":
                    idx = field_info["index"]
                    value = stats_current[idx]
                    if isinstance(value, (int, float)) and value >= 0:
                        value = f"${value / 100:.2f}" if field_info.get("is_price", False) else str(value)
                    else:
                        value = "-"
                elif field_info["source"] == "stats_avg90":
                    idx = field_info["index"]
                    value = stats_avg90[idx]
                    if isinstance(value, (int, float)) and value >= 0:
                        value = f"${value / 100:.2f}" if field_info.get("is_price", False) else str(value)
                    else:
                        value = "-"
                elif field_info["source"] == "stats_avg30":
                    idx = field_info["index"]
                    value = stats_avg30[idx]
                    if isinstance(value, (int, float)) and value >= 0:
                        value = f"${value / 100:.2f}" if field_info.get("is_price", False) else str(value)
                    else:
                        value = "-"
                elif field_info["source"] == "product":
                    value = product.get(field_info["field"], "-")
                    if field_info.get("is_list", False):
                        value = ";".join(value) if isinstance(value, list) else str(value)
            logging.debug(f"{header} (ASIN {product_90.get('asin')}): value={value}")
        except Exception as e:
            logging.error(f"Error processing {header} for ASIN {product_90.get('asin')}: {str(e)}")
            value = "-"
        return value
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
    logging.info("Starting Process500_Deals_v8...")
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