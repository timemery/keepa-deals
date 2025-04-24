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
# Dev Log: Implements get_field to map headers to data fields using nested field_mapping.json (2025-04-21).
# Fixes Amazon fields to use stats.current[4], adds robust checks for Percent Down calculations, and ensures proper price scaling (/100).
# Handles sparse fields with DROP, formats ASIN for Excel, and logs debug info (2025-04-21).
def safe_get(data, key, default=None):
    try:
        for k in key.split('.'):
            data = data[k]
        return data if data is not None else default
    except (KeyError, TypeError):
        return default

def get_field(data, deal_data, product_90, header, field):
    logging.debug(f"Header={header}, Field={field}")
    stats_90 = product_90.get('stats', {}) if product_90 else {}
    stats_365 = data.get('stats', {}) if data else {}
    current = stats_365.get('current', [-1] * 20)

    # Check nested FIELD_MAPPING groups
    field_value = None
    for group in ["basic", "sales_ranks", "reviews", "buy_box", "buy_box_used", 
                  "amazon", "new", "new_third_party_fba", "new_third_party_fbm", 
                  "used", "used_conditions", "list_price", "offer_counts"]:
        if header in FIELD_MAPPING.get(group, {}):
            field_value = FIELD_MAPPING[group][header]
            break
    if field_value:
        field = field_value

    # Sparse fields that should return DROP
    sparse_fields = [
        "Variation Attributes", "Author", "Buy Box - Stock", "Amazon - Stock",
        "New - Stock", "New, 3rd Party FBA - Stock", "New, 3rd Party FBM - Stock",
        "Buy Box Used - Stock", "Used - Stock", "Used, like new - Stock",
        "Used, very good - Stock", "Used, good - Stock", "Used, acceptable - Stock",
        "List Price - Stock"
    ]
    if header in sparse_fields:
        value = data.get(field, '') if data else deal_data.get(field, '')
        if value in [None, '', 'None', -1, 0]:
            logging.debug(f"{header} - sparse field, marking DROP")
            return "DROP"

    # Amazon fields
    if header.startswith("Amazon - "):
        periods = {
            "Current": ("current", stats_365), "30 days avg.": ("avg30", stats_90),
            "60 days avg.": ("avg", stats_90), "90 days avg.": ("avg90", stats_90),
            "180 days avg.": ("avg180", stats_365), "365 days avg.": ("avg365", stats_365),
            "Lowest": ("min", stats_365), "Lowest 365 days": ("min365", stats_365),
            "Highest": ("max", stats_365), "Highest 365 days": ("max365", stats_365),
            "90 days OOS": ("outOfStockPercentage90", stats_90)
        }
        period_key, stats_source = periods.get(header.replace("Amazon - ", ""), (None, None))
        if period_key and stats_source:
            value = stats_source.get(period_key, [-1] * 20)[4] if len(stats_source.get(period_key, [-1] * 20)) > 4 else -1
            if period_key in ["min", "max", "min365", "max365"] and isinstance(value, list):
                value = min(value) if "min" in period_key else max(value) if value else -1
            logging.debug(f"{header} - value={value}")
            if period_key == "outOfStockPercentage90":
                return f"{value}%" if value >= 0 else '-'
            return f"${value / 100:.2f}" if value is not None and value > 0 else 'DROP' if "60 days" in header or "365 days" in header else '-'

    # Core fields
    if header == "Percent Down 90":
        avg = stats_90.get('avg90', [-1] * 20)[2]  # Used
        curr = stats_90.get('current', [-1] * 20)[2]  # Used
        value = ((avg - curr) / avg * 100) if avg is not None and curr is not None and avg > 0 and curr >= 0 else -1
        logging.debug(f"Percent Down 90 (ASIN {product_90.get('asin')}): avg[2]={avg}, current[2]={curr}, value={value}")
        return f"{value:.0f}%" if value >= 0 else '-'
    elif header == "Avg. Price 90":
        value = stats_90.get('avg90', [-1] * 20)[2]  # Used
        logging.debug(f"Avg. Price 90 (ASIN {product_90.get('asin')}): value={value}")
        return f"${value / 100:.2f}" if value is not None and value > 0 else '-'
    elif header == "Percent Down 365":
        avg = stats_365.get('avg365', [-1] * 20)[2]  # Used
        curr = stats_365.get('current', [-1] * 20)[2]  # Used
        value = ((avg - curr) / avg * 100) if avg is not None and curr is not None and avg > 0 and curr >= 0 else -1
        logging.debug(f"Percent Down 365 (ASIN {product_90.get('asin')}): avg[2]={avg}, current[2]={curr}, value={value}")
        return f"{value:.0f}%" if value >= 0 else '-'
    elif header == "Avg. Price 365":
        value = stats_365.get('avg365', [-1] * 20)[2]  # Used
        logging.debug(f"Avg. Price 365 (ASIN {product_90.get('asin')}): used={value}")
        return f"${value / 100:.2f}" if value is not None and value > 0 else '-'
    elif header == "Price Now":
        value = stats_90.get('current', [-1] * 20)[2]  # Used
        logging.debug(f"Price Now (ASIN {product_90.get('asin')}): used={value}")
        if value is not None and value >= 2000 and value <= 30100:  # $20-$301
            return f"${value / 100:.2f}"
        return '-'
    elif header == "Price Now Source":
        buy_box_used = stats_90.get('current', [-1] * 20)[11]  # Buy Box Used
        used = stats_90.get('current', [-1] * 20)[2]  # Used
        logging.debug(f"Price Now Source (ASIN {product_90.get('asin')}): buy_box_used={buy_box_used}, used={used}")
        if buy_box_used is not None and buy_box_used >= 2000 and buy_box_used <= 30100:  # $20-$301
            return "Buy Box Used"
        if used is not None and used >= 2000 and used <= 30100:  # $20-$301
            return "Used"
        return "-"
    elif header == "Deal found":
        ts = deal_data.get('creationDate', 0)
        logging.debug(f"Deal found - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "last update":
        ts = deal_data.get('lastUpdate', 0)
        logging.debug(f"last update - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "last price change":
        ts = deal_data.get('currentSince', [-1] * 20)[11]
        logging.debug(f"last price change - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "Sales Rank - Reference":
        cat_id = data.get('salesRankReference', 0)
        logging.debug(f"Sales Rank - Reference - cat_id={cat_id}")
        return f"https://www.amazon.com/b/?node={cat_id}" if cat_id > 0 else '-'
    elif header == "FBA Pick&Pack Fee":
        weight = data.get('packageWeight', 0)
        height = data.get('packageHeight', 0)
        length = data.get('packageLength', 0)
        width = data.get('packageWidth', 0)
        binding = data.get('binding', '')
        logging.debug(f"FBA Pick&Pack Fee - weight={weight}g, dims=[{height},{length},{width}], binding={binding}")
        return calculate_fba_fee(weight, height, length, width, binding)
    elif header == "Referral Fee %":
        asin = data.get('asin', '')
        product_type = data.get('productType', 0)
        binding = data.get('binding', '')
        logging.debug(f"Referral Fee % - asin={asin}, product_type={PRODUCT_TYPES.get(product_type, str(product_type))}, binding={binding}")
        return calculate_referral_fee(asin, PRODUCT_TYPES.get(product_type, str(product_type)), binding)
    elif header == "Tracking since":
        ts = data.get('trackingSince', 0)
        logging.debug(f"Tracking since - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 0 else None
        return dt.strftime('%Y-%m-%d') if dt else '-'
    elif header == "Categories - Root":
        category_tree = data.get('categoryTree', [])
        return category_tree[0]['name'] if category_tree else '-'
    elif header == "Categories - Sub":
        category_tree = data.get('categoryTree', [])
        return ', '.join(cat['name'] for cat in category_tree[2:]) if len(category_tree) > 2 else '-'
    elif header == "Categories - Tree":
        return format_categories_tree(data.get('categoryTree', []))
    elif header == "Freq. Bought Together":
        return format_freq_bought_together(data.get('frequentlyBoughtTogether', []))
    elif header == "Type":
        product_type = data.get('productType', 0)
        logging.debug(f"Type - productType={product_type}")
        return PRODUCT_TYPES.get(product_type, str(product_type))
    elif header == "Contributors":
        return format_contributors(data.get('contributors', []))
    elif header == "Languages":
        return format_languages(data.get('languages', []))
    elif header == "Publication Date":
        return format_date(data.get('publicationDate', ''), full_date=False)
    elif header == "Release Date":
        return format_date(data.get('releaseDate', ''), full_date=True)
    elif header == "Listed since":
        ts = data.get('listedSince', 0)
        logging.debug(f"Listed since - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 0 else None
        return dt.strftime('%Y-%m-%d') if dt else '-'
    elif header == "ASIN":
        asin = data.get('asin', '')
        logging.debug(f"ASIN - raw={asin}")
        return f'="{asin.zfill(10)}"'
    elif header == "AMZ link":
        asin = data.get('asin', '')
        logging.debug(f"AMZ link - asin={asin}")
        return f"https://www.amazon.com/dp/{asin}" if asin else '-'
    elif header == "Keepa Link":
        asin = data.get('asin', '')
        logging.debug(f"Keepa Link - asin={asin}")
        return f"https://keepa.com/#!product/1-{asin}" if asin else '-'
    elif header == "Package - Quantity":
        value = data.get('packageQuantity', 0)
        logging.debug(f"Package - Quantity - value={value}")
        return str(value)
    elif header == "Package Weight":
        value = data.get('packageWeight', 0)
        logging.debug(f"Package Weight - value={value}g")
        return str(value) if value > 0 else '-'
    elif header == "Package Height":
        value = data.get('packageHeight', 0)
        logging.debug(f"Package Height - value={value}mm")
        return str(value) if value > 0 else '-'
    elif header == "Package Length":
        value = data.get('packageLength', 0)
        logging.debug(f"Package Length - value={value}mm")
        return str(value) if value > 0 else '-'
    elif header == "Package Width":
        value = data.get('packageWidth', 0)
        logging.debug(f"Package Width - value={value}mm")
        return str(value) if value > 0 else '-'

    # Generic stats handler
    if field.startswith('stats.'):
        if '[' in field:
            key, idx_part = field.split('[')
            idx = int(idx_part.rstrip(']'))
            if 'current' in key:
                value = stats_365.get('current', [-1] * 20)[idx]
            elif 'avg30' in key:
                value = stats_90.get('avg30', [-1] * 20)[idx]
            elif 'avg' in key and '60 days' in header:
                value = stats_90.get('avg', [-1] * 20)[idx]
            elif 'avg90' in key:
                value = stats_90.get('avg90', [-1] * 20)[idx]
            elif 'avg180' in key:
                value = stats_365.get('avg180', [-1] * 20)[idx]
            elif 'avg365' in key:
                value = stats_365.get('avg365', [-1] * 20)[idx]
            elif 'min' in key:
                value = stats_365.get('min', [-1] * 20)[idx]
                if isinstance(value, list):
                    value = min(value) if value else -1
            elif 'min365' in key:
                value = stats_365.get('min365', [-1] * 20)[idx]
                if isinstance(value, list):
                    value = min(value) if value else -1
            elif 'max' in key:
                value = stats_365.get('max', [-1] * 20)[idx]
                if isinstance(value, list):
                    value = max(value) if value else -1
            elif 'max365' in key:
                value = stats_365.get('max365', [-1] * 20)[idx]
                if isinstance(value, list):
                    value = max(value) if value else -1
            elif 'outOfStockPercentage90' in key:
                value = stats_90.get('outOfStockPercentage90', [-1] * 20)[idx]
            else:
                value = -1
        else:
            key = field.split('.')[1]
            if 'salesRankDrops' in key:
                drop_period = key.split('salesRankDrops')[1]
                value = stats_90.get(f'salesRankDrops{drop_period}', -1) if '90' in drop_period or '30' in drop_period or '60' in drop_period else stats_365.get(f'salesRankDrops{drop_period}', -1)
            else:
                value = -1
        logging.debug(f"{header} - stats field={field}, value={value}")
        if header.startswith("Reviews - "):
            return str(value) if value > 0 else 'DROP' if '60 days' in header else '-'
        elif header.startswith("Sales Rank - "):
            if 'Drops' in header:
                return str(value) if value > 0 else 'DROP' if '60 days' in header else '-'
            return f"{value:,}" if value is not None and value > 0 else 'DROP' if '60 days' in header else '-'
        elif header.startswith("Buy Box - ") or header.startswith("Buy Box Used - ") or header.startswith("Used, ") or header.startswith("New - ") or header.startswith("New, 3rd Party"):
            if 'OOS' in header:
                return f"{value}%" if value >= 0 else '-'
            return f"${value / 100:.2f}" if value is not None and value > 0 else 'DROP' if '60 days' in header else '-'
        elif header.startswith(("New Offer Count", "Used Offer Count")):
            return f"{value:,}" if value > 0 else '-'
        elif 'OOS' in header:
            return f"{value}%" if value >= 0 else '-'

    # Direct fields
    value = data.get(field, '') if data else deal_data.get(field, '')
    if value in [None, '', 'None', -1, 0] and header not in ['Package - Quantity']:
        logging.debug(f"{header} - direct field={field}, value={value}, defaulting to '-'")
        return '-'
    if header in ['Contributors', 'Freq. Bought Together', 'Languages', 'Categories - Tree']:
        value = globals()[f"format_{field}"](value)
    elif header in ['Publication Date', 'Release Date']:
        value = format_date(value, full_date=(header == 'Release Date'))
    elif header == 'Type':
        value = PRODUCT_TYPES.get(value, str(value))
    logging.debug(f"{header} - direct field={field}, value={value}")
    return str(value)
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