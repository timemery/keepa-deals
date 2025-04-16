# Chunk 1 starts
import requests
import json
import csv
import os
import urllib.parse
from datetime import datetime, timedelta
from pytz import timezone

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
# Dev Log: Restored fetch_deals (GET with selection JSON) and fetch_product (stats_period, rate limiting) from working script.
# Handles buy_box_used for stats.current[11]. Matches main() calls in Chunk 8.
import time

def fetch_deals(page):
    selection = DEAL_FILTERS.copy()
    selection["page"] = page
    selection_str = json.dumps(selection)
    url = f'{BASE_URL}/deal?key={api_key}&selection={urllib.parse.quote(selection_str)}'
    response = requests.get(url, headers=headers)
    deals = response.json().get('deals', {}).get('dr', [])
    print(f"DEBUG: fetch_deals - page={page}, deals_count={len(deals)}")
    return deals

def fetch_product(asin, stats_period):
    url = f'{BASE_URL}/product?key={api_key}&domain=1&asin={asin}&stats={stats_period}&offers=20&rating=1'
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        print("Rate limit hit - sleeping for 5 seconds")
        time.sleep(5)
        return fetch_product(asin, stats_period)  # Retry
    tokens_left = response.json().get('tokensLeft', -1)
    if tokens_left < 100:
        sleep_time = max(5, (100 - tokens_left) // 5)
        print(f"Low tokens ({tokens_left}) - sleeping for {sleep_time} seconds")
        time.sleep(sleep_time)
    product = response.json().get('products', [])[0] if response.json().get('products') else {}
    offers = product.get('offers', [])
    buy_box_used = -1
    if not offers:
        pass
    for offer in offers:
        is_bb = offer.get('isBuyBox', False)
        cond = offer.get('condition', -1)
        price = offer.get('price', -1)
        if is_bb and cond in [2, 3, 4, 5]:
            buy_box_used = price
            break
    if 'stats' in product and 'current' in product['stats']:
        product['stats']['current'][11] = buy_box_used
    else:
        product['stats'] = {'current': [-1] * 12}
        product['stats']['current'][11] = buy_box_used
    print(f"DEBUG: fetch_product - ASIN={asin}, stats_period={stats_period}, buy_box_used={buy_box_used}")
    return product
# Chunk 2 ends

# Chunk 3 starts
# Dev Log: Restored lb-based FBA fee logic from working script to handle missing dims.
def calculate_fba_fee(weight_g, height_mm, length_mm, width_mm):
    weight_lb = weight_g / 453.59237
    dims_in = [x / 25.4 for x in [height_mm, length_mm, width_mm]]
    dims_in.sort()
    print(f"FBA Calc: weight={weight_lb:.2f} lb, dims={dims_in}")
    if weight_lb <= 15 and dims_in[2] <= 18 and dims_in[1] <= 14 and dims_in[0] <= 8:
        if weight_lb <= 0.25: total_fee = 3.15
        elif weight_lb <= 1: total_fee = 3.53
        elif weight_lb <= 3: total_fee = 6.21
        else: total_fee = 6.21 + (weight_lb - 3) * 0.15
        print(f"Small Standard (2025): total=${total_fee:.2f}")
        return f"${total_fee:.2f}"
    print("Oversize: defaulting to $10.53 base (2025)")
    return "$10.53"

def calculate_referral_fee(price, category_tree):
    price = float(price.replace('$', '')) if isinstance(price, str) else price
    root_cat = category_tree[0]['catId'] if category_tree else 0
    print(f"Referral Calc: price=${price}, root_cat={root_cat}")
    if root_cat == 283155:
        rate = 14.95
        fee = max(price * (rate / 100), 0.30)
        print(f"Books: {rate}%, fee=${fee:.2f}")
        return f"{rate:.2f}%"
    elif root_cat in [2335752011, 163856011]:
        rate = 8.0
        fee = price * (rate / 100)
        print(f"Electronics: {rate}%, fee=${fee:.2f}")
        return f"{rate:.2f}%"
    elif root_cat == 3760911:
        rate = 10.0 if price <= 10 else 15.0
        fee = price * (rate / 100)
        print(f"Beauty: {rate}%, fee=${fee:.2f}")
        return f"{rate:.2f}%"
    print("Default: 15.0%")
    return "15.00%"
# Chunk 3 ends

# Chunk 4 starts
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
# Dev Log: Merged restored logic for Percent Down 365, Type, ASIN, last update, etc., with nested field_mapping support.
# Fixed Reviews - Review Count (stats.current[17]), list handling (min/max), FBA fee paths.
def get_field(data, deal_data, product_90, header, field):
    print(f"DEBUG: Header={header}, Field={field}")
    stats_90 = product_90.get('stats', {}) if product_90 else {}
    stats_365 = data.get('stats', {}) if data else {}
    current = deal_data.get('current', [-1] * 20)

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

    # Amazon fields
    if header == "Amazon - Current":
        value = current[0] if len(current) > 0 and current[0] is not None else -1
        print(f"DEBUG: Amazon - Current - value={value}")
        if value <= 0 and stats_90.get('avg', [-1] * 20)[4] > 0:
            value = stats_90['avg'][4]
            print(f"DEBUG: Amazon - Current - fallback to avg[4]={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 30 days avg.":
        value = stats_90.get('avg30', [-1] * 20)[4] if len(stats_90.get('avg30', [-1] * 20)) > 4 and stats_90.get('avg30', [-1] * 20)[4] is not None else -1
        print(f"DEBUG: Amazon - 30 days avg. - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 60 days avg.":
        value = stats_90.get('avg', [-1] * 20)[4] if len(stats_90.get('avg', [-1] * 20)) > 4 and stats_90.get('avg', [-1] * 20)[4] is not None else -1
        print(f"DEBUG: Amazon - 60 days avg. - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 90 days avg.":
        value = stats_90.get('avg90', [-1] * 20)[4] if len(stats_90.get('avg90', [-1] * 20)) > 4 and stats_90.get('avg90', [-1] * 20)[4] is not None else -1
        print(f"DEBUG: Amazon - 90 days avg. - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 180 days avg.":
        value = stats_365.get('avg180', [-1] * 20)[4] if len(stats_365.get('avg180', [-1] * 20)) > 4 and stats_365.get('avg180', [-1] * 20)[4] is not None else -1
        print(f"DEBUG: Amazon - 180 days avg. - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 365 days avg.":
        value = stats_365.get('avg365', [-1] * 20)[4] if len(stats_365.get('avg365', [-1] * 20)) > 4 and stats_365.get('avg365', [-1] * 20)[4] is not None else -1
        print(f"DEBUG: Amazon - 365 days avg. - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Lowest":
        value = stats_365.get('min', [-1] * 20)[4] if len(stats_365.get('min', [-1] * 20)) > 4 and stats_365.get('min', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = min(value) if value else -1
        print(f"DEBUG: Amazon - Lowest - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Lowest 365 days":
        value = stats_365.get('min365', [-1] * 20)[4] if len(stats_365.get('min365', [-1] * 20)) > 4 and stats_365.get('min365', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = min(value) if value else -1
        print(f"DEBUG: Amazon - Lowest 365 days - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Highest":
        value = stats_365.get('max', [-1] * 20)[4] if len(stats_365.get('max', [-1] * 20)) > 4 and stats_365.get('max', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = max(value) if value else -1
        print(f"DEBUG: Amazon - Highest - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Highest 365 days":
        value = stats_365.get('max365', [-1] * 20)[4] if len(stats_365.get('max365', [-1] * 20)) > 4 and stats_365.get('max365', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = max(value) if value else -1
        print(f"DEBUG: Amazon - Highest 365 days - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'

    # Core fields (restored from working script)
    if header == "Percent Down 90":
        avg = stats_90.get('avg', [-1] * 20)
        value = ((avg[2] - current[2]) / avg[2] * 100) if len(avg) > 2 and avg[2] is not None and avg[2] > 0 and len(current) > 2 and current[2] is not None else -1
        print(f"DEBUG: Percent Down 90 - avg[2]={avg[2] if len(avg) > 2 else 'N/A'}, current[2]={current[2] if len(current) > 2 else 'N/A'}, value={value}")
        return f"{value:.0f}%" if value != -1 else '-'
    elif header == "Avg. Price 90":
        value = stats_90.get('avg', [-1] * 20)[2] if len(stats_90.get('avg', [-1] * 20)) > 2 and stats_90.get('avg', [-1] * 20)[2] is not None else -1
        print(f"DEBUG: Avg. Price 90 - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Percent Down 365":
        avg = stats_365.get('avg', [-1] * 20)
        value = ((avg[2] - current[2]) / avg[2] * 100) if len(avg) > 2 and avg[2] is not None and avg[2] > 0 and len(current) > 2 and current[2] is not None else -1
        print(f"DEBUG: Percent Down 365 - avg[2]={avg[2] if len(avg) > 2 else 'N/A'}, current[2]={current[2] if len(current) > 2 else 'N/A'}, value={value}")
        return f"{value:.0f}%" if value != -1 else '-'
    elif header == "Avg. Price 365":
        value = stats_365.get('avg', [-1] * 20)[2] if len(stats_365.get('avg', [-1] * 20)) > 2 and stats_365.get('avg', [-1] * 20)[2] is not None else -1
        print(f"DEBUG: Avg. Price 365 - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Price Now":
        value = current[2] if len(current) > 2 and current[2] is not None else -1
        print(f"DEBUG: Price Now - value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Deal found":
        ts = deal_data.get('creationDate', 0)
        print(f"DEBUG: Deal found - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "last update":
        ts = deal_data.get('lastUpdate', 0)
        print(f"DEBUG: last update - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "last price change":
        ts = deal_data.get('currentSince', [-1] * 20)[2] if len(deal_data.get('currentSince', [-1] * 20)) > 2 and deal_data.get('currentSince', [-1] * 20)[2] is not None else -1
        print(f"DEBUG: last price change - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "Sales Rank - Reference":
        cat_id = data.get('salesRankReference', 0)
        print(f"DEBUG: Sales Rank - Reference - cat_id={cat_id}")
        return f"https://www.amazon.com/b/?node={cat_id}" if cat_id > 0 else '-'
    elif header == "FBA Pick&Pack Fee":
        weight = data.get('packageWeight', 0)
        height = data.get('packageHeight', 0)
        length = data.get('packageLength', 0)
        width = data.get('packageWidth', 0)
        print(f"DEBUG: FBA Pick&Pack Fee - weight={weight}g ({weight / 453.59237:.2f} lb), dims=[{height},{length},{width}]")
        return calculate_fba_fee(weight, height, length, width)
    elif header == "Referral Fee %":
        price = get_field(data, deal_data, product_90, "Price Now", "stats.current[2]")
        category_tree = data.get('categoryTree', [])
        print(f"DEBUG: Referral Fee % - price={price}")
        return calculate_referral_fee(price, category_tree)
    elif header == "Tracking since":
        ts = data.get('trackingSince', 0)
        print(f"DEBUG: Tracking since - raw ts={ts}")
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
        print(f"DEBUG: Type - productType={product_type}")
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
        print(f"DEBUG: Listed since - raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 0 else None
        return dt.strftime('%Y-%m-%d') if dt else '-'
    elif header == "ASIN":
        asin = data.get('asin', '')
        print(f"DEBUG: ASIN - raw={asin}")
        return f'"{asin.zfill(10)}"'
    elif header == "AMZ link":
        asin = data.get('asin', '')
        print(f"DEBUG: AMZ link - asin={asin}")
        return f"https://www.amazon.com/dp/{asin}" if asin else '-'
    elif header == "Keepa Link":
        asin = data.get('asin', '')
        print(f"DEBUG: Keepa Link - asin={asin}")
        return f"https://keepa.com/#!product/1-{asin}" if asin else '-'

    # Generic stats handler (restored list handling)
    if field.startswith('stats.'):
        if '[' in field:
            key, idx_part = field.split('[')
            idx = int(idx_part.rstrip(']'))
            if 'current' in key:
                value = stats_365.get('current', [-1] * 20)[idx] if len(stats_365.get('current', [-1] * 20)) > idx and stats_365.get('current', [-1] * 20)[idx] is not None else -1
            elif 'avg30' in key:
                value = stats_90.get('avg30', [-1] * 20)[idx] if len(stats_90.get('avg30', [-1] * 20)) > idx and stats_90.get('avg30', [-1] * 20)[idx] is not None else -1
            elif 'avg' in key and '60 days' in header:
                value = stats_90.get('avg', [-1] * 20)[idx] if len(stats_90.get('avg', [-1] * 20)) > idx and stats_90.get('avg', [-1] * 20)[idx] is not None else -1
            elif 'avg90' in key:
                value = stats_90.get('avg90', [-1] * 20)[idx] if len(stats_90.get('avg90', [-1] * 20)) > idx and stats_90.get('avg90', [-1] * 20)[idx] is not None else -1
            elif 'avg180' in key:
                value = stats_365.get('avg180', [-1] * 20)[idx] if len(stats_365.get('avg180', [-1] * 20)) > idx and stats_365.get('avg180', [-1] * 20)[idx] is not None else -1
            elif 'avg365' in key:
                value = stats_365.get('avg365', [-1] * 20)[idx] if len(stats_365.get('avg365', [-1] * 20)) > idx and stats_365.get('avg365', [-1] * 20)[idx] is not None else -1
            elif 'min' in key:
                value = stats_365.get('min', [-1] * 20)[idx] if len(stats_365.get('min', [-1] * 20)) > idx and stats_365.get('min', [-1] * 20)[idx] is not None else -1
                if isinstance(value, list):
                    value = min(value) if value else -1
            elif 'min365' in key:
                value = stats_365.get('min365', [-1] * 20)[idx] if len(stats_365.get('min365', [-1] * 20)) > idx and stats_365.get('min365', [-1] * 20)[idx] is not None else -1
                if isinstance(value, list):
                    value = min(value) if value else -1
            elif 'max' in key:
                value = stats_365.get('max', [-1] * 20)[idx] if len(stats_365.get('max', [-1] * 20)) > idx and stats_365.get('max', [-1] * 20)[idx] is not None else -1
                if isinstance(value, list):
                    value = max(value) if value else -1
            elif 'max365' in key:
                value = stats_365.get('max365', [-1] * 20)[idx] if len(stats_365.get('max365', [-1] * 20)) > idx and stats_365.get('max365', [-1] * 20)[idx] is not None else -1
                if isinstance(value, list):
                    value = max(value) if value else -1
            elif 'outOfStockPercentage90' in key:
                value = stats_90.get('outOfStockPercentage90', [-1] * 20)[idx] if len(stats_90.get('outOfStockPercentage90', [-1] * 20)) > idx and stats_90.get('outOfStockPercentage90', [-1] * 20)[idx] is not None else -1
            else:
                value = -1
        else:
            key = field.split('.')[1]
            if 'salesRankDrops' in key:
                drop_period = key.split('salesRankDrops')[1]
                value = stats_90.get(f'salesRankDrops{drop_period}', -1) if '90' in drop_period or '30' in drop_period or '60' in drop_period else stats_365.get(f'salesRankDrops{drop_period}', -1)
            else:
                value = -1
        print(f"DEBUG: {header} - stats field={field}, value={value}")
        # Format based on field type
        if header.startswith("Reviews - "):
            return str(value) if value > 0 else '-'
        elif header.startswith("Sales Rank - "):
            if 'Drops' in header:
                return str(value) if value > 0 else '-'
            return f"{value:,}" if value > 0 else '-'
        elif header.startswith("Buy Box - "):
            if 'OOS' in header:
                return f"{value}%" if value >= 0 else '-'
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header.startswith(("New Offer Count", "Used Offer Count")):
            return f"{value:,}" if value > 0 else '-'
        elif 'OOS' in header:
            return f"{value}%" if value >= 0 else '-'
        return f"${value / 100:.2f}" if value > 0 else '-'

    # Direct fields
    value = data.get(field, '') if data else deal_data.get(field, '')
    if value in [None, '', 'None', -1, 0] and header not in ['Package - Quantity']:
        print(f"DEBUG: {header} - direct field={field}, value={value}, defaulting to '-'")
        return '-'
    if header in ['Contributors', 'Freq. Bought Together', 'Languages', 'Categories - Tree']:
        value = globals()[f"format_{field}"](value)
    elif header in ['Publication Date', 'Release Date']:
        value = format_date(value, full_date=(header == 'Release Date'))
    elif header == 'Type':
        value = PRODUCT_TYPES.get(value, str(value))
    print(f"DEBUG: {header} - direct field={field}, value={value}")
    return str(value)
# Chunk 5 ends

# Chunk 6 starts
# Dev Log: Restored simple loop from working script, using nested field_mapping for all headers.
def extract_fields(deal, product_90, product_365):
    row = [get_field(product_365, deal, product_90, h, FIELD_MAPPING.get(h, h)) for h in HEADERS]
    print(f"DEBUG: Extracted ASIN {deal.get('asin')}: {row}")
    return row
# Chunk 6 ends

# Chunk 7 starts
import os
def write_csv(data, filename=os.path.join(os.path.dirname(__file__), 'keepa_full_deals_v6.csv')):
    print(f"DEBUG: CSV path = {filename}")
    try:
        if os.path.exists(filename):
            os.remove(filename)
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL)
            writer.writerow(HEADERS)
            writer.writerows(data)
        print(f"File size: {os.path.getsize(filename)} bytes")
    except Exception as e:
        print(f"Error writing CSV {filename}: {e}")
        raise
# Chunk 7 ends

# Chunk 8 starts
# Dev Log: Restored main() from working script, fetching deals and products for 90/365-day stats.
import sys

def main():
    print("Starting Process500_Deals_v6...")
    sys.stdout.flush()
    deals = fetch_deals(0)
    print(f"Deals returned: {len(deals)}")
    sys.stdout.flush()
    if deals:
        print("Fetching product data...")
        sys.stdout.flush()
        max_deals = min(10, len(deals))
        products_90 = {}
        products_365 = {}
        for i, deal in enumerate(deals[:max_deals]):
            asin = deal['asin']
            print(f"Fetching ASIN {asin} ({i+1}/{max_deals})...")
            sys.stdout.flush()
            products_90[asin] = fetch_product(asin, 90)
            products_365[asin] = fetch_product(asin, 365)
        print("Processing deals...")
        sys.stdout.flush()
        rows = []
        for i, deal in enumerate(deals[:max_deals]):
            asin = deal['asin']
            print(f"Extracting ASIN {asin} ({i+1}/{max_deals})...")
            sys.stdout.flush()
            row = extract_fields(deal, products_90.get(asin, {}), products_365.get(asin, {}))
            rows.append(row)
        print(f"Rows generated: {len(rows)}")
        sys.stdout.flush()
        write_csv(rows)
    else:
        print("No deals fetched!")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
# Chunk 8 ends