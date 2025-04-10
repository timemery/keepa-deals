# Chunk 1 starts
import requests
import json
import csv
import os
import urllib.parse
from datetime import datetime, timedelta
from pytz import timezone

with open('/home/timscripts/keepa_api/config.json') as file:
    config = json.load(file)
    api_key = config['api_key']

with open('/home/timscripts/keepa_api/deal_filters.json') as file:
    DEAL_FILTERS = json.load(file)

with open('/home/timscripts/keepa_api/headers.json') as file:
    HEADERS = json.load(file)

with open('/home/timscripts/keepa_api/field_mapping.json') as file:
    FIELD_MAPPING = json.load(file)

BASE_URL = 'https://api.keepa.com'
headers = {'accept': 'application/json', 'accept-encoding': 'identity'}
KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')
PRODUCT_TYPES = {0: 'ABIS_BOOK', 1: 'ABIS_MUSIC'}
# Chunk 1 ends

# Chunk 2 starts
def fetch_deals(page):
    selection = DEAL_FILTERS.copy()
    selection["page"] = page
    selection_str = json.dumps(selection)
    url = f'{BASE_URL}/deal?key={api_key}&selection={urllib.parse.quote(selection_str)}'
    response = requests.get(url, headers=headers)
    print(f"Deal fetch status: {response.status_code}")
    deals = response.json().get('deals', {}).get('dr', [])
    print(f"Deals fetched: {len(deals)} - Tokens left: {response.json().get('tokensLeft')}")
    return deals

def fetch_product(asin, stats_period):
    url = f'{BASE_URL}/product?key={api_key}&domain=1&asin={asin}&stats={stats_period}'
    print(f"Fetching product URL: {url}")
    response = requests.get(url, headers=headers)
    print(f"Product fetch status: {response.status_code}")
    product = response.json().get('products', [])[0] if response.json().get('products') else {}
    print(f"ASIN {asin} stats ({stats_period}-day): {json.dumps(product.get('stats', {}), indent=2)}")
    return product
# Chunk 2 ends

# Chunk 3 starts
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
    print("Default: 14.95%")
    return "14.95%"
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
def get_field(data, deal_data, product_90, header, field):
    print(f"Header: {header}, Field: {field}")
    stats_90 = product_90.get('stats', {})
    stats_365 = data.get('stats', {})
    current = deal_data.get('current', [-1] * 20)

    # v6 Amazon fields
    if header == "Amazon - Current":
        value = current[0] if len(current) > 0 and current[0] is not None else -1
        print(f"Amazon - Current: value={value}")
        if value <= 0 and stats_90.get('avg', [-1] * 20)[4] > 0:
            value = stats_90['avg'][4]  # Fallback to 90-day avg Amazon price
            print(f"Amazon - Current fallback to avg[4]: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 30 days avg.":
        value = stats_90.get('avg30', [-1] * 20)[4] if len(stats_90.get('avg30', [-1] * 20)) > 4 and stats_90.get('avg30', [-1] * 20)[4] is not None else -1
        print(f"Amazon - 30 days avg.: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 60 days avg.":
        value = stats_90.get('avg', [-1] * 20)[4] if len(stats_90.get('avg', [-1] * 20)) > 4 and stats_90.get('avg', [-1] * 20)[4] is not None else -1
        print(f"Amazon - 60 days avg.: value={value}")
        return f"${value / 100:.2f}" if value > 0 else 'DROP'
    elif header == "Amazon - 90 days avg.":
        value = stats_90.get('avg90', [-1] * 20)[4] if len(stats_90.get('avg90', [-1] * 20)) > 4 and stats_90.get('avg90', [-1] * 20)[4] is not None else -1
        print(f"Amazon - 90 days avg.: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 180 days avg.":
        value = stats_365.get('avg180', [-1] * 20)[4] if len(stats_365.get('avg180', [-1] * 20)) > 4 and stats_365.get('avg180', [-1] * 20)[4] is not None else -1
        print(f"Amazon - 180 days avg.: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - 365 days avg.":
        value = stats_365.get('avg365', [-1] * 20)[4] if len(stats_365.get('avg365', [-1] * 20)) > 4 and stats_365.get('avg365', [-1] * 20)[4] is not None else -1
        print(f"Amazon - 365 days avg.: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Lowest":
        value = stats_365.get('min', [-1] * 20)[4] if len(stats_365.get('min', [-1] * 20)) > 4 and stats_365.get('min', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = min(value) if value else -1
        print(f"Amazon - Lowest: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Lowest 365 days":
        value = stats_365.get('min365', [-1] * 20)[4] if len(stats_365.get('min365', [-1] * 20)) > 4 and stats_365.get('min365', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = min(value) if value else -1
        print(f"Amazon - Lowest 365 days: value={value}")
        return f"${value / 100:.2f}" if value > 0 else 'DROP'
    elif header == "Amazon - Highest":
        value = stats_365.get('max', [-1] * 20)[4] if len(stats_365.get('max', [-1] * 20)) > 4 and stats_365.get('max', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = max(value) if value else -1
        print(f"Amazon - Highest: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Amazon - Highest 365 days":
        value = stats_365.get('max365', [-1] * 20)[4] if len(stats_365.get('max365', [-1] * 20)) > 4 and stats_365.get('max365', [-1] * 20)[4] is not None else -1
        if isinstance(value, list):
            value = max(value) if value else -1
        print(f"Amazon - Highest 365 days: value={value}")
        return f"${value / 100:.2f}" if value > 0 else 'DROP'

    # v5 core fields
    if header == "Percent Down 90":
        avg = stats_90.get('avg', [-1] * 20)
        value = ((avg[2] - current[2]) / avg[2] * 100) if len(avg) > 2 and avg[2] is not None and avg[2] > 0 and len(current) > 2 and current[2] is not None else -1
        print(f"Percent Down 90: avg[2]={avg[2] if len(avg) > 2 else 'N/A'}, current[2]={current[2] if len(current) > 2 else 'N/A'}, value={value}")
        return f"{value:.0f}%" if value != -1 else '-'
    elif header == "Avg. Price 90":
        value = stats_90.get('avg', [-1] * 20)[2] if len(stats_90.get('avg', [-1] * 20)) > 2 and stats_90.get('avg', [-1] * 20)[2] is not None else -1
        print(f"Avg. Price 90: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Percent Down 365":
        avg = stats_365.get('avg', [-1] * 20)
        value = ((avg[2] - current[2]) / avg[2] * 100) if len(avg) > 2 and avg[2] is not None and avg[2] > 0 and len(current) > 2 and current[2] is not None else -1
        print(f"Percent Down 365: avg[2]={avg[2] if len(avg) > 2 else 'N/A'}, current[2]={current[2] if len(current) > 2 else 'N/A'}, value={value}")
        return f"{value:.0f}%" if value != -1 else '-'
    elif header == "Avg. Price 365":
        value = stats_365.get('avg', [-1] * 20)[2] if len(stats_365.get('avg', [-1] * 20)) > 2 and stats_365.get('avg', [-1] * 20)[2] is not None else -1
        print(f"Avg. Price 365: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Price Now":
        value = current[2] if len(current) > 2 and current[2] is not None else -1
        print(f"Price Now: value={value}")
        return f"${value / 100:.2f}" if value > 0 else '-'
    elif header == "Deal found":
        ts = deal_data.get('creationDate', 0)
        print(f"Deal found: raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "last update":
        ts = deal_data.get('lastUpdate', 0)
        print(f"Last update: raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "last price change":
        ts = deal_data.get('currentSince', [-1] * 20)[2] if len(deal_data.get('currentSince', [-1] * 20)) > 2 and deal_data.get('currentSince', [-1] * 20)[2] is not None else -1
        print(f"Last price change: raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif header == "Sales Rank - Reference":
        cat_id = data.get('salesRankReference', 0)
        print(f"Sales Rank - Reference: cat_id={cat_id}")
        return f"https://www.amazon.com/b/?node={cat_id}" if cat_id > 0 else '-'
    elif header == "FBA Pick&Pack Fee":
        weight = data.get('packageWeight', 0)
        height = data.get('packageHeight', 0)
        length = data.get('packageLength', 0)
        width = data.get('packageWidth', 0)
        print(f"Package Weight Check: {weight}g ({weight / 453.59237:.2f} lb) for ASIN {data.get('asin')}")
        return calculate_fba_fee(weight, height, length, width)
    elif header == "Referral Fee %":
        price = get_field(data, deal_data, product_90, "Price Now", "stats.current[2]")
        category_tree = data.get('categoryTree', [])
        return calculate_referral_fee(price, category_tree)
    elif header == "Tracking since":
        ts = data.get('trackingSince', 0)
        print(f"Tracking since: raw ts={ts}")
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
        print(f"Type mapping: productType={product_type}")
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
        print(f"Listed since: raw ts={ts}")
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 0 else None
        return dt.strftime('%Y-%m-%d') if dt else '-'
    elif header == "ASIN":
        asin = data.get('asin', '')
        print(f"ASIN padding: raw={asin}")
        return f'"{asin.zfill(10)}"'
    elif header == "AMZ link":
        asin = data.get('asin', '')
        return f"https://www.amazon.com/dp/{asin}" if asin else '-'
    elif header == "Keepa Link":
        asin = data.get('asin', '')
        return f"https://keepa.com/#!product/1-{asin}" if asin else '-'
    elif header.startswith("Sales Rank - "):
        if header == "Sales Rank - Current":
            value = current[3] if len(current) > 3 and current[3] is not None else -1
            print(f"Sales Rank - Current: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - 30 days avg.":
            value = stats_90.get('avg30', [-1] * 20)[3] if len(stats_90.get('avg30', [-1] * 20)) > 3 and stats_90.get('avg30', [-1] * 20)[3] is not None else -1
            print(f"Sales Rank - 30 days avg.: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - 60 days avg.":
            value = stats_90.get('avg', [-1] * 20)[3] if len(stats_90.get('avg', [-1] * 20)) > 3 and stats_90.get('avg', [-1] * 20)[3] is not None else -1
            print(f"Sales Rank - 60 days avg.: value={value}")
            return f"{value:,}" if value > 0 else 'DROP'
        elif header == "Sales Rank - 90 days avg.":
            value = stats_90.get('avg90', [-1] * 20)[3] if len(stats_90.get('avg90', [-1] * 20)) > 3 and stats_90.get('avg90', [-1] * 20)[3] is not None else -1
            print(f"Sales Rank - 90 days avg.: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - 180 days avg.":
            value = stats_365.get('avg180', [-1] * 20)[3] if len(stats_365.get('avg180', [-1] * 20)) > 3 and stats_365.get('avg180', [-1] * 20)[3] is not None else -1
            print(f"Sales Rank - 180 days avg.: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - 365 days avg.":
            value = stats_365.get('avg365', [-1] * 20)[3] if len(stats_365.get('avg365', [-1] * 20)) > 3 and stats_365.get('avg365', [-1] * 20)[3] is not None else -1
            print(f"Sales Rank - 365 days avg.: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - Lowest":
            value = stats_365.get('min', [-1] * 20)[3] if len(stats_365.get('min', [-1] * 20)) > 3 and stats_365.get('min', [-1] * 20)[3] is not None else -1
            if isinstance(value, list):
                value = min(value) if value else -1
            print(f"Sales Rank - Lowest: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - Lowest 365 days":
            value = stats_365.get('min365', [-1] * 20)[3] if len(stats_365.get('min365', [-1] * 20)) > 3 and stats_365.get('min365', [-1] * 20)[3] is not None else -1
            if isinstance(value, list):
                value = min(value) if value else -1
            print(f"Sales Rank - Lowest 365 days: value={value}")
            return f"{value:,}" if value > 0 else 'DROP'
        elif header == "Sales Rank - Highest":
            value = stats_365.get('max', [-1] * 20)[3] if len(stats_365.get('max', [-1] * 20)) > 3 and stats_365.get('max', [-1] * 20)[3] is not None else -1
            if isinstance(value, list):
                value = max(value) if value else -1
            print(f"Sales Rank - Highest: value={value}")
            return f"{value:,}" if value > 0 else '-'
        elif header == "Sales Rank - Highest 365 days":
            value = stats_365.get('max365', [-1] * 20)[3] if len(stats_365.get('max365', [-1] * 20)) > 3 and stats_365.get('max365', [-1] * 20)[3] is not None else -1
            if isinstance(value, list):
                value = max(value) if value else -1
            print(f"Sales Rank - Highest 365 days: value={value}")
            return f"{value:,}" if value > 0 else 'DROP'
        elif header == "Sales Rank - Drops last 30 days":
            value = stats_90.get('salesRankDrops30', -1)
            print(f"Sales Rank - Drops last 30 days: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Sales Rank - Drops last 60 days":
            value = stats_90.get('salesRankDrops60', -1)
            print(f"Sales Rank - Drops last 60 days: value={value}")
            return str(value) if value > 0 else 'DROP'
        elif header == "Sales Rank - Drops last 90 days":
            value = stats_90.get('salesRankDrops90', -1)
            print(f"Sales Rank - Drops last 90 days: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Sales Rank - Drops last 180 days":
            value = stats_365.get('salesRankDrops180', -1)
            print(f"Sales Rank - Drops last 180 days: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Sales Rank - Drops last 365 days":
            value = stats_365.get('salesRankDrops365', -1)
            print(f"Sales Rank - Drops last 365 days: value={value}")
            return str(value) if value > 0 else '-'
    elif header.startswith("Reviews - "):
        if header == "Reviews - Current":
            value = current[18] if len(current) > 18 and current[18] is not None else -1
            print(f"Reviews - Current: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Reviews - 30 days avg.":
            value = stats_90.get('avg30', [-1] * 20)[18] if len(stats_90.get('avg30', [-1] * 20)) > 18 and stats_90.get('avg30', [-1] * 20)[18] is not None else -1
            print(f"Reviews - 30 days avg.: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Reviews - 60 days avg.":
            value = stats_90.get('avg', [-1] * 20)[18] if len(stats_90.get('avg', [-1] * 20)) > 18 and stats_90.get('avg', [-1] * 20)[18] is not None else -1
            print(f"Reviews - 60 days avg.: value={value}")
            return str(value) if value > 0 else 'DROP'
        elif header == "Reviews - 90 days avg.":
            value = stats_90.get('avg90', [-1] * 20)[18] if len(stats_90.get('avg90', [-1] * 20)) > 18 and stats_90.get('avg90', [-1] * 20)[18] is not None else -1
            print(f"Reviews - 90 days avg.: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Reviews - 180 days avg.":
            value = stats_365.get('avg180', [-1] * 20)[18] if len(stats_365.get('avg180', [-1] * 20)) > 18 and stats_365.get('avg180', [-1] * 20)[18] is not None else -1
            print(f"Reviews - 180 days avg.: value={value}")
            return str(value) if value > 0 else '-'
        elif header == "Reviews - 365 days avg.":
            value = stats_365.get('avg365', [-1] * 20)[18] if len(stats_365.get('avg365', [-1] * 20)) > 18 and stats_365.get('avg365', [-1] * 20)[18] is not None else -1
            print(f"Reviews - 365 days avg.: value={value}")
            return str(value) if value > 0 else '-'
    elif header.startswith("Buy Box - "):
        if header == "Buy Box - Current":
            value = current[10] if len(current) > 10 and current[10] is not None else -1
            print(f"Buy Box - Current: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - 30 days avg.":
            value = stats_90.get('avg30', [-1] * 20)[10] if len(stats_90.get('avg30', [-1] * 20)) > 10 and stats_90.get('avg30', [-1] * 20)[10] is not None else -1
            print(f"Buy Box - 30 days avg.: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - 60 days avg.":
            value = stats_90.get('avg', [-1] * 20)[10] if len(stats_90.get('avg', [-1] * 20)) > 10 and stats_90.get('avg', [-1] * 20)[10] is not None else -1
            print(f"Buy Box - 60 days avg.: value={value}")
            return f"${value / 100:.2f}" if value > 0 else 'DROP'
        elif header == "Buy Box - 90 days avg.":
            value = stats_90.get('avg90', [-1] * 20)[10] if len(stats_90.get('avg90', [-1] * 20)) > 10 and stats_90.get('avg90', [-1] * 20)[10] is not None else -1
            print(f"Buy Box - 90 days avg.: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - 180 days avg.":
            value = stats_365.get('avg180', [-1] * 20)[10] if len(stats_365.get('avg180', [-1] * 20)) > 10 and stats_365.get('avg180', [-1] * 20)[10] is not None else -1
            print(f"Buy Box - 180 days avg.: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - 365 days avg.":
            value = stats_365.get('avg365', [-1] * 20)[10] if len(stats_365.get('avg365', [-1] * 20)) > 10 and stats_365.get('avg365', [-1] * 20)[10] is not None else -1
            print(f"Buy Box - 365 days avg.: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - Lowest":
            value = stats_365.get('min', [-1] * 20)[10] if len(stats_365.get('min', [-1] * 20)) > 10 and stats_365.get('min', [-1] * 20)[10] is not None else -1
            if isinstance(value, list):
                value = min(value) if value else -1
            print(f"Buy Box - Lowest: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - Lowest 365 days":
            value = stats_365.get('min365', [-1] * 20)[10] if len(stats_365.get('min365', [-1] * 20)) > 10 and stats_365.get('min365', [-1] * 20)[10] is not None else -1
            if isinstance(value, list):
                value = min(value) if value else -1
            print(f"Buy Box - Lowest 365 days: value={value}")
            return f"${value / 100:.2f}" if value > 0 else 'DROP'
        elif header == "Buy Box - Highest":
            value = stats_365.get('max', [-1] * 20)[10] if len(stats_365.get('max', [-1] * 20)) > 10 and stats_365.get('max', [-1] * 20)[10] is not None else -1
            if isinstance(value, list):
                value = max(value) if value else -1
            print(f"Buy Box - Highest: value={value}")
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == "Buy Box - Highest 365 days":
            value = stats_365.get('max365', [-1] * 20)[10] if len(stats_365.get('max365', [-1] * 20)) > 10 and stats_365.get('max365', [-1] * 20)[10] is not None else -1
            if isinstance(value, list):
                value = max(value) if value else -1
            print(f"Buy Box - Highest 365 days: value={value}")
            return f"${value / 100:.2f}" if value > 0 else 'DROP'
        elif header == "Buy Box - 90 days OOS":
            value = stats_90.get('outOfStockPercentage90', [-1] * 20)[10] if len(stats_90.get('outOfStockPercentage90', [-1] * 20)) > 10 and stats_90.get('outOfStockPercentage90', [-1] * 20)[10] is not None else -1
            print(f"Buy Box - 90 days OOS: value={value}")
            return f"{value}%" if value >= 0 else '-'
        elif header == "Buy Box - Stock":
            value = data.get('buyBoxStock', '')
            print(f"Buy Box - Stock: value={value}")
            return str(value) if value else "DROP"

    # Generic field handling
    elif field.startswith('https'):
        return field.format(asin=data.get('asin', ''))
    elif field in ['dealAdded', 'lastUpdate', 'lastPriceChange']:
        ts = deal_data.get(field) if field == 'dealAdded' else data.get(field)
        dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts else None
        return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'
    elif field.startswith('(('):
        avg90 = stats_90.get('avg', [-1] * 20)
        avg365 = stats_365.get('avg', [-1] * 20)
        if 'avg90[2]' in field:
            value = ((avg90[2] - current[2]) / avg90[2] * 100) if len(avg90) > 2 and avg90[2] is not None and avg90[2] > 0 and len(current) > 2 and current[2] is not None else -1
            return f"{value:.0f}%" if value != -1 else '-'
        if 'avg365[2]' in field:
            value = ((avg365[2] - current[2]) / avg365[2] * 100) if len(avg365) > 2 and avg365[2] is not None and avg365[2] > 0 and len(current) > 2 and current[2] is not None else -1
            return f"{value:.0f}%" if value != -1 else '-'
    elif '[' in field:
        key, idx_part = field.split('[')
        idx_str = idx_part.rstrip(']')
        if ':' in idx_str:  # Handle slice
            if key == 'categories' and idx_str == '1:':
                category_tree = data.get('categoryTree', [])
                return ', '.join(cat['name'] for cat in category_tree[1:]) if len(category_tree) > 1 else '-'
            print(f"{header}: unsupported slice {idx_str}, defaulting to '-'")
            return '-'
        idx = int(idx_str)  # Numeric index
        if 'outOfStockPercentage' in key:
            value = stats_365.get(key, {}).get(idx, -1)
        elif key == 'current':
            value = current[idx] if len(current) > idx and current[idx] is not None else -1
        else:
            value = stats_90.get(key, [-1] * 20)[idx] if '90' in key and len(stats_90.get(key, [-1] * 20)) > idx and stats_90.get(key, [-1] * 20)[idx] is not None else stats_365.get(key, [-1] * 20)[idx] if len(stats_365.get(key, [-1] * 20)) > idx and stats_365.get(key, [-1] * 20)[idx] is not None else -1
        if value is None or value == -1:
            print(f"{header}: value={value}, defaulting to '-'")
            return '-'
        if key in ['avg', 'avg90', 'avg365', 'current'] and idx in [2, 3, 10, 11, 12, 13, 14, 15, 17]:
            return f"${value / 100:.2f}" if value > 0 else '-'
        return str(value) if value > 0 else '-'

    value = data.get(field, '')
    if value in [None, '', 'None', -1, 0] and header not in ['Package - Quantity']:
        if header in ['Variation Attributes', 'Buy Box - Stock', 'Amazon - Stock', 'New - Stock', 
                      'New, 3rd Party FBA - Stock', 'New, 3rd Party FBM - Stock', 'Buy Box Used - Stock', 
                      'Used - Stock', 'Used, like new - Stock', 'Used, very good - Stock', 
                      'Used, good - Stock', 'Used, acceptable - Stock', 'List Price - Stock']:
            print(f"{header}: sparse field, marking DROP")
            return "DROP"
        print(f"{header}: value={value}, defaulting to '-'")
        return '-'
    print(f"Direct field {field}: value={value}")
    return str(value)
# Chunk 5 ends

# Chunk 6 starts
def extract_fields(deal, product_90, product_365):
    row = [get_field(product_365, deal, product_90, h, FIELD_MAPPING.get(h, h)) for h in HEADERS]
    print(f"Extracted ASIN {deal.get('asin')}: {row}")
    return row
# Chunk 6 ends

# Chunk 7 starts
def write_csv(data, filename='/home/timscripts/keepa_api/keepa_full_deals_v6.csv'):
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
        max_deals = min(10, len(deals))  # Up from 5
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
        write_csv(rows, filename='/home/timscripts/keepa_api/keepa_full_deals_v6.csv')
    else:
        print("No deals fetched!")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
# Chunk 8 ends