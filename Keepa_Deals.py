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
# Dev Log: Enhanced fetch_product with retries, stats validation, raw response logging (2025-04-28).
import time
import json

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
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 429:
                logging.warning(f"Rate limit hit for ASIN {asin} - sleeping for 5s")
                time.sleep(5)
                continue
            response_json = response.json()
            logging.debug(f"Raw API response for ASIN {asin}: {json.dumps(response_json, default=str)}")
            tokens_left = response_json.get('tokensLeft', -1)
            if tokens_left < 100:
                sleep_time = max(5, (100 - tokens_left) // 5)
                logging.warning(f"Low tokens ({tokens_left}) for ASIN {asin} - sleeping for {sleep_time}s")
                time.sleep(sleep_time)
            product = response_json.get('products', [])[0] if response_json.get('products') else {}
            if not product or 'stats' not in product or len(product.get('stats', {}).get('current', [])) < 16:
                logging.error(f"Incomplete data for ASIN {asin}, attempt {attempt+1}: {product.get('stats', {})}")
                time.sleep(2)
                continue
            offers = product.get('offers', [])
            buy_box_used = -1
            for offer in offers:
                is_bb = offer.get('isBuyBox', False)
                cond = offer.get('condition', -1)
                price = offer.get('price', -1)
                if is_bb and cond in [2, 3, 4, 5]:
                    buy_box_used = price
                    break
            product['stats']['current'][11] = buy_box_used
            logging.debug(f"fetch_product - ASIN={asin}, stats_period={stats_period}, buy_box_used={buy_box_used}")
            return product
        except requests.Timeout:
            logging.error(f"fetch_product timeout after 30s - ASIN={asin}, stats_period={stats_period}")
            time.sleep(2)
        except requests.RequestException as e:
            logging.error(f"fetch_product failed for ASIN {asin}, attempt {attempt+1}: {str(e)}")
            time.sleep(2)
    logging.error(f"Failed to fetch product for ASIN {asin} after {retries} attempts")
    return {'stats': {'current': [-1] * 30}}
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
# Dev Log: Simplified get_field stats handling, added None safety (2025-04-28).
def get_field(data, deal_data, product_90, header, field):
    logging.debug(f"Header={header}, Field={field}")
    stats_365 = data.get('stats', {}) if data else {}
    current = stats_365.get('current', [-1] * 30)
    if current[3] <= 0:
        logging.debug(f"ASIN {data.get('asin', '-')}: stats_365.current[3] invalid: {current[3]}")
    from stable_fields import get_stable_price_field

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

    # Handle stats fields
    if field.startswith('stats.'):
        try:
            if '[' in field:
                key, idx_part = field.split('[')
                idx = int(idx_part.rstrip(']'))
                parts = key.split('.')
                value = stats_365
                for part in parts[1:]:
                    value = value.get(part, [-1] * 30)
                if not isinstance(value, list) or idx >= len(value):
                    logging.error(f"Invalid stats for {header}: {value}, ASIN: {data.get('asin', '-')}")
                    return '-'
                value = value[idx]
            else:
                key = field.split('.')[1]
                if 'salesRankDrops' in key:
                    drop_period = key.split('salesRankDrops')[1]
                    value = product_90.get(f'salesRankDrops{drop_period}', -1) if drop_period in ['30', '60', '90'] else stats_365.get(f'salesRankDrops{drop_period}', -1)
                else:
                    value = stats_365.get(key, -1)
            logging.debug(f"{header} - stats field={field}, value={value}")
            if header.startswith("Reviews - "):
                return str(value) if value > 0 else 'DROP' if '60 days' in header else '-'
            elif header.startswith("Sales Rank - "):
                if 'Drops' in header:
                    return str(value) if value >= 0 else 'DROP' if '60 days' in header else '-'
                return f"{value:,}" if value > 0 else 'DROP' if '60 days' in header else '-'
            elif header.startswith(("Buy Box - ", "Buy Box Used - ", "Used, ", "New - ", "New, 3rd Party")):
                if 'OOS' in header:
                    return f"{value}%" if value >= 0 else '-'
                return f"${value / 100:.2f}" if value > 0 else 'DROP' if '60 days' in header else '-'
            elif header.startswith(("New Offer Count", "Used Offer Count")):
                return f"{value:,}" if value > 0 else '-'
            elif 'OOS' in header:
                return f"{value}%" if value >= 0 else '-'
            return str(value) if value >= 0 else '-'
        except Exception as e:
            logging.error(f"Stats error for {header}: {str(e)}, ASIN: {data.get('asin', '-')}")
            return '-'

    # Sparse fields
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
            "Current": ("current", stats_365), "30 days avg.": ("avg30", stats_365),
            "60 days avg.": ("avg", stats_365), "90 days avg.": ("avg90", stats_365),
            "180 days avg.": ("avg180", stats_365), "365 days avg.": ("avg365", stats_365),
            "Lowest": ("min", stats_365), "Lowest 365 days": ("min365", stats_365),
            "Highest": ("max", stats_365), "Highest 365 days": ("max365", stats_365),
            "90 days OOS": ("outOfStockPercentage90", stats_365)
        }
        period_key, stats_source = periods.get(header.replace("Amazon - ", ""), (None, None))
        if period_key and stats_source:
            value = stats_source.get(period_key, [-1] * 30)
            if isinstance(value, list) and len(value) > 4:
                value = value[4]
            elif period_key in ["min", "max", "min365", "max365"] and isinstance(value, list):
                value = min(value) if "min" in period_key else max(value) if value else -1
            else:
                value = -1
            logging.debug(f"{header} - value={value}")
            if period_key == "outOfStockPercentage90":
                return f"{value}%" if value >= 0 else '-'
            return f"${value / 100:.2f}" if value > 0 else 'DROP' if "60 days" in header or "365 days" in header else '-'

    # Condition prices
    if header in ["Used - Current", "Buy Box Used - Current", "Used, like new - Current", 
                  "Used, very good - Current", "Used, good - Current", "Used, acceptable - Current"]:
        index = {
            'Used - Current': 3, 'Buy Box Used - Current': 11, 'Used, like new - Current': 12,
            'Used, very good - Current': 13, 'Used, good - Current': 14, 'Used, acceptable - Current': 15
        }[header]
        value = stats_365.get('current', [-1] * 30)
        value = value[index] if len(value) > index else -1
        return f"${value / 100:.2f}" if value > 0 else '-'

    # Price fields
    if header == "Price Now":
        price = stats_365.get('current', [-1] * 30)
        price = price[3] if len(price) > 3 else -1
        logging.debug(f"Price Now - stats_365.current[3]={price}")
        return f"${price / 100:.2f}" if price > 0 else '-'
    if header == "Avg. Price 90":
        avg = stats_365.get('avg90', [-1] * 30)
        avg = avg[3] if len(avg) > 3 else -1
        logging.debug(f"Avg. Price 90 - stats_365.avg90[3]={avg}")
        return f"${avg / 100:.2f}" if avg > 0 else '-'
    if header == "Avg. Price 365":
        avg = stats_365.get('avg365', [-1] * 30)
        avg = avg[3] if len(avg) > 3 else -1
        logging.debug(f"Avg. Price 365 - stats_365.avg365[3]={avg}")
        return f"${avg / 100:.2f}" if avg > 0 else '-'
    if header == "Percent Down 90":
        current = stats_365.get('current', [-1] * 30)
        current = current[3] if len(current) > 3 else -1
        avg = stats_365.get('avg90', [-1] * 30)
        avg = avg[3] if len(avg) > 3 else -1
        logging.debug(f"Percent Down 90 - current={current}, avg90={avg}")
        return f"{((avg - current) / avg * 100):.0f}%" if current > 0 and avg > 0 else '-'
    if header == "Percent Down 365":
        current = stats_365.get('current', [-1] * 30)
        current = current[3] if len(current) > 3 else -1
        avg = stats_365.get('avg365', [-1] * 30)
        avg = avg[3] if len(avg) > 3 else -1
        logging.debug(f"Percent Down 365 - current={current}, avg365={avg}")
        return f"{((avg - current) / avg * 100):.0f}%" if current > 0 and avg > 0 else '-'
    if header in ["Buy Box - Lowest", "Buy Box - Highest"]:
        stats = stats_365.get('min' if "Lowest" in header else 'max', [-1] * 30)
        price = stats[10] if isinstance(stats, list) and len(stats) > 10 else -1
        return f"${price / 100:.2f}" if price > 0 else '-'

    # Used condition averages
    if header in ["Used, like new - 30 days avg.", "Used, like new - 60 days avg.", "Used, like new - 90 days avg.", 
                  "Used, like new - 180 days avg.", "Used, like new - 365 days avg."]:
        period = {"30 days": "avg30", "60 days": "avg", "90 days": "avg90", "180 days": "avg180", "365 days": "avg365"}[header.split(" - ")[1].replace(" avg.", "")]
        avg = stats_365.get(period, [-1] * 30)
        avg = avg[12] if len(avg) > 12 else -1
        logging.debug(f"{header} - stats_365.{period}[12]={avg}")
        return f"${avg / 100:.2f}" if avg > 0 else '-'

    # Sales rank drops
    if header.startswith("Sales Rank - Drops last "):
        drop_period = header.replace("Sales Rank - Drops last ", "").replace(" days", "")
        value = stats_365.get(f'salesRankDrops{drop_period}', -1)
        logging.debug(f"{header} - salesRankDrops{drop_period} value={value}")
        return str(value) if value >= 0 else '-'

    # Existing Deal found, last update, etc.
    if header == "Deal found":
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
        ts = deal_data.get('currentSince', [-1] * 20)
        ts = ts[11] if len(ts) > 11 else -1
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
# Dev Log: Enhanced get_field for None/empty stats, added stats logging (2025-04-28).
def get_field(product, deal, stats, header, field):
    try:
        if field.startswith('stats.'):
            if stats is None:
                logging.error(f"Stats is None for {header}, ASIN: {deal.get('asin', '-')}")
                return '-'
            parts = field.split('.')
            data = stats
            for part in parts[1:]:
                if '[' in part:
                    key, idx = part.split('[')
                    idx = int(idx.rstrip(']'))
                    data = data.get(key, []) if isinstance(data, dict) else data
                    if not isinstance(data, list) or len(data) <= idx:
                        logging.error(f"Invalid stats for {header}: {data}, ASIN: {deal.get('asin', '-')}")
                        return '-'
                    value = data[idx]
                else:
                    data = data.get(part, {}) if isinstance(data, dict) else data
            if isinstance(value, list) and len(value) > 1:
                value = value[1]  # Handle min/max lists
            if ('Price' in header or 'Used' in header or 'Amazon' in header or 'Buy Box' in header) and isinstance(value, (int, float)) and value > 0:
                return f"${value / 100:.2f}"
            return value
        elif field.startswith('https://'):
            return field.format(asin=deal.get('asin', '-'))
        elif field.startswith('calculate_'):
            return '-'
        else:
            return product.get(field, deal.get(field, '-'))
    except Exception as e:
        logging.error(f"get_field failed for {header}: {str(e)}, ASIN: {deal.get('asin', '-')}")
        return '-'
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
# Dev Log: Switched to keepa-based fetch_product, enhanced retries, logging (2025-04-29).
import sys
import keepa
from retrying import retry
import json

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_product(asin, days, api):
    logging.debug(f"Fetching ASIN {asin} for {days} days...")
    try:
        products = api.query(asin, stats=days, offers=20, rating=1, update=0)
        if not products or not isinstance(products, list) or len(products) == 0:
            logging.error(f"No product data for ASIN {asin}")
            return {'stats': {'current': [-1] * 30}}
        product = products[0]
        stats = product.get('stats', {})
        logging.debug(f"Raw stats for ASIN {asin}: {json.dumps(stats, default=str)}")
        if not stats or len(stats.get('current', [])) < 16:
            logging.error(f"Incomplete stats for ASIN {asin}: {stats}")
            return {'stats': {'current': [-1] * 30}}
        return product
    except Exception as e:
        logging.error(f"Fetch failed for ASIN {asin}: {str(e)}")
        return {'stats': {'current': [-1] * 30}}

def main():
    logging.info("Starting Process500_Deals_v8...")
    print("Initializing script...")
    sys.stdout.flush()
    try:
        api = keepa.Keepa(api_key)
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
            products_365 = {}
            for i, deal in enumerate(deals[:max_deals]):
                asin = deal['asin']
                logging.info(f"Fetching ASIN {asin} ({i+1}/{max_deals})...")
                print(f"Fetching ASIN {asin} ({i+1}/{max_deals})...")
                sys.stdout.flush()
                products_365[asin] = fetch_product(asin, 365, api)
            logging.info("Processing deals...")
            print("Processing deals...")
            sys.stdout.flush()
            rows = []
            for i, deal in enumerate(deals[:max_deals]):
                asin = deal['asin']
                logging.info(f"Processing ASIN {asin} ({i+1}/{max_deals})...")
                print(f"Processing ASIN {asin} ({i+1}/{max_deals})...")
                sys.stdout.flush()
                product = products_365.get(asin, {})
                row = []
                for header in HEADERS:
                    value = get_field(product, deal, product.get('stats', {}), header, FIELD_MAPPING.get('basic', {}).get(header, header))
                    row.append(value if value != 'DROP' else '-')
                    logging.debug(f"ASIN {asin}, Header={header}, Value={value}")
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