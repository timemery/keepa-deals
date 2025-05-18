# stable_products.py
# Unchanged imports and globals
import requests
import logging
from retrying import retry
from datetime import datetime, timedelta
from pytz import timezone
from stable_deals import validate_asin
import json
from keepa import Keepa

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_product_for_retry(asin):
    with open('config.json') as f:
        config = json.load(f)
    api = Keepa(config['api_key'])
    product = api.query(asin, product_code_is_asin=True, stats=90, domain='US', history=False)
    return product[0] if product else {}

# Constants
KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')

# Shared globals
API_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}

# Global stuff starts
def get_stat_value(stats, key, index, divisor=1, is_price=False):
    try:
        value = stats.get(key, [])
        logging.debug(f"get_stat_value: key={key}, index={index}, stats[{key}]={value}")
        if not value or len(value) <= index:
            logging.warning(f"get_stat_value: No data for key={key}, index={index}, returning '-'")
            return '-'
        value = value[index]
        logging.debug(f"get_stat_value: key={key}, index={index}, value={value}")
        if isinstance(value, list):
            value = value[1] if len(value) > 1 else -1
        if value == -1 or value is None:
            return '-'
        if is_price:
            return f"${value / divisor:.2f}"
        return f"{int(value / divisor):,}"
    except (IndexError, TypeError, AttributeError) as e:
        logging.error(f"get_stat_value failed: stats={stats}, key={key}, index={index}, error={str(e)}")
        return '-'
# Global stuff ends

# ASIN starts
def get_asin(product):
    asin = product.get('asin', '-')
    result = {'ASIN': f'="{asin}"' if asin != '-' else '-'}
    logging.debug(f"get_asin result for ASIN {asin}: {result}")
    return result
# ASIN ends

# Title starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_title(asin, api_key):
    if not validate_asin(asin):
        return {'Title': '-'}
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"get_title response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"get_title request failed for ASIN {asin}: {response.status_code}")
            return {'Title': '-'}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"get_title no product data for ASIN {asin}")
            return {'Title': '-'}
        title = products[0].get('title', '-')
        logging.debug(f"get_title result for ASIN {asin}: {title[:50]}")
        return {'Title': title}
    except Exception as e:
        logging.error(f"get_title fetch failed for ASIN {asin}: {str(e)}")
        return {'Title': '-'}
# Title ends

# Sales Rank - Current starts
def sales_rank_current(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - Current': get_stat_value(stats, 'current', 3, is_price=False)}
    return result
# Sales Rank - Current ends

# Used - Current starts
def used_current(product):
    stats = product.get('stats', {})
    result = {'Used - Current': get_stat_value(stats, 'current', 2, divisor=100, is_price=True)}
    return result
# Used - Current ends

# Sales Rank - 30 days avg starts
def sales_rank_30_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 30 days avg.': get_stat_value(stats, 'avg30', 3, is_price=False)}
    return result
# Sales Rank - 30 days avg ends

# Sales Rank - 90 days avg starts
def sales_rank_90_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 90 days avg.': get_stat_value(stats, 'avg90', 3, is_price=False)}
    logging.debug(f"Sales Rank - 90 days avg. for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Sales Rank - 90 days avg ends

# Sales Rank - 180 days avg starts
def sales_rank_180_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 180 days avg.': get_stat_value(stats, 'avg180', 3, is_price=False)}
    return result
# Sales Rank - 180 days avg ends

# Sales Rank - 365 days avg starts
def sales_rank_365_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 365 days avg.': get_stat_value(stats, 'avg365', 3, is_price=False)}
    return result
# Sales Rank - 365 days avg ends

# Package - Quantity starts
#@retry(stop_max_attempt_number=3, wait_fixed=5000)
#def package_quantity(asin, api_key):
#    if not validate_asin(asin):
#        return {'Package - Quantity': '-'}
#    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
#    try:
#        response = requests.get(url, headers=API_HEADERS, timeout=30)
#        logging.debug(f"package_quantity response status for ASIN {asin}: {response.status_code}")
#        if response.status_code != 200:
#            logging.error(f"package_quantity request failed for ASIN {asin}: {response.status_code}")
#            return {'Package - Quantity': '-'}
#        data = response.json()
#        products = data.get('products', [])
#        if not products:
#            logging.error(f"package_quantity no product data for ASIN {asin}")
#            return {'Package - Quantity': '-'}
#        quantity = products[0].get('packageQuantity', -1)
#        logging.debug(f"package_quantity result for ASIN {asin}: {quantity}")
#        return {'Package - Quantity': str(quantity) if quantity != -1 else '-'}
#    except Exception as e:
#        logging.error(f"package_quantity fetch failed for ASIN {asin}: {str(e)}")
#        return {'Package - Quantity': '-'}
# Package - Quantity ends

# Package Weight starts
def package_weight(product):
    weight = product.get('packageWeight', -1)
    result = {'Package Weight': f"{weight / 1000:.2f} kg" if weight != -1 else '-'}
    return result
# Package Weight ends

# Package Height starts
def package_height(product):
    height = product.get('packageHeight', -1)
    result = {'Package Height': f"{height / 10:.1f} cm" if height != -1 else '-'}
    return result
# Package Height ends

# Package Length starts
def package_length(product):
    length = product.get('packageLength', -1)
    result = {'Package Length': f"{length / 10:.1f} cm" if length != -1 else '-'}
    return result
# Package Length ends

# Package Width starts
def package_width(product):
    width = product.get('packageWidth', -1)
    result = {'Package Width': f"{width / 10:.1f} cm" if width != -1 else '-'}
    return result
# Package Width ends

# Used, like new - Current starts
def used_like_new(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    current_price = get_stat_value(stats, 'current', 4, divisor=100, is_price=True)
    result = {'Used, like new - Current': current_price}
    logging.debug(f"used_like_new for ASIN {asin}: stats.current={stats.get('current', [])}, current_price={current_price}")
    return result
# Used, like new - Current ends

# Used, very good - Current starts
def used_very_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'Used, very good - Current': get_stat_value(stats, 'current', 5, divisor=100, is_price=True)
    }
    logging.debug(f"used_very_good result for ASIN {asin}: {result}")
    return result
# Used, very good - Current ends

# Used, good - Current starts
def used_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'Used, good - Current': get_stat_value(stats, 'current', 6, divisor=100, is_price=True)
    }
    logging.debug(f"used_good result for ASIN {asin}: {result}")
    return result
# Used, good - Current ends

# Used, acceptable - Current starts
def used_acceptable(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'Used, acceptable - Current': get_stat_value(stats, 'current', 7, divisor=100, is_price=True)
    }
    logging.debug(f"used_acceptable result for ASIN {asin}: {result}")
    return result
# Used, acceptable - Current ends

# New, 3rd Party FBM - Current starts
def new_3rd_party_fbm_current(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    offers = product.get('offers', [])
    current_price = get_stat_value(stats, 'current', 1, divisor=100, is_price=True)
    fbm_prices = [o.get('price', -1) / 100 for o in offers if o.get('condition') == 'New' and not o.get('isFBA', False)]
    if fbm_prices and current_price != '-' and not any(abs(float(current_price[1:]) - p) < 0.01 for p in fbm_prices):
        logging.warning(f"FBM price mismatch for ASIN {asin}: stats={current_price}, offers={fbm_prices}")
        current_price = '-'
    result = {
        'New, 3rd Party FBM - Current': current_price
    }
    logging.debug(f"new_3rd_party_fbm_current result for ASIN {asin}: {result}")
    return result
# New, 3rd Party FBM - Current ends

# New, 3rd Party FBM starts
def new_3rd_party_fbm(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'New' and not o.get('isFBA', False) and o.get('stock', 0) > 0)
    result = {
        'New, 3rd Party FBM - 30 days avg.': get_stat_value(stats, 'avg30', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 60 days avg.': get_stat_value(stats, 'avg60', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 90 days avg.': get_stat_value(stats, 'avg90', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 180 days avg.': get_stat_value(stats, 'avg180', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 365 days avg.': get_stat_value(stats, 'avg365', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"new_3rd_party_fbm result for ASIN {asin}: {result}")
    return result
# New, 3rd Party FBM ends

# List Price starts
def list_price(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'List Price - Current': get_stat_value(stats, 'current', 8, divisor=100, is_price=True)
    }
    logging.debug(f"list_price result for ASIN {asin}: {result}")
    return result
# List Price ends

# Percent Down 90 starts
def percent_down_90(product):
    logging.debug(f"percent_down_90 input: {product.get('asin', '-')}")
    stats_90 = product.get('stats', {})
    avg = stats_90.get('avg90', [-1] * 20)[2]  # Used price
    curr = stats_90.get('current', [-1] * 20)[2]  # Used price
    if avg <= 0 or curr < 0 or avg is None or curr is None:
        logging.error(f"No valid avg90 or current for ASIN {product.get('asin', '-')}: avg={avg}, curr={curr}")
        return {'Percent Down 90': '-'}
    try:
        value = ((avg - curr) / avg * 100)
        percent = f"{value:.0f}%"
        logging.debug(f"percent_down_90 result: {percent}")
        return {'Percent Down 90': percent}
    except Exception as e:
        logging.error(f"percent_down_90 failed: {str(e)}")
        return {'Percent Down 90': '-'}
# Percent Down 90 ends

# AMZ link starts
def amz_link(product):
    asin = product.get('asin', '-')
    result = {'AMZ link': f"https://www.amazon.com/dp/{asin}" if asin != '-' else '-'}
    logging.debug(f"amz_link result for ASIN {asin}: {result}")
    return result
# AMZ link ends

# Keepa Link starts
def keepa_link(product):
    asin = product.get('asin', '-')
    result = {'Keepa Link': f"https://keepa.com/#!product/1-{asin}" if asin != '-' else '-'}
    logging.debug(f"keepa_link result for ASIN {asin}: {result}")
    return result
# Keepa Link ends

# Categories - Root starts
def categories_root(product):
    category_tree = product.get('categoryTree', [])
    result = {'Categories - Root': category_tree[0]['name'] if category_tree else '-'}
    logging.debug(f"categories_root result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Categories - Root ends

# Categories - Sub starts
def categories_sub(product):
    category_tree = product.get('categoryTree', [])
    result = {'Categories - Sub': ', '.join(cat['name'] for cat in category_tree[2:]) if len(category_tree) > 2 else '-'}
    logging.debug(f"categories_sub result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Categories - Sub ends

# Categories - Tree starts
def categories_tree(product):
    category_tree = product.get('categoryTree', [])
    result = {'Categories - Tree': ' > '.join(cat['name'] for cat in category_tree) if category_tree else '-'}
    logging.debug(f"categories_tree result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Categories - Tree ends

# Tracking since starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def tracking_since(product):
    ts = product.get('trackingSince', 0)
    logging.debug(f"Tracking since - raw ts={ts}")
    if ts <= 100000:
        logging.error(f"No valid trackingSince for ASIN {product.get('asin', 'unknown')}")
        return {'Tracking since': '-'}
    try:
        dt = KEEPA_EPOCH + timedelta(minutes=ts)
        formatted = TORONTO_TZ.localize(dt).strftime('%Y-%m-%d')
        logging.debug(f"Tracking since result for ASIN {product.get('asin', 'unknown')}: {formatted}")
        return {'Tracking since': formatted}
    except Exception as e:
        logging.error(f"tracking_since failed: {str(e)}")
        return {'Tracking since': '-'}
# Tracking since ends

# Manufacturer starts
def manufacturer(product):
    manufacturer_value = product.get('manufacturer', '-')
    result = {'Manufacturer': manufacturer_value}
    logging.debug(f"manufacturer result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Manufacturer ends

# Author starts
def author(product):
    author_value = product.get('author', '-')
    result = {'Author': author_value}
    logging.debug(f"author result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Author ends

# Binding starts
def binding(product):
    binding_value = product.get('binding', '-')
    result = {'Binding': binding_value}
    logging.debug(f"binding result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Binding ends

# Listed since starts
def listed_since(product):
    ts = product.get('listedSince', 0)
    asin = product.get('asin', 'unknown')
    logging.debug(f"Listed since - raw ts={ts} for ASIN {asin}")
    if ts <= 0:
        logging.info(f"No valid listedSince (ts={ts}) for ASIN {asin}")
        return {'Listed since': '-'}
    try:
        dt = KEEPA_EPOCH + timedelta(minutes=ts)
        formatted = TORONTO_TZ.localize(dt).strftime('%Y-%m-%d')
        logging.debug(f"Listed since result for ASIN {asin}: {formatted}")
        return {'Listed since': formatted}
    except Exception as e:
        logging.error(f"listed_since failed for ASIN {asin}: {str(e)}")
        return {'Listed since': '-'}
# Listed since ends

# Sales Rank - Drops last 365 days starts
def sales_rank_drops_last_365_days(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    value = stats.get('salesRankDrops365', -1)
    logging.debug(f"Sales Rank - Drops last 365 days - raw value={value} for ASIN {asin}")
    if value < 0:
        logging.info(f"No valid Sales Rank - Drops last 365 days (value={value}) for ASIN {asin}")
        return {'Sales Rank - Drops last 365 days': '-'}
    try:
        formatted = str(value)
        logging.debug(f"Sales Rank - Drops last 365 days result for ASIN {asin}: {formatted}")
        return {'Sales Rank - Drops last 365 days': formatted}
    except Exception as e:
        logging.error(f"sales_rank_drops_last_365_days failed for ASIN {asin}: {str(e)}")
        return {'Sales Rank - Drops last 365 days': '-'}
# Sales Rank - Drops last 365 days ends

# Sales Rank - Drops last 30 days starts
def sales_rank_drops_last_30_days(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    value = stats.get('salesRankDrops30', -1)
    logging.debug(f"Sales Rank - Drops last 30 days - raw value={value} for ASIN {asin}")
    if value < 0:
        logging.info(f"No valid Sales Rank - Drops last 30 days (value={value}) for ASIN {asin}")
        return {'Sales Rank - Drops last 30 days': '-'}
    try:
        formatted = str(value)
        logging.debug(f"Sales Rank - Drops last 30 days result for ASIN {asin}: {formatted}")
        return {'Sales Rank - Drops last 30 days': formatted}
    except Exception as e:
        logging.error(f"sales_rank_drops_last_30_days failed for ASIN {asin}: {str(e)}")
        return {'Sales Rank - Drops last 30 days': '-'}
# Sales Rank - Drops last 30 days ends

# Buy Box - Current starts
def buy_box_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    logging.debug(f"Buy Box - Current - product_keys={list(product.keys())}, stats={stats} for ASIN {asin}")
    current = stats.get('current', [-1] * 20)
    value = current[0] if len(current) > 0 else -1
    logging.debug(f"Buy Box - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
    if value <= 0:
        logging.info(f"No valid Buy Box - Current (value={value}) for ASIN {asin}")
        return {'Buy Box - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"Buy Box - Current result for ASIN {asin}: {formatted}")
        return {'Buy Box - Current': formatted}
    except Exception as e:
        logging.error(f"buy_box_current failed for ASIN {asin}: {str(e)}")
        return {'Buy Box - Current': '-'}
# Buy Box - Current ends

# Amazon - Current starts
def amazon_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current = stats.get('current', [-1] * 20)
    value = current[10]
    logging.debug(f"Amazon - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
    if value <= 0:
        logging.info(f"No valid Amazon - Current (value={value}) for ASIN {asin}")
        return '-'
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"Amazon - Current result for ASIN {asin}: {formatted}")
        return formatted
    except Exception as e:
        logging.error(f"amazon_current failed for ASIN {asin}: {str(e)}")
        return '-'
# Amazon - Current ends

# Price Now starts - this produces correct data for Sales Rank - Current NOT Price Now
#def price_now(product):
#    stats = product.get('stats', {})
#    result = {'Price Now': get_stat_value(stats, 'current', 3, divisor=100, is_price=True)}
#    logging.debug(f"price_now result for ASIN {product.get('asin', 'unknown')}: {result}")
#    return result
# Price Now ends

# Price Now - Reference - though this one looks wrong due to "product_90.get" 
#    elif header == "Price Now":
#        value = stats_90.get('current', [-1] * 20)[2]  # Used
#        logging.debug(f"Price Now (ASIN {product_90.get('asin')}): used={value}")
#        if value is not None and value >= 2000 and value <= 30100:  # $20-$301
#            return f"${value / 100:.2f}"
#        return '-'