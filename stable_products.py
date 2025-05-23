# stable_products.py
# Unchanged imports and globals
import requests
import logging
from retrying import retry
from datetime import datetime, timedelta
from pytz import timezone
from stable_deals import validate_asin
import json
# Removed unused import: from keepa import Keepa

# Fetch Product for Retry - starts
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_product_for_retry(asin):
    with open('config.json') as f:
        config = json.load(f)
    api = Keepa(config['api_key'])
    product = api.query(asin, product_code_is_asin=True, stats=90, domain='US', history=True, offers=20)
    if not product or not product[0]:
        logging.error(f"fetch_product_for_retry failed: no product data for ASIN {asin}")
        return {}
    stats = product[0].get('stats', {})
    stats_current = stats.get('current', [-1] * 20)
    offers = product.get('offers', []) if product.get('offers') is not None else []
    logging.debug(f"fetch_product_for_retry response for ASIN {asin}: stats_keys={list(stats.keys())}, stats_current={stats_current}, stats_raw={stats}, offers_count={len(offers)}")
    return product[0]
# Fetch Product for Retry - ends

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

# Avg. Price 90,
# Percent Down 365,
# Avg. Price 365,

# Price Now starts - This produces correct data for Sales Rank - Current -- NOT Price Now
#def price_now(product):
#    stats = product.get('stats', {})
#    result = {'Price Now': get_stat_value(stats, 'current', 3, divisor=100, is_price=True)}
#    logging.debug(f"price_now result for ASIN {product.get('asin', 'unknown')}: {result}")
#    return result
# Price Now ends

# Price Now Source,
# Deal found (stable_deals) 

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

# Title starts
def get_title(product):
    title = product.get('title', '-')
    asin = product.get('asin', 'unknown')
    if title == '-':
        logging.warning(f"get_title: No title found for ASIN {asin}")
    logging.debug(f"get_title result for ASIN {asin}: {title[:50]}")
    return {'Title': title}
# Title ends

# last update (stable_deals) 
# last price change (stable_deals)
# Sales Rank - Reference
# Reviews - Rating
# Reviews - Review Count
# FBA Pick&Pack Fee
# Referral Fee %

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

# ASIN starts
def get_asin(product):
    asin = product.get('asin', '-')
    result = {'ASIN': f'="{asin}"' if asin != '-' else '-'}
    logging.debug(f"get_asin result for ASIN {asin}: {result}")
    return result
# ASIN ends

# Freq. Bought Together
# Type

# Manufacturer starts
def manufacturer(product):
    manufacturer_value = product.get('manufacturer', '-')
    result = {'Manufacturer': manufacturer_value}
    logging.debug(f"manufacturer result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Manufacturer ends

# Brand
# Product Group
# Variation Attributes
# Item Type

# Author starts
def author(product):
    author_value = product.get('author', '-')
    result = {'Author': author_value}
    logging.debug(f"author result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Author ends

# Contributors

# Binding starts
def binding(product):
    binding_value = product.get('binding', '-')
    result = {'Binding': binding_value}
    logging.debug(f"binding result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Binding ends

# Number of Items
# Number of Pages
# Publication Date
# Languages

# Package - Quantity starts
# This one doesn't work - but we're keeping it as a reminder:
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

# Edition
# Release Date
# Format

# Sales Rank - Current starts
def sales_rank_current(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - Current': get_stat_value(stats, 'current', 3, is_price=False)}
    return result
# Sales Rank - Current ends

# Sales Rank - 30 days avg starts
def sales_rank_30_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 30 days avg.': get_stat_value(stats, 'avg30', 3, is_price=False)}
    return result
# Sales Rank - 30 days avg ends

# Sales Rank - 60 days avg.

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

# Sales Rank - Lowest
# Sales Rank - Lowest 365 days
# Sales Rank - Highest
# Sales Rank - Highest 365 days

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

# Sales Rank - Drops last 60 days
# Sales Rank - Drops last 90 days
# Sales Rank - Drops last 180 days

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

# Buy Box - Current starts - stopped working after a change to new 3rd party fbm current
# Buy Box - Current starts
def buy_box_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current = stats.get('current', [-1] * 20)
    value = current[0] if len(current) > 0 else -1
    logging.debug(f"Buy Box - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
    if value <= 0 or value == -1:
        logging.warning(f"No valid Buy Box - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
        return {'Buy Box - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"Buy Box - Current result for ASIN {asin}: {formatted}")
        return {'Buy Box - Current': formatted}
    except Exception as e:
        logging.error(f"buy_box_current failed for ASIN {asin}: {str(e)}")
        return {'Buy Box - Current': '-'}
# Buy Box - Current ends

# Buy Box - 30 days avg.
# Buy Box - 60 days avg.
# Buy Box - 90 days avg.
# Buy Box - 180 days avg.
# Buy Box - 365 days avg.
# Buy Box - Lowest
# Buy Box - Lowest 365 days
# Buy Box - Highest
# Buy Box - Highest 365 days
# Buy Box - 90 days OOS
# Buy Box - Stock

# This one doesn't work - but we're keeping it as a reminder:
# 2025-05-20: Removed &buyBox=1 from fetch_product URL (commit 95aac66e) to fix Amazon - Current, but stats.current[10] still -1 for ASIN 150137012X despite $6.26 offer. Reverted to commit 31cb7bee setup. Pivoted to New - Current. 
# Amazon - Current starts
# def amazon_current(product):
#    asin = product.get('asin', 'unknown')
#    stats = product.get('stats', {})
#    current = stats.get('current', [-1] * 20)
#    value = current[10] if len(current) > 10 else -1
#    logging.debug(f"Amazon - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
#    if value <= 0 or value == -1:
#        logging.warning(f"No valid Amazon - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
#        return {'Amazon - Current': '-'}
#    try:
#        formatted = f"${value / 100:.2f}"
#        logging.debug(f"Amazon - Current result for ASIN {asin}: {formatted}")
#        return {'Amazon - Current': formatted}
#    except Exception as e:
#        logging.error(f"amazon_current failed for ASIN {asin}: {str(e)}")
#        return {'Amazon - Current': '-'}
# Amazon - Current ends

# Amazon - 30 days avg.
# Amazon - 60 days avg.
# Amazon - 90 days avg.
# Amazon - 180 days avg.
# Amazon - 365 days avg.
# Amazon - Lowest
# Amazon - Lowest 365 days
# Amazon - Highest
# Amazon - Highest 365 days
# Amazon - 90 days OOS
# Amazon - Stock

# New - Current starts
def new_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current = stats.get('current', [-1] * 20)
    value = current[1] if len(current) > 1 else -1
    logging.debug(f"New - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
    if value <= 0 or value == -1:
        logging.warning(f"No valid New - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
        return {'New - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"New - Current result for ASIN {asin}: {formatted}")
        return {'New - Current': formatted}
    except Exception as e:
        logging.error(f"new_current failed for ASIN {asin}: {str(e)}")
        return {'New - Current': '-'}
# New - Current ends

# New - 30 days avg.
# New - 60 days avg.
# New - 90 days avg.
# New - 180 days avg.
# New - 365 days avg.
# New - Lowest
# New - Lowest 365 days
# New - Highest
# New - Highest 365 days
# New - 90 days OOS
# New - Stock

# New, 3rd Party FBA - Current starts
# 2025-05-20: Impossible to verify New, 3rd Party FBA - Current, as CSV and Keepa showed all '-' for 5 ASINs (commit 7ef4629e). Update uses offers array for reliability.
def new_3rd_party_fba_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    offers = product.get('offers', [])
    current_price = get_stat_value(stats, 'current', 11, divisor=100, is_price=True)
    fba_prices = [o.get('price', -1) / 100 for o in offers if o.get('condition') == 'New' and o.get('isFBA', False)]
    if not fba_prices or current_price == '-' or not any(abs(float(current_price[1:]) - p) < 0.01 for p in fba_prices):
        logging.warning(f"No valid FBA price for ASIN {asin}: stats={current_price}, offers={fba_prices}")
        return {'New, 3rd Party FBA - Current': '-'}
    result = {'New, 3rd Party FBA - Current': current_price}
    logging.debug(f"new_3rd_party_fba_current result for ASIN {asin}: {result}")
    return result
# New, 3rd Party FBA - Current ends

# New, 3rd Party FBA - 30 days avg.
# New, 3rd Party FBA - 60 days avg.
# New, 3rd Party FBA - 90 days avg.
# New, 3rd Party FBA - 180 days avg.
# New, 3rd Party FBA - 365 days avg.
# New, 3rd Party FBA - Lowest
# New, 3rd Party FBA - Lowest 365 days
# New, 3rd Party FBA - Highest
# New, 3rd Party FBA - Highest 365 days
# New, 3rd Party FBA - 90 days OOS
# New, 3rd Party FBA - Stock

# New, 3rd Party FBM - Current starts
# 2025-05-21: Minimal filters, enhanced logging (commit 83b9e853).
# 2025-05-21: Minimal filters, detailed offer logging (commit 923d4e20).
# 2025-05-22: Enhanced logging for offers=100 (commit a03ceb87).
# 2025-05-22: Enhanced logging for Python client, offers=100 (commit 69d2801d).
# 2025-05-22: Added Python client fallback for offers (commit e1f6f52e).
# 2025-05-22: Removed Python client, use HTTP fetch_product offers=100.
def new_3rd_party_fbm_current(product):
    asin = product.get('asin', 'unknown')
    offers = product.get('offers', [])
    logging.debug(f"HTTP FBM offers for ASIN {asin}: count={len(offers)}, offers={offers}")
    fbm_prices = [o.get('price') / 100 for o in offers if o.get('condition') == 'New' and o.get('isFBA', False) is False and o.get('price', -1) > 0]
    if not fbm_prices:
        logging.warning(f"No valid HTTP FBM offers for ASIN {asin}: fbm_prices={fbm_prices}, raw_offers={offers}")
        return {'New, 3rd Party FBM - Current': '-'}
    lowest_fbm = min(fbm_prices)
    formatted = f"${lowest_fbm:.2f}"
    logging.debug(f"New, 3rd Party FBM - Current - lowest_fbm={lowest_fbm}, result={formatted} for ASIN {asin}")
    return {'New, 3rd Party FBM - Current': formatted}
# New, 3rd Party FBM - Current ends





# New, 3rd Party FBM starts
# !!! This one doesn't work - these should all be individual ... maybe !!!
#def new_3rd_party_fbm(product):
#    stats = product.get('stats', {})
#    asin = product.get('asin', 'unknown')
#    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'New' and not o.get('isFBA', False) and o.get('stock', 0) > 0)
#    result = {
#        'New, 3rd Party FBM - 30 days avg.': get_stat_value(stats, 'avg30', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 60 days avg.': get_stat_value(stats, 'avg60', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 90 days avg.': get_stat_value(stats, 'avg90', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 180 days avg.': get_stat_value(stats, 'avg180', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 365 days avg.': get_stat_value(stats, 'avg365', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - Stock': str(stock) if stock > 0 else '0'
#    }
#    logging.debug(f"new_3rd_party_fbm result for ASIN {asin}: {result}")
#    return result
# New, 3rd Party FBM ends
# !!! This one doesn't work - these should all be individual ... maybe !!!

# New, 3rd Party FBM - 30 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 60 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 90 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 180 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 365 days avg. -- ABOVE - but doesn't work ... 

# New, 3rd Party FBM - Lowest
# New, 3rd Party FBM - Lowest 365 days
# New, 3rd Party FBM - Highest
# New, 3rd Party FBM - Highest 365 days
# New, 3rd Party FBM - 90 days OOS

# New, 3rd Party FBM - Stock -- ABOVE - but doesn't work ... 





# Buy Box Used - Current starts
# 2025-05-21: Enhanced logging for stats.current[9] debugging (commit 83b9e853).
# 2025-05-21: Detailed logging for stats.current[9] (commit 923d4e20).
# 2025-05-22: Enhanced logging for stats.current[9], offers=100 (commit a03ceb87).
# 2025-05-22: Enhanced logging for Python client, stats.current[9], offers=100 (commit 69d2801d).
# 2025-05-22: Added Python client fallback for stats.current[9] (commit e1f6f52e).
from keepa import Keepa
def buy_box_used_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current = stats.get('current', [-1] * 20)
    value = current[9] if len(current) > 9 else -1
    logging.debug(f"Buy Box Used - Current HTTP - raw value={value}, current array={current}, stats_keys={list(stats.keys())}, stats_current={stats.get('current', [])}, offers_count={len(product.get('offers', []))} for ASIN {asin}")
    if value <= 0 or value == -1:
        logging.warning(f"No valid HTTP Buy Box Used - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
        try:
            with open('config.json') as f:
                config = json.load(f)
            api = Keepa(config['api_key'])
            py_product = api.query(asin, product_code_is_asin=True, stats=90, domain='US', history=True, offers=100)
            py_stats = py_product[0].get('stats', {}) if py_product else {}
            py_current = py_stats.get('current', [-1] * 20)
            value = py_current[9] if len(py_current) > 9 else -1
            logging.debug(f"Buy Box Used - Current Python - raw value={value}, current array={py_current}, stats_keys={list(py_stats.keys())} for ASIN {asin}")
            if value <= 0 or value == -1:
                logging.warning(f"No valid Python Buy Box Used - Current (value={value}, current_length={len(py_current)}) for ASIN {asin}")
                return {'Buy Box Used - Current': '-'}
        except Exception as e:
            logging.error(f"Python fetch failed for ASIN {asin}: {str(e)}")
            return {'Buy Box Used - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"Buy Box Used - Current result for ASIN {asin}: {formatted}")
        return {'Buy Box Used - Current': formatted}
    except Exception as e:
        logging.error(f"buy_box_used_current failed for ASIN {asin}: {str(e)}")
        return {'Buy Box Used - Current': '-'}
# Buy Box Used - Current ends

# Buy Box Used - 30 days avg.
# Buy Box Used - 60 days avg.
# Buy Box Used - 90 days avg.
# Buy Box Used - 180 days avg.
# Buy Box Used - 365 days avg.
# Buy Box Used - Lowest
# Buy Box Used - Lowest 365 days
# Buy Box Used - Highest
# Buy Box Used - Highest 365 days
# Buy Box Used - 90 days OOS
# Buy Box Used - Stock

# Used - Current starts
def used_current(product):
    stats = product.get('stats', {})
    result = {'Used - Current': get_stat_value(stats, 'current', 2, divisor=100, is_price=True)}
    return result
# Used - Current ends

# Used - 30 days avg.
# Used - 60 days avg.
# Used - 90 days avg.
# Used - 180 days avg.
# Used - 365 days avg.
# Used - Lowest
# Used - Lowest 365 days
# Used - Highest
# Used - Highest 365 days
# Used - 90 days OOS
# Used - Stock

# Used, like new - Current starts
def used_like_new(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    current_price = get_stat_value(stats, 'current', 4, divisor=100, is_price=True)
    result = {'Used, like new - Current': current_price}
    logging.debug(f"used_like_new for ASIN {asin}: stats.current={stats.get('current', [])}, current_price={current_price}")
    return result
# Used, like new - Current ends

# Used, like new - 30 days avg.,
# Used, like new - 60 days avg.,
# Used, like new - 90 days avg.,
# Used, like new - 180 days avg.,
# Used, like new - 365 days avg.,
# Used, like new - Lowest,
# Used, like new - Lowest 365 days,
# Used, like new - Highest,
# Used, like new - Highest 365 days,
# Used, like new - 90 days OOS,
# Used, like new - Stock,

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

# Used, very good - 30 days avg.,
# Used, very good - 60 days avg.,
# Used, very good - 90 days avg.,
# Used, very good - 180 days avg.,
# Used, very good - 365 days avg.,
# Used, very good - Lowest,
# Used, very good - Lowest 365 days,
# Used, very good - Highest,
# Used, very good - Highest 365 days,
# Used, very good - 90 days OOS,
# Used, very good - Stock,

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

# Used, good - 30 days avg.,
# Used, good - 60 days avg.,
# Used, good - 90 days avg.,
# Used, good - 180 days avg.,
# Used, good - 365 days avg.,
# Used, good - Lowest,
# Used, good - Lowest 365 days,
# Used, good - Highest,
# Used, good - Highest 365 days,
# Used, good - 90 days OOS,
# Used, good - Stock,

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

# Used, acceptable - 30 days avg.,
# Used, acceptable - 60 days avg.,
# Used, acceptable - 90 days avg.,
# Used, acceptable - 180 days avg.,
# Used, acceptable - 365 days avg.,
# Used, acceptable - Lowest,
# Used, acceptable - Lowest 365 days,
# Used, acceptable - Highest,
# Used, acceptable - Highest 365 days,
# Used, acceptable - 90 days OOS,
# Used, acceptable - Stock,

# List Price - Current starts
# This one was not working - We're trying to solve this one now:
def list_price(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    current = stats.get('current', [-1] * 20)
    value = current[8] if len(current) > 8 else -1
    logging.debug(f"List Price - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())}, stats_raw={stats} for ASIN {asin}")
    if value <= 0 or value == -1:
        logging.warning(f"No valid List Price - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
        return {'List Price - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"List Price - Current result for ASIN {asin}: {formatted}")
        return {'List Price - Current': formatted}
    except Exception as e:
        logging.error(f"list_price failed for ASIN {asin}: {str(e)}")
        return {'List Price - Current': '-'}
# List Price - Current ends

# List Price - 30 days avg.,
# List Price - 60 days avg.,
# List Price - 90 days avg.,
# List Price - 180 days avg.,
# List Price - 365 days avg.,
# List Price - Lowest,
# List Price - Lowest 365 days,
# List Price - Highest,
# List Price - Highest 365 days,
# List Price - 90 days OOS,
# List Price - Stock,

#### END OF FILE ####