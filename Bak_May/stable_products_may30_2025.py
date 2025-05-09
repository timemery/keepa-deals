# stable_products.py
import requests
import logging
from retrying import retry

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
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_asin(asin, api_key):
    from stable_deals import validate_asin  # Import here to avoid circular imports
    if not validate_asin(asin):
        return {'ASIN': '-'}
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"get_asin response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"get_asin request failed for ASIN {asin}: {response.status_code}")
            return {'ASIN': '-'}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"get_asin no product data for ASIN {asin}")
            return {'ASIN': '-'}
        asin_value = products[0].get('asin', '-')
        logging.debug(f"get_asin result for ASIN {asin}: {asin_value}")
        return {'ASIN': f'="{asin_value}"' if asin_value != '-' else '-'}
    except Exception as e:
        logging.error(f"get_asin fetch failed for ASIN {asin}: {str(e)}")
        return {'ASIN': '-'}
# ASIN ends

# Title starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_title(asin, api_key):
    from stable_deals import validate_asin
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

# Package - Quantity starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_quantity(asin, api_key):
    from stable_deals import validate_asin
    if not validate_asin(asin):
        return {'Package - Quantity': '-'}
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"package_quantity response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"package_quantity request failed for ASIN {asin}: {response.status_code}")
            return {'Package - Quantity': '-'}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"package_quantity no product data for ASIN {asin}")
            return {'Package - Quantity': '-'}
        quantity = products[0].get('packageQuantity', -1)
        if quantity == 0:
            logging.warning(f"Package Quantity is 0 for ASIN {asin}, defaulting to 1")
            quantity = 1
        logging.debug(f"package_quantity result for ASIN {asin}: {quantity}")
        return {'Package - Quantity': str(quantity) if quantity != -1 else '-'}
    except Exception as e:
        logging.error(f"package_quantity fetch failed for ASIN {asin}: {str(e)}")
        return {'Package - Quantity': '-'}
# Package - Quantity ends

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