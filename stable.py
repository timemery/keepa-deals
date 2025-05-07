# stable.py - added import time
import logging
import time

# get_stat_value - Added logging.debug
def get_stat_value(stats, key, index, divisor=1, is_price=False):
    try:
        value = stats.get(key, [])
        logging.debug(f"get_stat_value: key={key}, index={index}, stats[{key}]={value}")
        if not value or len(value) <= index:
            logging.warning(f"get_stat_value: No data for key={key}, index={index}, returning '-'")
            return '-'
        value = value[index]
        logging.debug(f"get_stat_value: key={key}, index={index}, value={value}")
        if isinstance(value, list):  # Handle min/max [timestamp, price]
            value = value[1] if len(value) > 1 else -1  # Use price, not timestamp
        if value == -1 or value is None:
            return '-'
        if is_price:
            return f"${value / divisor:.2f}"
        return f"{int(value / divisor):,}"
    except (IndexError, TypeError, AttributeError) as e:
        logging.error(f"get_stat_value failed: stats={stats}, key={key}, index={index}, error={str(e)}")
        return '-'

# Title
def get_title(deal):
    title = deal.get('title', '-')
    return title if title else '-'

# ASIN
def get_asin(deal):
    asin = deal.get('asin', '-')
    return f'="{asin}"' if asin else '-'

# Sales Rank - Current
def sales_rank_current(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - Current': get_stat_value(stats, 'current', 3, is_price=False)}
    return result

# Used - Current
def used_current(product):
    stats = product.get('stats', {})
    result = {'Used - Current': get_stat_value(stats, 'current', 2, divisor=100, is_price=True)}
    return result

# Sales Rank - 30 days avg.
def sales_rank_30_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 30 days avg.': get_stat_value(stats, 'avg30', 3, is_price=False)}
    return result

# Sales Rank - 90 days avg.
def sales_rank_90_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 90 days avg.': get_stat_value(stats, 'avg90', 3, is_price=False)}
    logging.debug(f"Sales Rank - 90 days avg. for ASIN {product.get('asin', 'unknown')}: {result}")
    return result

# Sales Rank - 180 days avg.
def sales_rank_180_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 180 days avg.': get_stat_value(stats, 'avg180', 3, is_price=False)}
    return result

# Sales Rank - 365 days avg.
def sales_rank_365_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 365 days avg.': get_stat_value(stats, 'avg365', 3, is_price=False)}
    return result

# Package Quantity
def package_quantity(product):
    quantity = product.get('packageQuantity', -1)
    if quantity == 0:
        logging.warning(f"Package Quantity is 0 for ASIN {product.get('asin', 'unknown')}, defaulting to 1")
        quantity = 1
    result = {'Package - Quantity': str(quantity) if quantity != -1 else '-'}
    return result

# Package Weight
def package_weight(product):
    weight = product.get('packageWeight', -1)
    result = {'Package Weight': f"{weight / 1000:.2f} kg" if weight != -1 else '-'}
    return result

# Package Height
def package_height(product):
    height = product.get('packageHeight', -1)
    result = {'Package Height': f"{height / 10:.1f} cm" if height != -1 else '-'}
    return result

# Package Length
def package_length(product):
    length = product.get('packageLength', -1)
    result = {'Package Length': f"{length / 10:.1f} cm" if length != -1 else '-'}
    return result

# Package Width
def package_width(product):
    width = product.get('packageWidth', -1)
    result = {'Package Width': f"{width / 10:.1f} cm" if width != -1 else '-'}
    return result

# list_price - current
def list_price(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'List Price - Current': get_stat_value(stats, 'current', 8, divisor=100, is_price=True)
    }
    logging.debug(f"list_price result for ASIN {asin}: {result}")
    return result

# used good - current
def used_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'Used, good - Current': get_stat_value(stats, 'current', 6, divisor=100, is_price=True)
    }
    logging.debug(f"used_good result for ASIN {asin}: {result}")
    return result

# Used, very good - Current
def used_very_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'Used, very good - Current': get_stat_value(stats, 'current', 5, divisor=100, is_price=True)
    }
    logging.debug(f"used_very_good result for ASIN {asin}: {result}")
    return result


# Used, like new - Current
def used_like_new(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    current_price = get_stat_value(stats, 'current', 4, divisor=100, is_price=True)
    offers = product.get('offers', [])
    like_new_offers = [o.get('price', -1) / 100 for o in offers if o.get('condition') == 'Used - Like New']
    result = {'Used, like new - Current': current_price}
    logging.debug(f"used_like_new for ASIN {asin}: current_price={current_price}, like_new_offers={like_new_offers}, result={result}")
    return result

# Used, like new - Lowest Highest
# def used_like_new_lowest_highest(product):
#    stats = product.get('stats', {})
#    asin = product.get('asin', 'unknown')
#    result = {
#        'Used, like new - Lowest': get_stat_value(stats, 'min', 4, divisor=100, is_price=True),
#        'Used, like new - Highest': get_stat_value(stats, 'max', 4, divisor=100, is_price=True)
#    }
#    logging.debug(f"used_like_new_lowest_highest result for ASIN {asin}: {result}")
#    return result

# Used, acceptable - Current
def used_acceptable(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    result = {
        'Used, acceptable - Current': get_stat_value(stats, 'current', 7, divisor=100, is_price=True)
    }
    logging.debug(f"used_acceptable result for ASIN {asin}: {result}")
    return result

# New, 3rd Party FBM - Current
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

# Updated list_price

# Used Like New