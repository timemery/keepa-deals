# stable.py - helping grok reorient itself
import logging

# get_stat_value
def get_stat_value(stats, key, index, divisor=1, is_price=False):
    try:
        value = stats.get(key, [])[index]
        if isinstance(value, list):
            value = value[0] if value else -1
        if value == -1 or value is None:
            return '-'
        if is_price:
            return f"${value / divisor:.2f}"
        return f"{int(value / divisor):,}"  # Fix decimals
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

# List Price
def list_price(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 8 or csv_field[8] is None or not isinstance(csv_field[8], list) or not csv_field[8]:
        logging.warning(f"No valid CSV data for List Price, ASIN {asin}: {csv_field[:9] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[8]
    logging.debug(f"CSV data length for List Price, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for List Price, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
              if isinstance(price, (int, float)) and price > 0 and 
                 isinstance(timestamp, (int, float)) and timestamp > 0] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
                  if isinstance(price, (int, float)) and price > 0 and 
                     isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    result = {
        'List Price - Current': get_stat_value(stats, 'current', 8, divisor=100, is_price=True),
        'List Price - 30 days avg.': get_stat_value(stats, 'avg30', 8, divisor=100, is_price=True),
        'List Price - 60 days avg.': get_stat_value(stats, 'avg60', 8, divisor=100, is_price=True),
        'List Price - 90 days avg.': get_stat_value(stats, 'avg90', 8, divisor=100, is_price=True),
        'List Price - 180 days avg.': get_stat_value(stats, 'avg180', 8, divisor=100, is_price=True),
        'List Price - 365 days avg.': get_stat_value(stats, 'avg365', 8, divisor=100, is_price=True),
        'List Price - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'List Price - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'List Price - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'List Price - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'List Price - 90 days OOS': get_stat_value(stats, 'outOfStock90', 8, is_price=False),
        'List Price - Stock': '-'
    }
    logging.debug(f"list_price result for ASIN {asin}: {result}")
    return result    