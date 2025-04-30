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
# Updated list_price function
# Updated list_price
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
    logging.debug(f"CSV raw data for List Price, ASIN {asin}: {csv_data[:40]}")
    logging.debug(f"Stats keys for List Price, ASIN {asin}: {list(stats.keys())}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
              if isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp > 0] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
                  if isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
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
        'List Price - Stock': '0'
    }
    logging.debug(f"list_price stats.current[8] for ASIN {asin}: {stats.get('current', [-1]*30)[8]}")
    logging.debug(f"list_price prices for ASIN {asin}: {prices[:20]}")
    logging.debug(f"list_price result for ASIN {asin}: {result}")
    print(f"List Price for ASIN {asin}: {result}")
    return result

# Used Like New
# Moved used_like_new
def used_like_new(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 4 or csv_field[4] is None or not isinstance(csv_field[4], list) or not csv_field[4]:
        logging.warning(f"No valid CSV data for Used, like new, ASIN {asin}: {csv_field[:5] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[4]
    logging.debug(f"CSV data length for Used, like new, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for Used, like new, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
              if isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp > 0] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
                  if isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'Used - Like New' and o.get('stock', 0) > 0)
    result = {
        'Used, like new - Current': get_stat_value(stats, 'current', 4, divisor=100, is_price=True),
        'Used, like new - 30 days avg.': get_stat_value(stats, 'avg30', 4, divisor=100, is_price=True),
        'Used, like new - 60 days avg.': get_stat_value(stats, 'avg60', 4, divisor=100, is_price=True),
        'Used, like new - 90 days avg.': get_stat_value(stats, 'avg90', 4, divisor=100, is_price=True),
        'Used, like new - 180 days avg.': get_stat_value(stats, 'avg180', 4, divisor=100, is_price=True),
        'Used, like new - 365 days avg.': get_stat_value(stats, 'avg365', 4, divisor=100, is_price=True),
        'Used, like new - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'Used, like new - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, like new - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'Used, like new - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'Used, like new - 90 days OOS': get_stat_value(stats, 'outOfStock90', 4, is_price=False),
        'Used, like new - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"used_like_new stats.current[4] for ASIN {asin}: {stats.get('current', [-1]*30)[4]}")
    logging.debug(f"used_like_new result for ASIN {asin}: {result}")
    print(f"Used, like new for ASIN {asin}: {result}")
    return result

# New, 3rd Party FBM
# Moved functions from Keepa_Deals.py
# Moved new_3rd_party_fbm
def new_3rd_party_fbm(product):
    stats = product.get('stats', {})
    csv_field = product.get('csv', [[] for _ in range(11)])
    asin = product.get('asin', 'unknown')
    if not isinstance(csv_field, list) or len(csv_field) <= 1 or csv_field[1] is None or not isinstance(csv_field[1], list) or not csv_field[1]:
        logging.warning(f"No valid CSV data for New, 3rd Party FBM, ASIN {asin}: {csv_field[:2] if isinstance(csv_field, list) else csv_field}")
        csv_data = []
    else:
        csv_data = csv_field[1]
    logging.debug(f"CSV data length for New, 3rd Party FBM, ASIN {asin}: {len(csv_data)}")
    logging.debug(f"CSV raw data for New, 3rd Party FBM, ASIN {asin}: {csv_data[:20]}")
    prices = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
              if isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp > 0] if csv_data else []
    prices_365 = [price for timestamp, price in zip(csv_data[0::2], csv_data[1::2]) 
                  if isinstance(price, (int, float)) and isinstance(timestamp, (int, float)) and timestamp >= (time.time() - 365*24*3600)*1000] if csv_data else []
    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'New' and not o.get('isFBA', False) and o.get('stock', 0) > 0)
    result = {
        'New, 3rd Party FBM - Current': get_stat_value(stats, 'current', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 30 days avg.': get_stat_value(stats, 'avg30', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 60 days avg.': get_stat_value(stats, 'avg60', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 90 days avg.': get_stat_value(stats, 'avg90', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 180 days avg.': get_stat_value(stats, 'avg180', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - 365 days avg.': get_stat_value(stats, 'avg365', 1, divisor=100, is_price=True),
        'New, 3rd Party FBM - Lowest': f"${min(prices) / 100:.2f}" if prices else '-',
        'New, 3rd Party FBM - Lowest 365 days': f"${min(prices_365) / 100:.2f}" if prices_365 else '-',
        'New, 3rd Party FBM - Highest': f"${max(prices) / 100:.2f}" if prices else '-',
        'New, 3rd Party FBM - Highest 365 days': f"${max(prices_365) / 100:.2f}" if prices_365 else '-',
        'New, 3rd Party FBM - 90 days OOS': get_stat_value(stats, 'outOfStock90', 1, is_price=False),
        'New, 3rd Party FBM - Stock': str(stock) if stock > 0 else '0'
    }
    logging.debug(f"new_3rd_party_fbm stats.current[1] for ASIN {asin}: {stats.get('current', [-1]*30)[1]}")
    logging.debug(f"new_3rd_party_fbm result for ASIN {asin}: {result}")
    print(f"New, 3rd Party FBM for ASIN {asin}: {result}")
    return result