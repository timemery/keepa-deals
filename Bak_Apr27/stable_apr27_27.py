# stable.py
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
        return f"{int(value / divisor):,}"  # Cast to int to remove decimals
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