# stable.py
def get_stat_value(stats, key, index, divisor=100, is_price=True):
    value = stats.get(key, [-1] * 30)[index]
    if isinstance(value, list):
        value = value[0] if value else -1
    if value <= 0:
        return '-'
    if is_price:
        return f"${value / divisor:.2f}"
    return f"{value:,}"  # Add comma separators for non-price values

# Title
def get_title(deal):
    result = {'Title': deal.get('title', '-')}
    return result['Title']

# ASIN
def get_asin(deal):
    result = {'ASIN': f'="{deal["asin"]}"'}
    return result['ASIN']

# Buy Box Used - Current
def buy_box_used_current(product):
    stats = product.get('stats', {})
    result = {'Buy Box Used - Current': get_stat_value(stats, 'current', 2, divisor=100, is_price=True)}
    return result

# Sales Rank - Current
def sales_rank_current(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - Current': get_stat_value(stats, 'current', 3, divisor=1, is_price=False)}
    return result