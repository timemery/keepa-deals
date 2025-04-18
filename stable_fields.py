# stable_fields.py
def format_price_now(deal):
    current = deal.get('current', [-1] * 20)[2] if len(deal.get('current', [-1] * 20)) > 2 else -1
    return f"${current / 100:.2f}" if current > 0 else "-"

def format_deal_found(deal):
    ts = deal.get('creationDate', 0)
    from datetime import datetime, timedelta
    from pytz import timezone
    KEEPA_EPOCH = datetime(2011, 1, 1)
    TORONTO_TZ = timezone('America/Toronto')
    dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
    return TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else "-"

def format_amz_link(deal):
    asin = deal.get('asin', '')
    return f"https://www.amazon.com/dp/{asin}" if asin else "-"

def format_keepa_link(deal):
    asin = deal.get('asin', '')
    return f"https://keepa.com/#!product/1-{asin}" if asin else "-"

def format_fba_fee(data):
    weight = data.get('packageWeight', 0)
    height = data.get('packageHeight', 0)
    length = data.get('packageLength', 0)
    width = data.get('packageWidth', 0)
    from .Process500_Deals_v6 import calculate_fba_fee
    return calculate_fba_fee(weight, height, length, width)

def format_referral_fee(data, deal):
    current = deal.get('current', [-1] * 20)[2] if len(deal.get('current', [-1] * 20)) > 2 else -1
    price = current / 100 if current > 0 else 0
    category_tree = data.get('categoryTree', [])
    from .Process500_Deals_v6 import calculate_referral_fee
    return calculate_referral_fee(price, category_tree)

def format_percent_down_90(deal, stats_90):
    avg = stats_90.get('avg', [-1] * 20)[2] if len(stats_90.get('avg', [-1] * 20)) > 2 else -1
    current = deal.get('current', [-1] * 20)[2] if len(deal.get('current', [-1] * 20)) > 2 else -1
    value = ((avg - current) / avg * 100) if avg > 0 and current > 0 else -1
    return f"{value:.0f}%" if value > 0 else "-"