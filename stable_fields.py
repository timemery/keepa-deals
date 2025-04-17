import urllib.parse
from datetime import datetime, timedelta
from pytz import timezone

KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')
PRODUCT_TYPES = {0: 'ABIS_BOOK', 1: 'ABIS_MUSIC'}

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

def get_stable_field(data, deal_data, product_90, header):
    print(f"DEBUG: Stable Header={header}")
    try:
        stats_365 = data.get('stats', {}) if data else {}
        stats_90 = product_90.get('stats', {}) if product_90 else {}
        current = deal_data.get('current', [-1] * 20)

        if header == 'Price Now':
            value = current[2] if len(current) > 2 and current[2] is not None else -1
            return f"${value / 100:.2f}" if value > 0 else '-'
        elif header == 'AMZ link':
            asin = data.get('asin', '')
            return f"https://www.amazon.com/dp/{asin}" if asin else '-'
        elif header == 'Keepa Link':
            asin = data.get('asin', '')
            return f"https://keepa.com/#!product/1-{asin}" if asin else '-'
        elif header == 'Title':
            value = data.get('title', '')
            return str(value) if value else '-'
        elif header == 'Sales Rank - Reference':
            cat_id = data.get('salesRankReference', 0)
            return f"https://www.amazon.com/b/?node={cat_id}" if cat_id > 0 else '-'
        elif header == 'Reviews - Review Count':
            value = stats_365.get('current', [-1] * 20)[17] if len(stats_365.get('current', [-1] * 20)) > 17 else -1
            return str(value) if value > 0 else '-'
        elif header == 'Tracking since':
            ts = data.get('trackingSince', 0)
            dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 0 else None
            return dt.strftime('%Y-%m-%d') if dt else '-'
        elif header == 'Categories - Root':
            category_tree = data.get('categoryTree', [])
            return category_tree[0]['name'] if category_tree else '-'
        elif header == 'Categories - Sub':
            category_tree = data.get('categoryTree', [])
            return ', '.join(cat['name'] for cat in category_tree[2:]) if len(category_tree) > 2 else '-'
        elif header == 'Categories - Tree':
            return format_categories_tree(data.get('categoryTree', []))
        elif header == 'ASIN':
            asin = data.get('asin', '')
            return f'"{asin.zfill(10)}"'
        elif header == 'Freq. Bought Together':
            return format_freq_bought_together(data.get('frequentlyBoughtTogether', []))
        elif header == 'Type':
            product_type = data.get('productType', 0)
            return PRODUCT_TYPES.get(product_type, str(product_type))
        elif header == 'Manufacturer':
            value = data.get('manufacturer', '')
            return str(value) if value else '-'
        elif header == 'Brand':
            value = data.get('brand', '')
            return str(value) if value else '-'
        elif header == 'Product Group':
            value = data.get('productGroup', '')
            return str(value) if value else '-'
        elif header == 'Item Type':
            value = data.get('productGroup', '')
            return str(value) if value else '-'
        elif header == 'Author':
            value = data.get('author', '')
            return str(value) if value else '-'
        elif header == 'Contributors':
            return format_contributors(data.get('contributors', []))
        elif header == 'Binding':
            value = data.get('binding', '')
            return str(value) if value else '-'
        elif header == 'Publication Date':
            return format_date(data.get('publicationDate', ''), full_date=False)
        elif header == 'Languages':
            return format_languages(data.get('languages', []))
        elif header == 'Sales Rank - Current':
            value = stats_365.get('current', [-1] * 20)[3] if len(stats_365.get('current', [-1] * 20)) > 3 else -1
            return f"{value:,}" if value > 0 else '-'
        elif header == 'Buy Box - 90 days OOS':
            value = stats_90.get('outOfStockPercentage90', [-1] * 20)[10] if len(stats_90.get('outOfStockPercentage90', [-1] * 20)) > 10 else -1
            return f"{value}%" if value >= 0 else '-'
        elif header == 'Amazon - 90 days OOS':
            value = stats_90.get('outOfStockPercentage90', [-1] * 20)[0] if len(stats_90.get('outOfStockPercentage90', [-1] * 20)) > 0 else -1
            return f"{value}%" if value >= 0 else '-'
        elif header == 'New, 3rd Party FBA - 90 days OOS':
            value = stats_90.get('outOfStockPercentage90', [-1] * 20)[5] if len(stats_90.get('outOfStockPercentage90', [-1] * 20)) > 5 else -1
            return f"{value}%" if value >= 0 else '-'
        elif header == 'New, 3rd Party FBM - 90 days OOS':
            value = stats_90.get('outOfStockPercentage90', [-1] * 20)[6] if len(stats_90.get('outOfStockPercentage90', [-1] * 20)) > 6 else -1
            return f"{value}%" if value >= 0 else '-'
        print(f"DEBUG: {header} - no handler, defaulting to '-'")
        return '-'
    except Exception as e:
        print(f"DEBUG: Error in get_stable_field for header={header}: {e}")
        return '-'

def extract_stable_fields(deal, product_90, product_365, headers):
    print(f"DEBUG: Input data - deal_asin={deal.get('asin')}, product_90_stats={product_90.get('stats') is not None}, product_365_stats={product_365.get('stats') is not None}")
    result = []
    for h in headers:
        try:
            value = get_stable_field(product_365, deal, product_90, h)
            result.append(value)
        except Exception as e:
            print(f"DEBUG: Error in get_stable_field for header={h}: {e}")
            result.append('-')
    print(f"DEBUG: stable_row length={len(result)}, expected={len(headers)}")
    return result