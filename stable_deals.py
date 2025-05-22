# stable_deals.py force change window
import logging
import requests
import json
import urllib.parse
from retrying import retry
from datetime import datetime, timedelta
from pytz import timezone

# Configure logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

# Constants
KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')

# Load API key
try:
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        logging.debug(f"API key loaded: {api_key[:5]}...")
except Exception as e:
    logging.error(f"API key load failed: {str(e)}")
    raise SystemExit(1)

def validate_asin(asin):
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        return False
    return True

# Do not modify fetch_deals_for_deals! It mirrors the "Show API query" (https://api.keepa.com/deal), with critical parameters.
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_deals_for_deals(page):
    logging.debug(f"Fetching deals page {page} for Percent Down 90...")
    print(f"Fetching deals page {page} for Percent Down 90...")
    deal_query = {
        "page": page,
        "domainId": "1",
        "excludeCategories": [],
        "includeCategories": [283155],
        "priceTypes": [2],
        "deltaRange": [1950, 9900],
        "deltaPercentRange": [50, 2147483647],
        "salesRankRange": [50000, 1500000],
        "currentRange": [2000, 30100],
        "minRating": 10,
        "isLowest": False,
        "isLowest90": False,
        "isLowestOffer": False,
        "isOutOfStock": False,
        "titleSearch": "",
        "isRangeEnabled": True,
        "isFilterEnabled": True,
        "filterErotic": False,
        "singleVariation": True,
        "hasReviews": False,
        "isPrimeExclusive": False,
        "mustHaveAmazonOffer": False,
        "mustNotHaveAmazonOffer": False,
        "sortType": 4,
        "dateRange": "3"
    }
    query_json = json.dumps(deal_query, separators=(',', ':'), sort_keys=True)
    logging.debug(f"Raw query JSON: {query_json}")
    encoded_selection = urllib.parse.quote(query_json)
    url = f"https://api.keepa.com/deal?key={api_key}&selection={encoded_selection}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    logging.debug(f"Deal URL: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Full deal response: {response.text}")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            print(f"Deal fetch failed: {response.status_code}, {response.text}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        logging.debug(f"Fetched {len(deals)} deals: {[d.get('asin', '-') for d in deals]}")
        logging.debug(f"Deal response structure: {list(data.get('deals', {}).keys())}")
        logging.debug(f"All deal keys: {[list(d.keys()) for d in deals]}")
        logging.debug(f"Deals data: {[{'asin': d.get('asin', '-'), 'current': d.get('current', []), 'current[9]': d.get('current', [-1] * 20)[9] if len(d.get('current', [])) > 9 else -1, 'current[1]': d.get('current', [-1] * 20)[1] if len(d.get('current', [])) > 1 else -1} for d in deals]}")
        print(f"Fetched {len(deals)} deals")
        return deals[:5]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []
        
# Deal Found starts
def deal_found(deal):
    ts = deal.get('creationDate', 0)
    logging.debug(f"Deal found - raw ts={ts}")
    dt = (KEEPA_EPOCH + timedelta(minutes=ts)) if ts > 100000 else None
    return {'Deal found': TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S') if dt else '-'}
# Deal Found ends

# Last update starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def last_update(deal):
    ts = deal.get('lastUpdate', 0)
    logging.debug(f"last update - raw ts={ts}")
    if ts <= 100000:
        logging.error(f"No valid lastUpdate for deal: {deal}")
        return {'last update': '-'}
    try:
        dt = KEEPA_EPOCH + timedelta(minutes=ts)
        formatted = TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"last update result: {formatted}")
        return {'last update': formatted}
    except Exception as e:
        logging.error(f"last_update failed: {str(e)}")
        return {'last update': '-'}
# Last update ends

# Last price change starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def last_price_change(deal):
    ts = deal.get('currentSince', [-1] * 20)[11]
    logging.debug(f"last price change - raw ts={ts}")
    if ts <= 100000:
        logging.error(f"No valid currentSince[11] for deal: {deal}")
        return {'last price change': '-'}
    try:
        dt = KEEPA_EPOCH + timedelta(minutes=ts)
        formatted = TORONTO_TZ.localize(dt).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"last price change result: {formatted}")
        return {'last price change': formatted}
    except Exception as e:
        logging.error(f"last_price_change failed: {str(e)}")
        return {'last price change': '-'}
# Last price change ends

#### END OF FILE ####