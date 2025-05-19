# Chunk 1 starts
# stable_deals.py change_win_1
import logging
import requests
import json
import urllib.parse
from retrying import retry
from datetime import datetime, timedelta
from pytz import timezone
from typing import Dict, Any

# Configure logging
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

# Constants
KEEPA_EPOCH = datetime(2011, 1, 1)
TORONTO_TZ = timezone('America/Toronto')

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_deals_for_deals(page: int) -> list:
    logging.debug(f"Fetching deals page {page} for Percent Down 90...")
    print(f"Fetching deals page {page} for Percent Down 90...")
    try:
        with open('config.json') as f:
            config = json.load(f)
            api_key = config['api_key']
            logging.debug(f"API key loaded: {api_key[:5]}...")
    except Exception as e:
        logging.error(f"API key load failed: {str(e)}")
        return []
    
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
        "dateRange": "3",
        "warehouseConditions": [2, 3, 4, 5]
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
        logging.debug(f"Full deals: {deals}")
        print(f"Fetched {len(deals)} deals")
        return deals[:5]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []

def validate_asin(asin: str) -> bool:
    """Validate ASIN format (10 characters, alphanumeric)."""
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        return False
    return True

def deal_found(deal: Dict[str, Any]) -> str:
    deal_time = deal.get('dealTime', None)
    if deal_time:
        try:
            dt = datetime.fromtimestamp(deal_time / 1000, tz=TORONTO_TZ)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.error(f"deal_found failed for deal: {str(e)}")
            return '-'
    logging.debug(f"No valid dealTime for deal: {deal}")
    return '-'

def amz_link(deal: Dict[str, Any]) -> str:
    asin = deal.get('asin', '-')
    return f"https://www.amazon.com/dp/{asin}" if asin != '-' else '-'

def keepa_link(deal: Dict[str, Any]) -> str:
    asin = deal.get('asin', '-')
    return f"https://keepa.com/#!product/1-{asin}" if asin != '-' else '-'

def last_update(deal: Dict[str, Any]) -> str:
    update_time = deal.get('lastUpdate', None)
    if update_time:
        try:
            dt = datetime.fromtimestamp(update_time, tz=TORONTO_TZ)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.error(f"last_update failed for deal: {str(e)}")
            return '-'
    logging.debug(f"No valid lastUpdate for deal: {deal}")
    return '-'

def last_price_change(deal: Dict[str, Any]) -> str:
    price_change_time = deal.get('lastPriceChange', None)
    if price_change_time:
        try:
            dt = datetime.fromtimestamp(price_change_time, tz=TORONTO_TZ)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.error(f"last_price_change failed for deal: {str(e)}")
            return '-'
    logging.debug(f"No valid lastPriceChange for deal: {deal}")
    return '-'
# Chunk 1 ends