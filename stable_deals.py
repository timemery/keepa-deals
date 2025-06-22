# stable_deals.py 
# (Last update: Version 5)

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
# Change the number of Rows Here
        return deals[:25]
# Change the number of Rows Here
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []
        
# Deal Found starts
def deal_found(deal_object, config_data=None, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
    
    asin = deal_object.get('asin', 'Unknown ASIN')
    ts = deal_object.get('creationDate', 0)
    logging.debug(f"Deal found - raw ts={ts}") # Keep original module-level debug log
    if ts <= 100000: # If timestamp is invalid or too old
        dt = None
    else:
        dt = KEEPA_EPOCH + timedelta(minutes=ts) # This is a naive datetime, assumed to be UTC

    if dt:
        utc_dt = timezone('UTC').localize(dt) # Make it timezone-aware UTC
        toronto_dt = utc_dt.astimezone(TORONTO_TZ) # Convert to Toronto time
        
        func_name = 'deal_found'
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - KEEPA_EPOCH.tzinfo: {KEEPA_EPOCH.tzinfo}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - dt (naive UTC from Keepa): {dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - utc_dt (aware UTC): {utc_dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - TORONTO_TZ object: {TORONTO_TZ}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - toronto_dt (converted to Toronto): {toronto_dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - toronto_dt.tzinfo: {toronto_dt.tzinfo}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - toronto_dt.utcoffset(): {toronto_dt.utcoffset()}")
        
        return {'Deal found': toronto_dt.strftime('%Y-%m-%d %H:%M:%S')}
    else:
        return {'Deal found': '-'}
# Deal Found ends

# Last update starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def last_update(deal_object, config_data, logger, product_data=None):
    # Ensure logger is available, though it's expected to be passed by Keepa_Deals.py
    if logger is None: 
        logger = logging.getLogger(__name__)

    asin = deal_object.get('asin', 'Unknown ASIN') # Get ASIN for logging, with a default
    raw_ts_value = None
    source_used = None

    # Try to get lastUpdate from product_data first
    if product_data and isinstance(product_data, dict) and \
       'products' in product_data and isinstance(product_data['products'], list) and \
       len(product_data['products']) > 0 and isinstance(product_data['products'][0], dict) and \
       'lastUpdate' in product_data['products'][0]:
        raw_ts_value = product_data['products'][0]['lastUpdate']
        if raw_ts_value is not None: # Ensure it's not None before logging and setting source
            logger.info(f"ASIN: {asin} - Using lastUpdate from product_data: {raw_ts_value}")
            source_used = 'product_data'
        else: # If product_data['products'][0]['lastUpdate'] is None
            logger.info(f"ASIN: {asin} - lastUpdate is None in product_data. Will attempt fallback.")
            raw_ts_value = None # Explicitly set to None to trigger fallback

    # Fallback to deal_object if not found or invalid in product_data
    if source_used != 'product_data':
        original_deal_ts = deal_object.get('lastUpdate')
        if asin is None or original_deal_ts is None: # asin from deal_object might be None
            logger.info(f"ASIN or raw lastUpdate value missing in deal_object for fallback. ASIN: {asin}, Raw TS from deal: {original_deal_ts}")
        else:
            logger.info(f"ASIN: {asin} - Using lastUpdate from deal_object (fallback): {original_deal_ts}")
        raw_ts_value = original_deal_ts # This might be None
        source_used = 'deal_object'

    ts = raw_ts_value if raw_ts_value is not None else 0
    
    # Existing debug log, now reflects the chosen ts
    logging.debug(f"last update - raw ts={ts} (source: {source_used})")
    
    if ts <= 100000:
        logging.error(f"No valid lastUpdate for deal_object ASIN {asin} (ts={ts}, source={source_used})")
        return {'last update': '-'}
    try:
        dt = KEEPA_EPOCH + timedelta(minutes=ts) # This is a naive datetime, assumed to be UTC
        utc_dt = timezone('UTC').localize(dt) # Make it timezone-aware UTC
        toronto_dt = utc_dt.astimezone(TORONTO_TZ) # Convert to Toronto time
        
        logger.debug(f"ASIN: {asin} - Timezone Debug - KEEPA_EPOCH.tzinfo: {KEEPA_EPOCH.tzinfo}")
        logger.debug(f"ASIN: {asin} - Timezone Debug - dt (naive UTC from Keepa): {dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug - utc_dt (aware UTC): {utc_dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug - TORONTO_TZ object: {TORONTO_TZ}")
        logger.debug(f"ASIN: {asin} - Timezone Debug - toronto_dt (converted to Toronto): {toronto_dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug - toronto_dt.tzinfo: {toronto_dt.tzinfo}")
        logger.debug(f"ASIN: {asin} - Timezone Debug - toronto_dt.utcoffset(): {toronto_dt.utcoffset()}")
        
        formatted = toronto_dt.strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"last update result: {formatted}")
        return {'last update': formatted}
    except Exception as e:
        logging.error(f"last_update failed: {str(e)}")
        return {'last update': '-'}
# Last update ends

# Last price change starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def last_price_change(deal_object, config_data=None, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    asin = deal_object.get('asin', 'Unknown ASIN')
    
    # Log the value of 'lastPriceChange' from the deal_object
    raw_ts_from_lastPriceChange_key = deal_object.get('lastPriceChange')
    logger.info(f"ASIN: {asin} - Raw value from deal_object.get('lastPriceChange'): {raw_ts_from_lastPriceChange_key}")

    # Set ts based *only* on deal_object.get('lastPriceChange')
    raw_ts_value = raw_ts_from_lastPriceChange_key # Use the value we just logged
    ts = raw_ts_value if raw_ts_value is not None else 0
    
    # Updated debug log to reflect the source
    logging.debug(f"last price change - raw ts={ts} (source: deal_object.lastPriceChange)")
    
    if ts <= 100000:
        # Use logger instance for consistency if available, otherwise global logging
        log_message = f"ASIN: {asin} - No valid lastPriceChange value (ts={ts}, source=deal_object.lastPriceChange)"
        if logger:
            logger.error(log_message)
        else:
            logging.error(log_message)
        return {'last price change': '-'}
    
    try:
        dt = KEEPA_EPOCH + timedelta(minutes=ts) # This is a naive datetime, assumed to be UTC
        utc_dt = timezone('UTC').localize(dt) # Make it timezone-aware UTC
        toronto_dt = utc_dt.astimezone(TORONTO_TZ) # Convert to Toronto time

        func_name = 'last_price_change'
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - KEEPA_EPOCH.tzinfo: {KEEPA_EPOCH.tzinfo}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - dt (naive UTC from Keepa): {dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - utc_dt (aware UTC): {utc_dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - TORONTO_TZ object: {TORONTO_TZ}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - toronto_dt (converted to Toronto): {toronto_dt.isoformat()}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - toronto_dt.tzinfo: {toronto_dt.tzinfo}")
        logger.debug(f"ASIN: {asin} - Timezone Debug ({func_name}) - toronto_dt.utcoffset(): {toronto_dt.utcoffset()}")

        formatted = toronto_dt.strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"last price change result: {formatted}")
        return {'last price change': formatted}
    except Exception as e:
        logging.error(f"last_price_change failed: {str(e)}")
        return {'last price change': '-'}
# Last price change ends

#### END of stable_deals.py ####