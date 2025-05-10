# stable_deals.py
import logging
from datetime import datetime
import requests
import json
import urllib.parse
from retrying import retry

def validate_asin(asin):
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        return False
    return True

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def fetch_deals_for_deals(page, api_key):
    logging.debug(f"Fetching deals page {page} for Deal found...")
    print(f"Fetching deals page {page} for Deal found...")
    deal_query = {
        "page": page,
        "domainId": "1",
        "includeCategories": [283155],
        "priceTypes": [0],  # Reference code used broader price types
        "deltaPercentRange": [50, 90],
        "salesRankRange": [1, 1500000],
        "currentRange": [1000, 50000],
        "sortType": 0,
        "dateRange": "7",
        "isFilterEnabled": True,
        "filterErotic": True
    }
    query_json = json.dumps(deal_query, separators=(',', ':'), sort_keys=True)
    logging.debug(f"Raw query JSON: {query_json}")
    encoded_selection = urllib.parse.quote(query_json)
    url = f"https://api.keepa.com/deal?key={api_key}&selection={encoded_selection}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    logging.debug(f"Deal URL: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        logging.debug(f"Deal response: {response.text[:1000]}...")
        if response.status_code != 200:
            logging.error(f"Deal fetch failed: {response.status_code}, {response.text}")
            print(f"Deal fetch failed: {response.status_code}")
            return []
        data = response.json()
        deals = data.get('deals', {}).get('dr', [])
        logging.debug(f"Fetched {len(deals)} deals: {[d.get('asin', '-') for d in deals[:5]]}")
        logging.debug(f"Deal response structure: {list(data.get('deals', {}).keys())}")
        logging.debug(f"First deal keys: {[list(d.keys()) for d in deals[:1]]}")
        logging.debug(f"First deal full: {deals[:1]}")
        print(f"Fetched {len(deals)} deals")
        return deals[:5]
    except Exception as e:
        logging.error(f"Deal fetch exception: {str(e)}")
        print(f"Deal fetch exception: {str(e)}")
        return []

# Deal Found starts
def deal_found(deal):
    logging.debug(f"deal_found input keys: {list(deal.keys())}")
    deal_date = deal.get('dealStartDate', -1)
    if deal_date == -1:
        logging.error(f"No dealStartDate for deal: {deal}")
        return {'Deal found': '-'}
    try:
        date = datetime.fromtimestamp(deal_date / 1000).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"deal_found result: {date}")
        return {'Deal found': date}
    except Exception as e:
        logging.error(f"deal_found failed: {str(e)}")
        return {'Deal found': '-'}
# Deal Found ends
