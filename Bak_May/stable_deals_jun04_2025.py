# stable_deals.py
import logging
from datetime import datetime

def validate_asin(asin):
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        return False
    return True

# Deal Found starts
def deal_found(deal):
    deal_date = deal.get('dealDate', -1)
    if deal_date == -1:
        logging.error(f"No dealDate for deal: {deal}")
        return {'Deal found': '-'}
    try:
        # Convert Keepa timestamp (milliseconds) to readable date
        date = datetime.fromtimestamp(deal_date / 1000).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"deal_found result: {date}")
        return {'Deal found': date}
    except Exception as e:
        logging.error(f"deal_found failed: {str(e)}")
        return {'Deal found': '-'}
# Deal Found ends
