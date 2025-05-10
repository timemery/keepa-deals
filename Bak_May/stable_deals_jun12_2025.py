# stable_deals.py
import logging
from datetime import datetime

def validate_asin(asin):
    if not isinstance(asin, str) or len(asin) != 10 or not asin.isalnum():
        logging.error(f"Invalid ASIN format: {asin}")
        return False
    return True

# Deal Found starts
def deal_found(deal, date_index=None):
    logging.debug(f"deal_found input keys: {list(deal.keys())}")
    deal_date = deal.get('dealStartDate', deal.get('dealDate', deal.get('startTime', deal.get('dealStartTime', deal.get('timestamp', -1)))))
    if deal_date == -1 and date_index is not None:
        logging.debug(f"Trying date_index: {date_index}")
        deal_date = date_index.get(deal.get('asin', ''), -1)
    if deal_date == -1:
        logging.error(f"No timestamp for deal: {deal}")
        return {'Deal found': '-'}
    try:
        date = datetime.fromtimestamp(deal_date / 1000).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"deal_found result: {date}")
        return {'Deal found': date}
    except Exception as e:
        logging.error(f"deal_found failed: {str(e)}")
        return {'Deal found': '-'}
# Deal Found ends
