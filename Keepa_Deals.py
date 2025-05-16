# Keepa_Deals.py
# Chunk 1 starts forced a change so grok can review this one... 
import csv
import json
import keepa
import logging
from datetime import datetime
from field_mappings import FUNCTION_LIST
from stable_products import get_stat_value
# Chunk 1 ends

# Chunk 2 starts
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG)
# Chunk 2 ends

# Chunk 3 starts
def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)
# Chunk 3 ends

# Chunk 4 starts
def initialize_api(api_key):
    return keepa.Keepa(api_key)
# Chunk 4 ends

# Chunk 5 starts
def main():
    config = load_config()
    api_key = config['keepa_api_key']
    api = initialize_api(api_key)
    
    with open('deal_filters.json', 'r') as f:
        deal_filters = json.load(f)
    
    deals = api.deals(deal_filters=deal_filters)
    
    with open('headers.json', 'r') as f:
        headers = json.load(f)
    
    with open('Keepa_Deals_Export.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        for deal in deals:
            row = {}
            asin = deal['asin']
            product = api.query(asin, stats=90, history=0)[0]
            
            for func in FUNCTION_LIST:
                if func:  # Skip None placeholders
                    try:
                        result = func(product)
                        row.update(result)
                    except Exception as e:
                        logging.error(f"Error in {func.__name__} for ASIN {asin}: {e}")
                        row.update({headers[FUNCTION_LIST.index(func)]: '-'})
            
            writer.writerow(row)
    
    logging.info(f"Processed {len(deals)} deals at {datetime.now()}")
# Chunk 5 ends

if __name__ == "__main__":
    main()