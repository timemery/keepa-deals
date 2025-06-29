# stable_products.py
# (Last update: Version 5)

# Unchanged imports and globals
import requests
import logging
from retrying import retry
from datetime import datetime, timedelta
from pytz import timezone
from stable_deals import validate_asin
import json
# Removed unused import: from keepa import Keepa

# Fetch Product for Retry - starts
# We removed this whole chunk - I'm leaving it here commented out to remind us that we don't want it. Amazon - Current is unique because it relies on stats.current[1], which requires a direct Keepa API call
#@retry(stop_max_attempt_number=3, wait_fixed=2000)
#def fetch_product_for_retry(asin):
#    with open('config.json') as f:
#        config = json.load(f)
#    api = Keepa(config['api_key'])
#    product = api.query(asin, product_code_is_asin=True, stats=90, domain='US', history=True, offers=20)
#    if not product or not product[0]:
#        logging.error(f"fetch_product_for_retry failed: no product data for ASIN {asin}")
#        return {}
#    stats = product[0].get('stats', {})
#    stats_current = stats.get('current', [-1] * 20)
#    offers = product.get('offers', []) if product.get('offers') is not None else []
#    logging.debug(f"fetch_product_for_retry response for ASIN {asin}: stats_keys={list(stats.keys())}, stats_current={stats_current}, stats_raw={stats}, offers_count={len(offers)}")
#    return product[0]
# Fetch Product for Retry - ends

# Constants
KEEPA_EPOCH_DATETIME = datetime(2000, 1, 1) # Keepa epoch is Jan 1, 2000
KEEPA_EPOCH_LEGACY = datetime(2011, 1, 1) # Older epoch used in some functions
TORONTO_TZ = timezone('America/Toronto')

# Helper function to convert Keepa Time Minutes (KTM) to a formatted string
# KTM is minutes since January 1, 2000, 00:00:00 UTC
def keepa_minutes_to_datetime_str(keepa_minutes, date_format='%Y-%m-%d'):
    """Converts Keepa time minutes to a datetime string."""
    if keepa_minutes is None or not isinstance(keepa_minutes, int) or keepa_minutes <= 0:
        return '-'
    try:
        # Keepa time is minutes past January 1, 2000 UTC
        dt_utc = KEEPA_EPOCH_DATETIME + timedelta(minutes=keepa_minutes)
        # Convert to Toronto time as per other date fields in this file
        dt_toronto = dt_utc.replace(tzinfo=timezone('UTC')).astimezone(TORONTO_TZ)
        return dt_toronto.strftime(date_format)
    except Exception as e:
        logging.error(f"Error converting Keepa minutes ({keepa_minutes}) to datetime: {e}")
        return '-'

# Shared globals
API_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}

# Global stuff starts
def get_stat_value(stats, key, index, divisor=1, is_price=False):
    try:
        value = stats.get(key, [])
        logging.debug(f"get_stat_value: key={key}, index={index}, stats[{key}]={value}")
        if not value or len(value) <= index:
            logging.warning(f"get_stat_value: No data for key={key}, index={index}, returning '-'")
            return '-'
        value = value[index]
        logging.debug(f"get_stat_value: key={key}, index={index}, value={value}")
        if isinstance(value, list):
            value = value[1] if len(value) > 1 else -1
        if value == -1 or value is None:
            return '-'
        if is_price:
            return f"${value / divisor:.2f}"
        return f"{int(value / divisor):,}"
    except (IndexError, TypeError, AttributeError) as e:
        logging.error(f"get_stat_value failed: stats={stats}, key={key}, index={index}, error={str(e)}")
        return '-'
# Global stuff ends

# Percent Down 90 starts
def percent_down_90(product):
    logging.debug(f"percent_down_90 input: {product.get('asin', '-')}")
    stats_90 = product.get('stats', {})
    avg = stats_90.get('avg90', [-1] * 20)[2]  # Used price
    curr = stats_90.get('current', [-1] * 20)[2]  # Used price
    if avg <= 0 or curr < 0 or avg is None or curr is None:
        logging.error(f"No valid avg90 or current for ASIN {product.get('asin', '-')}: avg={avg}, curr={curr}")
        return {'Percent Down 90': '-'}
    try:
        value = ((avg - curr) / avg * 100)
        percent = f"{value:.0f}%"
        logging.debug(f"percent_down_90 result: {percent}")
        return {'Percent Down 90': percent}
    except Exception as e:
        logging.error(f"percent_down_90 failed: {str(e)}")
        return {'Percent Down 90': '-'}
# Percent Down 90 ends

# Avg. Price 90,
# Percent Down 365,
# Avg. Price 365,

# Price Now starts - This produces correct data for Sales Rank - Current -- NOT Price Now
#def price_now(product):
#    stats = product.get('stats', {})
#    result = {'Price Now': get_stat_value(stats, 'current', 3, divisor=100, is_price=True)}
#    logging.debug(f"price_now result for ASIN {product.get('asin', 'unknown')}: {result}")
#    return result
# Price Now ends

# Price Now Source,
# Deal found (stable_deals) 

# AMZ link starts
def amz_link(product):
    asin = product.get('asin', '-')
    result = {'AMZ link': f"https://www.amazon.com/dp/{asin}" if asin != '-' else '-'}
    logging.debug(f"amz_link result for ASIN {asin}: {result}")
    return result
# AMZ link ends

# Keepa Link starts
def keepa_link(product):
    asin = product.get('asin', '-')
    result = {'Keepa Link': f"https://keepa.com/#!product/1-{asin}" if asin != '-' else '-'}
    logging.debug(f"keepa_link result for ASIN {asin}: {result}")
    return result
# Keepa Link ends

# Title starts
def get_title(product):
    title = product.get('title', '-')
    asin = product.get('asin', 'unknown')
    if title == '-':
        logging.warning(f"get_title: No title found for ASIN {asin}")
    logging.debug(f"get_title result for ASIN {asin}: {title[:50]}")
    return {'Title': title}
# Title ends

# last update (stable_deals) 
# last price change (stable_deals)
# Sales Rank - Reference
# Reviews - Rating
# Reviews - Review Count
# FBA Pick&Pack Fee
# Referral Fee %

# Tracking since starts
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def tracking_since(product):
    ts = product.get('trackingSince', 0)
    logging.debug(f"Tracking since - raw ts={ts}")
    if ts <= 100000: # This threshold might be specific to the 2011 epoch interpretation
        logging.error(f"No valid trackingSince for ASIN {product.get('asin', 'unknown')}")
        return {'Tracking since': '-'}
    try:
        # This function seems to use the older 2011 epoch based on its original implementation
        dt = KEEPA_EPOCH_LEGACY + timedelta(minutes=ts)
        # Assuming ts is minutes from 2011-01-01 and needs localization if it's naive
        if dt.tzinfo is None:
            dt_toronto = TORONTO_TZ.localize(dt)
        else:
            dt_toronto = dt.astimezone(TORONTO_TZ)
        formatted = dt_toronto.strftime('%Y-%m-%d')
        logging.debug(f"Tracking since result for ASIN {product.get('asin', 'unknown')}: {formatted}")
        return {'Tracking since': formatted}
    except Exception as e:
        logging.error(f"tracking_since failed: {str(e)}")
        return {'Tracking since': '-'}
# Tracking since ends

# Categories - Root starts
def categories_root(product):
    category_tree = product.get('categoryTree', [])
    result = {'Categories - Root': category_tree[0]['name'] if category_tree else '-'}
    logging.debug(f"categories_root result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Categories - Root ends

# Categories - Sub starts
def categories_sub(product):
    category_tree = product.get('categoryTree', [])
    result = {'Categories - Sub': ', '.join(cat['name'] for cat in category_tree[2:]) if len(category_tree) > 2 else '-'}
    logging.debug(f"categories_sub result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Categories - Sub ends

# Categories - Tree starts
def categories_tree(product):
    category_tree = product.get('categoryTree', [])
    result = {'Categories - Tree': ' > '.join(cat['name'] for cat in category_tree) if category_tree else '-'}
    logging.debug(f"categories_tree result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Categories - Tree ends

# ASIN starts
def get_asin(product):
    asin = product.get('asin', '-')
    result = {'ASIN': f'="{asin}"' if asin != '-' else '-'}
    logging.debug(f"get_asin result for ASIN {asin}: {result}")
    return result
# ASIN ends

# Freq. Bought Together
# Type

# Manufacturer starts
def manufacturer(product):
    manufacturer_value = product.get('manufacturer', '-')
    result = {'Manufacturer': manufacturer_value}
    logging.debug(f"manufacturer result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Manufacturer ends

# Brand
# Product Group
# Variation Attributes
# Item Type

# Author starts
def author(product):
    author_value = product.get('author', '-')
    result = {'Author': author_value}
    logging.debug(f"author result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Author ends

# Contributors

# Binding starts
def binding(product):
    binding_value = product.get('binding', '-')
    result = {'Binding': binding_value}
    logging.debug(f"binding result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Binding ends

# Number of Items
# Number of Pages
# Publication Date starts
def get_publication_date(product_data):
    """
    Retrieves and formats the publication date of the product.
    Handles Keepa Time Minutes (KTM), YYYYMMDD/YYYYMM/YYYY integers,
    and common date string formats ('YYYY-MM-DD', 'YYYY-MM', 'MMM-YY', 'YYYY').
    Outputs 'YYYY-MM-DD', 'YYYY-MM', or 'YYYY'.
    """
    asin = product_data.get('asin', 'unknown')
    logging.debug(f"ASIN {asin}: Attempting to get publication date.")

    date_value = None
    source_field = None

    # 1. Try 'publicationDate'
    raw_pub_date = product_data.get('publicationDate')
    if raw_pub_date is not None:
        date_value = raw_pub_date
        source_field = 'publicationDate'
        logging.debug(f"ASIN {asin}: Found raw 'publicationDate': {raw_pub_date} (type: {type(raw_pub_date)}) from product_data.")
    
    # 2. Fallback: Check if 'publicationDate' is nested under 'data' (less common for direct product API)
    if date_value is None and 'data' in product_data and isinstance(product_data['data'], dict):
        raw_pub_date_nested = product_data['data'].get('publicationDate')
        if raw_pub_date_nested is not None:
            date_value = raw_pub_date_nested
            source_field = "data['publicationDate']"
            logging.debug(f"ASIN {asin}: Found raw nested 'publicationDate': {raw_pub_date_nested} (type: {type(raw_pub_date_nested)}) from product_data['data'].")

    # 3. Fallback: Try 'releaseDate' if 'publicationDate' was not found
    if date_value is None:
        raw_release_date = product_data.get('releaseDate')
        if raw_release_date is not None:
            date_value = raw_release_date
            source_field = 'releaseDate'
            logging.info(f"ASIN {asin}: No 'publicationDate' found. Using 'releaseDate': {raw_release_date} (type: {type(raw_release_date)}) from product_data.")
        else: # Neither publicationDate nor releaseDate found
            logging.warning(f"ASIN {asin}: Neither 'publicationDate' (direct/nested) nor 'releaseDate' found. Outputting '-'.")
            return {'Publication Date': '-'}
            
    if date_value is None: # Should be caught by above, but as a safeguard
        logging.warning(f"ASIN {asin}: date_value is None after checking all sources. Outputting '-'.")
        return {'Publication Date': '-'}

    formatted_date = '-'
    
    # --- INTEGER PROCESSING ---
    if isinstance(date_value, int):
        logging.debug(f"ASIN {asin}: Integer Input Path: Value '{date_value}' from '{source_field}'.")
        date_str = str(date_value)
        parsed_as_specific_int = False

        # Try YYYYMMDD (e.g., 20170801)
        if not parsed_as_specific_int and len(date_str) == 8 and 19000101 <= date_value <= 20991231:
            logging.debug(f"ASIN {asin}: Integer Path - Attempting YYYYMMDD for {date_value}.")
            try:
                year, month, day = int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8])
                dt_object = datetime(year, month, day)
                formatted_date = dt_object.strftime('%Y-%m-%d')
                logging.info(f"ASIN {asin}: Parsed integer {date_value} as YYYYMMDD -> {formatted_date}.")
                parsed_as_specific_int = True
            except ValueError:
                logging.warning(f"ASIN {asin}: Integer {date_value} resembled YYYYMMDD but failed validation.")
        
        # Try YYYYMM (e.g., 198506)
        if not parsed_as_specific_int and len(date_str) == 6 and 190001 <= date_value <= 209912:
            logging.debug(f"ASIN {asin}: Integer Path - Attempting YYYYMM for {date_value}.")
            try:
                year, month = int(date_str[0:4]), int(date_str[4:6])
                dt_object = datetime(year, month, 1)
                formatted_date = dt_object.strftime('%Y-%m')
                logging.info(f"ASIN {asin}: Parsed integer {date_value} as YYYYMM -> {formatted_date}.")
                parsed_as_specific_int = True
            except ValueError:
                logging.warning(f"ASIN {asin}: Integer {date_value} resembled YYYYMM but failed validation.")

        # Try YYYY (e.g., 1994)
        if not parsed_as_specific_int and len(date_str) == 4 and 1900 <= date_value <= 2099:
            logging.debug(f"ASIN {asin}: Integer Path - Attempting YYYY for {date_value}.")
            try:
                datetime(date_value, 1, 1) # Validate year
                formatted_date = date_str
                logging.info(f"ASIN {asin}: Parsed integer {date_value} as YYYY -> {formatted_date}.")
                parsed_as_specific_int = True
            except ValueError:
                logging.warning(f"ASIN {asin}: Integer {date_value} resembled YYYY but failed validation.")
        
        # Fallback to KTM for positive integers not matching specific date formats
        if not parsed_as_specific_int:
            if date_value > 0:
                logging.debug(f"ASIN {asin}: Integer {date_value} did not match YYYYMMDD/YYYYMM/YYYY. Treating as KTM.")
                formatted_date = keepa_minutes_to_datetime_str(date_value)
                logging.info(f"ASIN {asin}: Processed integer {date_value} as KTM -> {formatted_date}.")
            else: # Handles negative or zero if not parsed above
                 logging.warning(f"ASIN {asin}: Non-positive/unhandled integer date_value {date_value}. Outputting '-'.")


    # --- STRING PROCESSING ---
    elif isinstance(date_value, str):
        logging.debug(f"ASIN {asin}: String Input Path: Value '{date_value}' from '{source_field}'.")
        original_string_value = date_value 
        parsed_string_directly = False # Flag to track if a direct string format was successfully parsed

        # Try 'YYYY-MM-DD'
        if len(date_value) == 10 and date_value[4] == '-' and date_value[7] == '-':
            logging.debug(f"ASIN {asin}: String Path - Attempting 'YYYY-MM-DD' for '{date_value}'.")
            try:
                datetime.strptime(date_value, '%Y-%m-%d')
                formatted_date = date_value
                logging.info(f"ASIN {asin}: Parsed string '{date_value}' as YYYY-MM-DD.")
                parsed_string_directly = True
            except ValueError:
                logging.debug(f"ASIN {asin}: String '{date_value}' resembled YYYY-MM-DD but failed validation.")
        
        # Try 'YYYY-MM' if not already parsed
        if not parsed_string_directly and len(date_value) == 7 and date_value[4] == '-':
            logging.debug(f"ASIN {asin}: String Path - Attempting 'YYYY-MM' for '{date_value}'.")
            try:
                datetime.strptime(date_value, '%Y-%m')
                formatted_date = date_value
                logging.info(f"ASIN {asin}: Parsed string '{date_value}' as YYYY-MM.")
                parsed_string_directly = True
            except ValueError:
                logging.debug(f"ASIN {asin}: String '{date_value}' resembled YYYY-MM but failed validation.")

        # Try 'MMM-YY' (e.g., "Jun-85") if not already parsed
        if not parsed_string_directly and len(date_value) == 6 and date_value[3] == '-':
            logging.debug(f"ASIN {asin}: String Path - Attempting 'MMM-YY' for '{date_value}'.")
            try:
                dt_object = datetime.strptime(date_value, '%b-%y')
                reformatted_value = dt_object.strftime('%Y-%m') # Standardize to YYYY-MM
                logging.info(f"ASIN {asin}: Parsed string '{date_value}' as MMM-YY, successfully reformatted to '{reformatted_value}'.")
                formatted_date = reformatted_value
                parsed_string_directly = True
            except ValueError as e_strptime:
                logging.warning(f"ASIN {asin}: String '{date_value}' resembled MMM-YY but failed strptime('%b-%y'): {e_strptime}.")

        # Try 'YYYY' (4-digit string) if not already parsed
        if not parsed_string_directly and len(date_value) == 4 and date_value.isdigit():
            logging.debug(f"ASIN {asin}: String Path - Attempting 'YYYY' for '{date_value}'.")
            try:
                year_val = int(date_value)
                if 1900 <= year_val <= 2099:
                    datetime(year_val, 1, 1) # Validate year
                    formatted_date = date_value
                    logging.info(f"ASIN {asin}: Parsed string '{date_value}' as YYYY.")
                    parsed_string_directly = True
                else:
                    logging.debug(f"ASIN {asin}: String '{date_value}' is 4-digit but not in year range 1900-2099.")
            except ValueError: 
                logging.debug(f"ASIN {asin}: String '{date_value}' failed YYYY validation unexpectedly.")
        
        # Fallback: Try converting string to integer and re-applying integer logic ONLY if no direct string parse worked
        if not parsed_string_directly:
            logging.debug(f"ASIN {asin}: String '{original_string_value}' did not match direct formats. Attempting integer conversion.")
            try:
                int_from_str = int(original_string_value)
                logging.debug(f"ASIN {asin}: Converted string '{original_string_value}' to int {int_from_str}. Re-processing as integer.")
                
                # --- Nested Integer Processing for Strings ---
                date_str_from_int = str(int_from_str)
                parsed_int_str_as_specific = False

                if len(date_str_from_int) == 8 and 19000101 <= int_from_str <= 20991231: # YYYYMMDD from string
                    logging.debug(f"ASIN {asin}: String-Int Path - Attempting YYYYMMDD for {int_from_str}.")
                    try:
                        year, month, day = int(date_str_from_int[0:4]), int(date_str_from_int[4:6]), int(date_str_from_int[6:8])
                        dt_object = datetime(year, month, day)
                        formatted_date = dt_object.strftime('%Y-%m-%d')
                        logging.info(f"ASIN {asin}: Parsed string-int {int_from_str} as YYYYMMDD -> {formatted_date}")
                        parsed_int_str_as_specific = True
                    except ValueError:
                        logging.warning(f"ASIN {asin}: String-int {int_from_str} resembled YYYYMMDD but failed validation.")
                
                if not parsed_int_str_as_specific and len(date_str_from_int) == 6 and 190001 <= int_from_str <= 209912: # YYYYMM from string
                    logging.debug(f"ASIN {asin}: String-Int Path - Attempting YYYYMM for {int_from_str}.")
                    try:
                        year, month = int(date_str_from_int[0:4]), int(date_str_from_int[4:6])
                        dt_object = datetime(year, month, 1)
                        formatted_date = dt_object.strftime('%Y-%m')
                        logging.info(f"ASIN {asin}: Parsed string-int {int_from_str} as YYYYMM -> {formatted_date}")
                        parsed_int_str_as_specific = True
                    except ValueError:
                        logging.warning(f"ASIN {asin}: String-int {int_from_str} resembled YYYYMM but failed validation.")

                if not parsed_int_str_as_specific and len(date_str_from_int) == 4 and 1900 <= int_from_str <= 2099: # YYYY from string
                    logging.debug(f"ASIN {asin}: String-Int Path - Attempting YYYY for {int_from_str}.")
                    try:
                        datetime(int_from_str, 1, 1) # Validate year
                        formatted_date = date_str_from_int
                        logging.info(f"ASIN {asin}: Parsed string-int {int_from_str} as YYYY -> {formatted_date}")
                        parsed_int_str_as_specific = True # Corrected this flag
                    except ValueError:
                        logging.warning(f"ASIN {asin}: String-int {int_from_str} resembled YYYY but failed validation.")

                if not parsed_int_str_as_specific: # Fallback to KTM for string-ints
                    if int_from_str > 0: 
                        logging.debug(f"ASIN {asin}: String-int {int_from_str} did not match specific formats. Treating as KTM.")
                        formatted_date = keepa_minutes_to_datetime_str(int_from_str)
                        logging.info(f"ASIN {asin}: Processed string-int {int_from_str} as KTM -> {formatted_date}")
                    else:
                         logging.warning(f"ASIN {asin}: Non-positive string-int {int_from_str}. Outputting '-'.")
                # --- End of Nested Integer Processing ---
            except ValueError: # Failed to convert original_string_value to int
                logging.warning(f"ASIN {asin}: String '{original_string_value}' is not a recognized date string and not a valid integer. Outputting '-'.")
    
    else: # Not an int or str
        logging.warning(f"ASIN {asin}: Unexpected data type for date_value: {type(date_value)} ('{date_value}'). Outputting '-'.")

    if formatted_date == '-' and date_value is not None: 
        logging.warning(f"ASIN {asin}: Date value '{date_value}' (type: {type(date_value)}) from field '{source_field}' could not be parsed by any rule. Outputting '-'.")
        
    return {'Publication Date': formatted_date}
# Publication Date ends
# Languages

# Package - Quantity starts
# This one doesn't work - but we're keeping it as a reminder:
#@retry(stop_max_attempt_number=3, wait_fixed=5000)
#def package_quantity(asin, api_key):
#    if not validate_asin(asin):
#        return {'Package - Quantity': '-'}
#    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
#    try:
#        response = requests.get(url, headers=API_HEADERS, timeout=30)
#        logging.debug(f"package_quantity response status for ASIN {asin}: {response.status_code}")
#        if response.status_code != 200:
#            logging.error(f"package_quantity request failed for ASIN {asin}: {response.status_code}")
#            return {'Package - Quantity': '-'}
#        data = response.json()
#        products = data.get('products', [])
#        if not products:
#            logging.error(f"package_quantity no product data for ASIN {asin}")
#            return {'Package - Quantity': '-'}
#        quantity = products[0].get('packageQuantity', -1)
#        logging.debug(f"package_quantity result for ASIN {asin}: {quantity}")
#        return {'Package - Quantity': str(quantity) if quantity != -1 else '-'}
#    except Exception as e:
#        logging.error(f"package_quantity fetch failed for ASIN {asin}: {str(e)}")
#        return {'Package - Quantity': '-'}
# Package - Quantity ends

# Package Weight starts
def package_weight(product):
    weight = product.get('packageWeight', -1)
    result = {'Package Weight': f"{weight / 1000:.2f} kg" if weight != -1 else '-'}
    return result
# Package Weight ends

# Package Height starts
def package_height(product):
    height = product.get('packageHeight', -1)
    if height == -1 or height == 0:
        result = {'Package Height': "Missing"}
    else:
        result = {'Package Height': f"{height / 10:.1f} cm"}
    return result
# Package Height ends

# Package Length starts
def package_length(product):
    length = product.get('packageLength', -1)
    if length == -1 or length == 0:
        result = {'Package Length': "Missing"}
    else:
        result = {'Package Length': f"{length / 10:.1f} cm"}
    return result
# Package Length ends

# Package Width starts
def package_width(product):
    width = product.get('packageWidth', -1)
    if width == -1 or width == 0:
        result = {'Package Width': "Missing"}
    else:
        result = {'Package Width': f"{width / 10:.1f} cm"}
    return result
# Package Width ends

# Listed since starts
def listed_since(product):
    ts = product.get('listedSince', 0)
    asin = product.get('asin', 'unknown')
    logging.debug(f"Listed since - raw ts={ts} for ASIN {asin}")
    if ts <= 0: # This field likely uses the 2011 epoch as well, or is a direct timestamp
        logging.info(f"No valid listedSince (ts={ts}) for ASIN {asin}")
        return {'Listed since': '-'}
    try:
        # This function also seems to use the older 2011 epoch
        dt = KEEPA_EPOCH_LEGACY + timedelta(minutes=ts)
        if dt.tzinfo is None:
            dt_toronto = TORONTO_TZ.localize(dt)
        else:
            dt_toronto = dt.astimezone(TORONTO_TZ)
        formatted = dt_toronto.strftime('%Y-%m-%d')
        logging.debug(f"Listed since result for ASIN {asin}: {formatted}")
        return {'Listed since': formatted}
    except Exception as e:
        logging.error(f"listed_since failed for ASIN {asin}: {str(e)}")
        return {'Listed since': '-'}
# Listed since ends

# Edition
# Release Date
# Format

# Sales Rank - Current starts
def sales_rank_current(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - Current': get_stat_value(stats, 'current', 3, is_price=False)}
    return result
# Sales Rank - Current ends

# Sales Rank - 30 days avg starts
def sales_rank_30_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 30 days avg.': get_stat_value(stats, 'avg30', 3, is_price=False)}
    return result
# Sales Rank - 30 days avg ends

# Sales Rank - 60 days avg.

# Sales Rank - 90 days avg starts
def sales_rank_90_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 90 days avg.': get_stat_value(stats, 'avg90', 3, is_price=False)}
    logging.debug(f"Sales Rank - 90 days avg. for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Sales Rank - 90 days avg ends

# Sales Rank - 180 days avg starts
def sales_rank_180_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 180 days avg.': get_stat_value(stats, 'avg180', 3, is_price=False)}
    return result
# Sales Rank - 180 days avg ends

# Sales Rank - 365 days avg starts
def sales_rank_365_days_avg(product):
    stats = product.get('stats', {})
    result = {'Sales Rank - 365 days avg.': get_stat_value(stats, 'avg365', 3, is_price=False)}
    return result
# Sales Rank - 365 days avg ends

# Sales Rank - Lowest
# Sales Rank - Lowest 365 days
# Sales Rank - Highest
# Sales Rank - Highest 365 days

# Sales Rank - Drops last 30 days starts
def sales_rank_drops_last_30_days(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    value = stats.get('salesRankDrops30', -1)
    logging.debug(f"Sales Rank - Drops last 30 days - raw value={value} for ASIN {asin}")
    if value < 0:
        logging.info(f"No valid Sales Rank - Drops last 30 days (value={value}) for ASIN {asin}")
        return {'Sales Rank - Drops last 30 days': '-'}
    try:
        formatted = str(value)
        logging.debug(f"Sales Rank - Drops last 30 days result for ASIN {asin}: {formatted}")
        return {'Sales Rank - Drops last 30 days': formatted}
    except Exception as e:
        logging.error(f"sales_rank_drops_last_30_days failed for ASIN {asin}: {str(e)}")
        return {'Sales Rank - Drops last 30 days': '-'}
# Sales Rank - Drops last 30 days ends

# Sales Rank - Drops last 60 days
# Sales Rank - Drops last 90 days
# Sales Rank - Drops last 180 days

# Sales Rank - Drops last 365 days starts
def sales_rank_drops_last_365_days(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    value = stats.get('salesRankDrops365', -1)
    logging.debug(f"Sales Rank - Drops last 365 days - raw value={value} for ASIN {asin}")
    if value < 0:
        logging.info(f"No valid Sales Rank - Drops last 365 days (value={value}) for ASIN {asin}")
        return {'Sales Rank - Drops last 365 days': '-'}
    try:
        formatted = str(value)
        logging.debug(f"Sales Rank - Drops last 365 days result for ASIN {asin}: {formatted}")
        return {'Sales Rank - Drops last 365 days': formatted}
    except Exception as e:
        logging.error(f"sales_rank_drops_last_365_days failed for ASIN {asin}: {str(e)}")
        return {'Sales Rank - Drops last 365 days': '-'}
# Sales Rank - Drops last 365 days ends

# Buy Box - Current starts - stopped working after a change to new 3rd party fbm current
# Buy Box - Current starts
def buy_box_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    buy_box_price_raw = stats.get('buyBoxPrice', -1)
    logging.debug(f"Buy Box - Current - ASIN {asin} - Attempting to use 'buyBoxPrice' field. Raw value: {buy_box_price_raw}")

    if buy_box_price_raw is not None and buy_box_price_raw > 0:
        try:
            formatted_price = f"${buy_box_price_raw / 100:.2f}"
            logging.info(f"Buy Box - Current for ASIN {asin}: Using 'buyBoxPrice', value: {formatted_price}")
            return {'Buy Box - Current': formatted_price}
        except Exception as e:
            logging.error(f"Buy Box - Current - ASIN {asin} - Error formatting 'buyBoxPrice' ({buy_box_price_raw}): {str(e)}")
            # Fall through to fallback if formatting fails, though it's unlikely for a number.
    else:
        logging.warning(f"Buy Box - Current - ASIN {asin} - 'buyBoxPrice' is missing, None, or invalid ({buy_box_price_raw}). Attempting fallback.")

    # Fallback logic
    buy_box_seller_id = product.get('buyBoxSellerId')
    # Default to condition 1 (New) if not specified. Keepa API docs suggest 0-11 for condition.
    buy_box_condition = product.get('buyBoxCondition', 1) 
    logging.debug(f"Buy Box - Current - ASIN {asin} - Fallback: buyBoxSellerId='{buy_box_seller_id}', buyBoxCondition='{buy_box_condition}'")

    if buy_box_seller_id:
        offers = product.get('offers', [])
        if not offers:
            logging.warning(f"Buy Box - Current - ASIN {asin} - Fallback: No offers array found to search for sellerId {buy_box_seller_id}.")
        for i, offer in enumerate(offers):
            offer_seller_id = offer.get('sellerId')
            offer_condition = offer.get('condition') # Assuming numeric, directly comparable
            offer_price_cents = offer.get('price', -1) # Assuming price is in cents

            logging.debug(f"Buy Box - Current - ASIN {asin} - Fallback: Checking offer {i}: sellerId='{offer_seller_id}', condition='{offer_condition}', price='{offer_price_cents}'")

            if offer_seller_id == buy_box_seller_id and offer_condition == buy_box_condition:
                if offer_price_cents > 0:
                    try:
                        formatted_price = f"${offer_price_cents / 100:.2f}"
                        logging.info(f"Buy Box - Current for ASIN {asin}: Using Fallback Logic - Found matching offer for sellerId '{buy_box_seller_id}' and condition '{buy_box_condition}'. Price: {formatted_price}")
                        return {'Buy Box - Current': formatted_price}
                    except Exception as e:
                        logging.error(f"Buy Box - Current - ASIN {asin} - Fallback: Error formatting offer price ({offer_price_cents}): {str(e)}")
                        # If formatting this specific offer fails, continue, maybe another offer matches.
                else:
                    logging.warning(f"Buy Box - Current - ASIN {asin} - Fallback: Matching offer found for sellerId '{buy_box_seller_id}' but price is invalid ({offer_price_cents}).")
        logging.warning(f"Buy Box - Current - ASIN {asin} - Fallback: No matching offer found for sellerId '{buy_box_seller_id}' and condition '{buy_box_condition}' with a positive price.")
    else:
        logging.warning(f"Buy Box - Current - ASIN {asin} - Fallback: 'buyBoxSellerId' is missing. Cannot perform fallback search.")

    logging.warning(f"Buy Box - Current - ASIN {asin} - Final decision: No valid Buy Box price found through primary or fallback methods. Returning '-'.")
    return {'Buy Box - Current': '-'}
# Buy Box - Current ends

# Buy Box - 30 days avg.
# Buy Box - 60 days avg.
# Buy Box - 90 days avg.
# Buy Box - 180 days avg.
# Buy Box - 365 days avg.
# Buy Box - Lowest
# Buy Box - Lowest 365 days
# Buy Box - Highest
# Buy Box - Highest 365 days
# Buy Box - 90 days OOS
# Buy Box - Stock

# Amazon - Current starts
# Amazon - Current is unique because it relies on stats.current[1], which requires a direct Keepa API call.
from retrying import retry
@retry(stop_max_attempt_number=3, wait_fixed=5000)
def amazon_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    # stats.current[0] is typically Amazon's price, while current[1] is New overall.
    price = stats.get('current', [None] * 23)[0] # <--- Changed to 1
    if price is None or price <= 0:
        logging.warning(f"No valid Amazon - Current price for ASIN {asin}")
        return {'Amazon - Current': '-'}
    formatted = f"${price / 100:.2f}"
    logging.debug(f"Amazon - Current result for ASIN {asin}: {formatted}")
    return {'Amazon - Current': formatted}
# Amazon - Current ends

# This one doesn't work - but we're keeping it as a reminder:
# 2025-05-20: Removed &buyBox=1 from fetch_product URL (commit 95aac66e) to fix Amazon - Current, but stats.current[10] still -1 for ASIN 150137012X despite $6.26 offer. Reverted to commit 31cb7bee setup. Pivoted to New - Current. 
# Amazon - Current starts
# def amazon_current(product):
#    asin = product.get('asin', 'unknown')
#    stats = product.get('stats', {})
#    current = stats.get('current', [-1] * 20)
#    value = current[10] if len(current) > 10 else -1
#    logging.debug(f"Amazon - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
#    if value <= 0 or value == -1:
#        logging.warning(f"No valid Amazon - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
#        return {'Amazon - Current': '-'}
#    try:
#        formatted = f"${value / 100:.2f}"
#        logging.debug(f"Amazon - Current result for ASIN {asin}: {formatted}")
#        return {'Amazon - Current': formatted}
#    except Exception as e:
#        logging.error(f"amazon_current failed for ASIN {asin}: {str(e)}")
#        return {'Amazon - Current': '-'}
# Amazon - Current ends

# Amazon - 30 days avg.
# Amazon - 60 days avg.
# Amazon - 90 days avg.
# Amazon - 180 days avg.

# Amazon - 365 days avg. starts
def amazon_365_days_avg(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    price_str = '-'

    logging.debug(f"Amazon - 365 days avg. for ASIN {asin}: Attempting to use stats.avg365[0].")

    avg365_array = stats.get('avg365', [])
    logging.debug(f"ASIN {asin}: stats.avg365 raw: {avg365_array}")

    if avg365_array and len(avg365_array) > 0:
        price_cents = avg365_array[0]
        logging.debug(f"ASIN {asin}: Raw value at stats.avg365[0]: {price_cents}")
        if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
            try:
                price_str = f"${price_cents / 100:.2f}"
                logging.info(f"Amazon - 365 days avg. for ASIN {asin}: Using stats.avg365[0], value: {price_str}")
            except Exception as e:
                logging.error(f"Amazon - 365 days avg. for ASIN {asin}: Error formatting price {price_cents}: {e}. Setting to '-'.")
                price_str = '-'
        else:
            logging.warning(f"Amazon - 365 days avg. for ASIN {asin}: Invalid or missing price at stats.avg365[0] ({price_cents}). Setting to '-'")
            price_str = '-'
    else:
        logging.warning(f"Amazon - 365 days avg. for ASIN {asin}: stats.avg365 array is empty or missing. Setting to '-'")
        price_str = '-'
        
    return {'Amazon - 365 days avg.': price_str}
# Amazon - 365 days avg. ends

# Amazon - Lowest
# Amazon - Lowest 365 days
# Amazon - Highest
# Amazon - Highest 365 days
# Amazon - 90 days OOS
# Amazon - Stock

# New - Current starts
def new_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current = stats.get('current', [-1] * 20)
    value = current[1] if len(current) > 1 else -1
    logging.debug(f"New - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())} for ASIN {asin}")
    if value <= 0 or value == -1:
        logging.warning(f"No valid New - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
        return {'New - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"New - Current result for ASIN {asin}: {formatted}")
        return {'New - Current': formatted}
    except Exception as e:
        logging.error(f"new_current failed for ASIN {asin}: {str(e)}")
        return {'New - Current': '-'}
# New - Current ends

def new_3rd_party_fba_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current_array = stats.get('current', [])
    price_str = '-'

    logging.debug(f"New, 3rd Party FBA - Current for ASIN {asin}: Attempting to use stats.current[10]. current_array length: {len(current_array)}")

    if len(current_array) > 10:
        price_cents = current_array[10]
        logging.debug(f"ASIN {asin}: Raw value at stats.current[10]: {price_cents}")
        if price_cents is not None and price_cents > 0:
            try:
                price_str = f"${price_cents / 100:.2f}"
                logging.info(f"New, 3rd Party FBA - Current for ASIN {asin}: Using stats.current[10], value: {price_str}")
            except Exception as e:
                logging.error(f"New, 3rd Party FBA - Current for ASIN {asin}: Error formatting price {price_cents}: {e}. Setting to '-'.")
                price_str = '-'
        else:
            logging.warning(f"New, 3rd Party FBA - Current for ASIN {asin}: Invalid or missing price at stats.current[10] ({price_cents}). Setting to '-'")
            price_str = '-'
    else:
        logging.warning(f"New, 3rd Party FBA - Current for ASIN {asin}: stats.current array is too short (len {len(current_array)}) to access index 10. Setting to '-'")
        price_str = '-'
        
    return {'New, 3rd Party FBA - Current': price_str}

# New, 3rd Party FBA - Current starts

    # Finds the lowest priced New offer from a 3rd Party FBA seller by parsing the 'offers' array.
# New - 30 days avg.
# New - 60 days avg.
# New - 90 days avg.
# New - 180 days avg.
# New - 365 days avg.
# New - Lowest
# New - Lowest 365 days
# New - Highest
# New - Highest 365 days
# New - 90 days OOS
# New - Stock

# New, 3rd Party FBA - Current starts

    # Finds the lowest priced New offer from a 3rd Party FBA seller by parsing the 'offers' array.
    # Price is usually in offer_csv[1] for current offers, or in 'price' for historical snapshots
    # Condition: 1 for "New". Some offers might use string "New".
    # The 'condition' field in offers seems to be numeric from provided logs.
    # Ensure seller_id exists before comparison
    # Detailed log for each offer considered (can be very verbose, use with caution or sample)
    # logging.debug(f"ASIN {asin} - Offer {i}: price_cents={offer_price_cents}, cond_code={offer_condition_code}, is_new={is_new_condition}, is_fba={is_fba_offer}, seller_id='{seller_id}', is_3p={is_third_party}")
    # logging.debug(f"ASIN {asin} - Offer {i} MATCHED New/3P/FBA criteria: price={offer_price_cents/100}")

# New, 3rd Party FBA - Current ends

# New, 3rd Party FBA - 30 days avg.
# New, 3rd Party FBA - 60 days avg.
# New, 3rd Party FBA - 90 days avg.
# New, 3rd Party FBA - 180 days avg.
# New, 3rd Party FBA - 365 days avg.

# New, 3rd Party FBA - Lowest starts
def new_3rd_party_fba_lowest(product):
    asin = product.get('asin', 'unknown')
    price_str = '-' # Default to '-'

    try:
        stats = product.get('stats', {})
        min_prices_array = stats.get('min', []) # This is an array of arrays

        logging.debug(f"ASIN {asin} - new_3rd_party_fba_lowest: stats.min raw: {min_prices_array}")

        # Index 10 corresponds to 'New, 3rd Party FBA'
        # Each element in min_prices_array is typically [timestamp, price_in_cents]
        if min_prices_array and len(min_prices_array) > 10:
            fba_lowest_pair = min_prices_array[10]
            logging.debug(f"ASIN {asin} - new_3rd_party_fba_lowest: stats.min[10] pair: {fba_lowest_pair}")
            if isinstance(fba_lowest_pair, list) and len(fba_lowest_pair) > 1:
                price_cents = fba_lowest_pair[1] # Get the price (second element)
                if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
                    price_str = f"${price_cents / 100:.2f}"
                    logging.info(f"New, 3rd Party FBA - Lowest for ASIN {asin}: Found price {price_str} from stats.min[10][1]")
                else:
                    logging.warning(f"New, 3rd Party FBA - Lowest for ASIN {asin}: Invalid price value in stats.min[10][1] ({price_cents}).")
            else:
                logging.warning(f"New, 3rd Party FBA - Lowest for ASIN {asin}: stats.min[10] is not a valid pair: {fba_lowest_pair}")
        else:
            logging.warning(f"New, 3rd Party FBA - Lowest for ASIN {asin}: stats.min array is too short or missing (length: {len(min_prices_array)}), cannot access index 10.")

    except Exception as e:
        logging.error(f"Error processing new_3rd_party_fba_lowest for ASIN {asin}: {str(e)}")
        price_str = '-' # Ensure it defaults to '-' on error

    return {'New, 3rd Party FBA - Lowest': price_str}
# New, 3rd Party FBA - Lowest ends

# New, 3rd Party FBA - Lowest 365 days
# New, 3rd Party FBA - Highest
# New, 3rd Party FBA - Highest 365 days
# New, 3rd Party FBA - 90 days OOS
# New, 3rd Party FBA - Stock

# New, 3rd Party FBM - Current starts
# 2025-05-21: Minimal filters, enhanced logging (commit 83b9e853).
# 2025-05-21: Minimal filters, detailed offer logging (commit 923d4e20).
# 2025-05-22: Enhanced logging for offers=100 (commit a03ceb87).
# 2025-05-22: Enhanced logging for Python client, offers=100 (commit 69d2801d).
# 2025-05-22: Added Python client fallback for offers (commit e1f6f52e).
# 2025-05-22: Removed Python client, use HTTP fetch_product offers=100.
def new_3rd_party_fbm_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    current_array = stats.get('current', [])
    price_str = '-'
    source = "None"

    logging.debug(f"New, 3rd Party FBM - Current for ASIN {asin}: Attempting to use stats.current[7]. current_array: {current_array}")

    if len(current_array) > 7:
        price_cents = current_array[7]
        logging.debug(f"ASIN {asin}: Raw value at stats.current[7]: {price_cents}")
        if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
            try:
                price_str = f"${price_cents / 100:.2f}"
                source = "stats.current[7]"
                logging.info(f"New, 3rd Party FBM - Current for ASIN {asin}: Using {source}, value: {price_str}")
            except Exception as e:
                logging.error(f"New, 3rd Party FBM - Current for ASIN {asin}: Error formatting price {price_cents} from stats.current[7]: {e}. Setting to '-'.")
                price_str = '-'
                source = "stats.current[7] (formatting error)"
        else:
            logging.warning(f"New, 3rd Party FBM - Current for ASIN {asin}: Invalid or non-positive price at stats.current[7] ({price_cents}). Setting to '-'")
            price_str = '-'
            source = "stats.current[7] (invalid value)"
    else:
        logging.warning(f"New, 3rd Party FBM - Current for ASIN {asin}: stats.current array is too short (len {len(current_array)}) to access index 7. Setting to '-'")
        price_str = '-'
        source = "stats.current (too short)"
    
    # As per AGENTS.md: "If this direct source is invalid... the column should output "-" rather than falling back to parsing general offers."
    # The offer parsing logic previously here has been removed to adhere to this.

    logging.info(f"New, 3rd Party FBM - Current for ASIN {asin}: Final result: {price_str}, Source: {source}")
    return {'New, 3rd Party FBM - Current': price_str}
# New, 3rd Party FBM - Current ends





# New, 3rd Party FBM starts
# !!! This one doesn't work - these should all be individual ... maybe !!!
#def new_3rd_party_fbm(product):
#    stats = product.get('stats', {})
#    asin = product.get('asin', 'unknown')
#    stock = sum(1 for o in product.get('offers', []) if o.get('condition') == 'New' and not o.get('isFBA', False) and o.get('stock', 0) > 0)
#    result = {
#        'New, 3rd Party FBM - 30 days avg.': get_stat_value(stats, 'avg30', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 60 days avg.': get_stat_value(stats, 'avg60', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 90 days avg.': get_stat_value(stats, 'avg90', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 180 days avg.': get_stat_value(stats, 'avg180', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - 365 days avg.': get_stat_value(stats, 'avg365', 1, divisor=100, is_price=True),
#        'New, 3rd Party FBM - Stock': str(stock) if stock > 0 else '0'
#    }
#    logging.debug(f"new_3rd_party_fbm result for ASIN {asin}: {result}")
#    return result
# New, 3rd Party FBM ends
# !!! This one doesn't work - these should all be individual ... maybe !!!

# New, 3rd Party FBM - 30 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 60 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 90 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 180 days avg. -- ABOVE - but doesn't work ... 
# New, 3rd Party FBM - 365 days avg. -- ABOVE - but doesn't work ... 

# New, 3rd Party FBM - 365 days avg. starts
def new_3rd_party_fbm_365_days_avg(product_data):
    """
    Retrieves the 365-day average price for new items from 3rd party FBM sellers.
    Corresponds to stats.avg365[7].
    Prices are in cents, converted to dollars. Returns '-' if data is unavailable or invalid.
    """
    asin = product_data.get('asin', 'unknown')
    price_str = '-'
    try:
        stats = product_data.get('stats', {})
        if not stats:
            logging.warning(f"ASIN {asin}: 'stats' object missing for new_3rd_party_fbm_365_days_avg.")
            return {"New, 3rd Party FBM - 365 days avg.": "-"}

        avg365_array = stats.get('avg365', [])
        logging.debug(f"ASIN {asin} - new_3rd_party_fbm_365_days_avg: stats.avg365 raw: {avg365_array}")

        # Index 7 is for "New, 3rd Party FBM" (NEW_FBM) average price
        fbm_avg_index = 7

        if len(avg365_array) > fbm_avg_index and \
           avg365_array[fbm_avg_index] is not None and \
           isinstance(avg365_array[fbm_avg_index], (int, float)) and \
           avg365_array[fbm_avg_index] > 0:
            
            price_in_cents = avg365_array[fbm_avg_index]
            price_in_dollars = price_in_cents / 100.0
            formatted_price = f"${price_in_dollars:.2f}" # Format to ensure two decimal places
            logging.info(f"ASIN {asin}: New, 3rd Party FBM - 365 days avg. found: {formatted_price} from stats.avg365[{fbm_avg_index}]")
            price_str = formatted_price
        else:
            logging.info(f"ASIN {asin}: New, 3rd Party FBM - 365 days avg. not available or invalid at stats.avg365[{fbm_avg_index}]. Value: {avg365_array[fbm_avg_index] if len(avg365_array) > fbm_avg_index else 'N/A'}. avg365 array: {avg365_array}")
            price_str = "-"

    except IndexError:
        logging.warning(f"ASIN {asin}: IndexError accessing stats.avg365[{fbm_avg_index}] for New, 3rd Party FBM - 365 days avg. avg365 array: {product_data.get('stats', {}).get('avg365', [])}")
        price_str = "-"
    except TypeError:
        logging.warning(f"ASIN {asin}: TypeError accessing stats.avg365[{fbm_avg_index}] for New, 3rd Party FBM - 365 days avg. avg365 array: {product_data.get('stats', {}).get('avg365', [])}")
        price_str = "-"
    except Exception as e:
        logging.error(f"ASIN {asin}: Unexpected error in new_3rd_party_fbm_365_days_avg: {str(e)}")
        price_str = "-"
    
    return {"New, 3rd Party FBM - 365 days avg.": price_str}
# New, 3rd Party FBM - 365 days avg. ends

# New, 3rd Party FBM - Lowest
# New, 3rd Party FBM - Lowest 365 days
# New, 3rd Party FBM - Highest
# New, 3rd Party FBM - Highest 365 days
# New, 3rd Party FBM - 90 days OOS

# New, 3rd Party FBM - Stock -- ABOVE - but doesn't work ... 





# Buy Box Used - Current starts
# 2025-05-21: Enhanced logging for stats.current[9] debugging (commit 83b9e853).
# 2025-05-21: Detailed logging for stats.current[9] (commit 923d4e20).
# 2025-05-22: Enhanced logging for stats.current[9], offers=100 (commit a03ceb87).
# 2025-05-22: Enhanced logging for Python client, stats.current[9], offers=100 (commit 69d2801d).
# 2025-05-22: Added Python client fallback for stats.current[9] (commit e1f6f52e).
# from keepa import Keepa - we removed this - I'm keeping it commented out to remind us we don't want it
def buy_box_used_current(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    
    logging.debug(f"Buy Box Used - Current for ASIN {asin}: Starting process. Relevant stats keys: buyBoxUsedIsFBA, buyBoxUsedPrice, buyBoxUsedShipping.")

    final_price_cents = -1
    price_source_info = "No valid price found"

    buy_box_used_is_fba = stats.get('buyBoxUsedIsFBA') # Can be True, False, or None
    item_price_cents = stats.get('buyBoxUsedPrice', -1)

    if item_price_cents is not None and item_price_cents > 0:
        if buy_box_used_is_fba is True:
            final_price_cents = item_price_cents
            price_source_info = f"FBA item price: {item_price_cents}"
        else: # FBM or buyBoxUsedIsFBA is None (treat as FBM for safety)
            shipping_price_cents = stats.get('buyBoxUsedShipping', -1)
            price_source_info = f"FBM item price: {item_price_cents}"
            if shipping_price_cents is not None and shipping_price_cents >= 0:
                final_price_cents = item_price_cents + shipping_price_cents
                price_source_info += f" + shipping: {shipping_price_cents} = {final_price_cents}"
            else: # No valid shipping, use item price only for FBM
                final_price_cents = item_price_cents
                price_source_info += " (shipping not specified or invalid)"
    
    if final_price_cents > 0:
        try:
            formatted_price = f"${final_price_cents / 100:.2f}"
            logging.info(f"Buy Box Used - Current for ASIN {asin}: Price found via primary logic. {price_source_info}. Formatted: {formatted_price}")
            return {'Buy Box Used - Current': formatted_price}
        except Exception as e:
            logging.error(f"Buy Box Used - Current for ASIN {asin}: Error formatting price ({final_price_cents}) from primary logic: {str(e)}. Will attempt fallback.")
            # Fall through to fallback if formatting fails
    else:
        logging.info(f"Buy Box Used - Current for ASIN {asin}: Primary FBA/FBM logic did not yield a valid price ({price_source_info}). Attempting fallback to stats.current[32].")

    # Fallback method: Try stats.current[32]
    current = stats.get('current', [])
    if len(current) > 32:
        value_from_current_32 = current[32]
        logging.debug(f"Buy Box Used - Current for ASIN {asin}: Fallback check of stats.current[32]. Value: {value_from_current_32}")
        if value_from_current_32 is not None and value_from_current_32 > 0:
            try:
                formatted_price = f"${value_from_current_32 / 100:.2f}"
                logging.info(f"Buy Box Used - Current for ASIN {asin}: Using fallback stats.current[32]. Raw: {value_from_current_32}, Formatted: {formatted_price}")
                return {'Buy Box Used - Current': formatted_price}
            except Exception as e:
                logging.error(f"Buy Box Used - Current for ASIN {asin}: Error formatting stats.current[32] value ({value_from_current_32}): {str(e)}")
        else:
            logging.info(f"Buy Box Used - Current for ASIN {asin}: Fallback stats.current[32] is missing, None or invalid ({value_from_current_32}).")
    else:
        logging.info(f"Buy Box Used - Current for ASIN {asin}: Fallback stats.current array is too short (len: {len(current)}) to access index 32.")

    logging.warning(f"Buy Box Used - Current for ASIN {asin}: No valid price found. Initial FBA/FBM price calc: {final_price_cents}. Fallback stats.current[32] also failed or not applicable. Returning '-'.")
    return {'Buy Box Used - Current': '-'}
# Buy Box Used - Current ends

# Buy Box Used - 30 days avg.
# Buy Box Used - 60 days avg.
# Buy Box Used - 90 days avg.
# Buy Box Used - 180 days avg.
# Buy Box Used - 365 days avg.
# Buy Box Used - Lowest
# Buy Box Used - Lowest 365 days
# Buy Box Used - Highest
# Buy Box Used - Highest 365 days
# Buy Box Used - 90 days OOS
# Buy Box Used - Stock

# Used - Current starts
def used_current(product):
    stats = product.get('stats', {})
    result = {'Used - Current': get_stat_value(stats, 'current', 2, divisor=100, is_price=True)}
    return result
# Used - Current ends

# Used - 365 days avg starts
def used_365_days_avg(product):
    stats = product.get('stats', {})
    result = {'Used - 365 days avg.': get_stat_value(stats, 'avg365', 2, divisor=100, is_price=True)}
    logging.debug(f"used_365_days_avg result for ASIN {product.get('asin', 'unknown')}: {result}")
    return result
# Used - 365 days avg ends

# Used - 30 days avg.,
# Used - 60 days avg.,
# Used - 90 days avg.,
# Used - 180 days avg.,
# Used - 365 days avg.,
# Used - Lowest,
# Used - Lowest 365 days,
# Used - Highest,
# Used - Highest 365 days,
# Used - 90 days OOS,
# Used - Stock,

# Used, like new - Current starts
# Retrieves the 'Used - Like New' price. Experimental: using stats.current[19]. Previously used stats.current[4].
# Relies on get_stat_value to return '-' if data is unavailable at this index
def used_like_new(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    current_price = get_stat_value(stats, 'current', 19, divisor=100, is_price=True) # <--- changed 'current', 4 to 'current', 19
    result = {'Used, like new - Current': current_price}
    logging.debug(f"used_like_new for ASIN {asin}: stats.current={stats.get('current', [])}, current_price={current_price}")
    return result
# Used, like new - Current ends

# Used, like new - 30 days avg.,
# Used, like new - 60 days avg.,
# Used, like new - 90 days avg.,
# Used, like new - 180 days avg.,
# Used, like new - 365 days avg.,
# Used, like new - Lowest,
# Used, like new - Lowest 365 days,
# Used, like new - Highest,
# Used, like new - Highest 365 days,
# Used, like new - 30 days avg.,
# Used, like new - 60 days avg.,
# Used, like new - 90 days avg.,
# Used, like new - 180 days avg.,

# Used, like new - 365 days avg. starts
def used_like_new_365_days_avg(product):
    """
    Retrieves the 365-day average 'Used - Like New' price from product stats.
    Corresponds to stats.avg365[19].
    Prices are in cents, converted to dollars. Returns '-' if data is unavailable or invalid.
    """
    asin = product.get('asin', 'unknown')
    price_str = '-'
    source_index = 19 

    try:
        stats = product.get('stats', {})
        if not stats:
            logging.warning(f"ASIN {asin}: 'stats' object missing for used_like_new_365_days_avg.")
            return {"Used, like new - 365 days avg.": "-"}

        avg365_array = stats.get('avg365', [])
        logging.debug(f"ASIN {asin} - used_like_new_365_days_avg: stats.avg365 raw: {avg365_array}")

        if len(avg365_array) > source_index:
            price_cents = avg365_array[source_index]
            logging.debug(f"ASIN {asin}: Raw value at stats.avg365[{source_index}] for Used, like new: {price_cents}")
            
            if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
                try:
                    price_str = f"${price_cents / 100:.2f}"
                    logging.info(f"Used, like new - 365 days avg. for ASIN {asin}: Using stats.avg365[{source_index}], value: {price_str}")
                except Exception as e:
                    logging.error(f"Used, like new - 365 days avg. for ASIN {asin}: Error formatting price {price_cents}: {e}. Setting to '-'.")
                    price_str = '-'
            else:
                logging.warning(f"Used, like new - 365 days avg. for ASIN {asin}: Invalid or non-positive price at stats.avg365[{source_index}] ({price_cents}). Setting to '-'")
                price_str = '-'
        else:
            logging.warning(f"Used, like new - 365 days avg. for ASIN {asin}: stats.avg365 array is too short (len {len(avg365_array)}) to access index {source_index}. Setting to '-'")
            price_str = '-'
            
    except Exception as e:
        logging.error(f"ASIN {asin}: Unexpected error in used_like_new_365_days_avg: {str(e)}")
        price_str = "-"
    
    return {"Used, like new - 365 days avg.": price_str}
# Used, like new - 365 days avg. ends

# Used, like new - Lowest,
# Used, like new - Lowest 365 days,
# Used, like new - Highest,
# Used, like new - Highest 365 days,
# Used, like new - 90 days OOS,
# Used, like new - Stock,

# Used, very good - Current starts
# Retrieves the 'Used - Very Good' price. Experimental: using stats.current[20]. Previously stats.current[5].
def used_very_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
#    result = {
#        'Used, very good - Current': get_stat_value(stats, 'current', 5, divisor=100, is_price=True)
#    }
#    logging.debug(f"used_very_good result for ASIN {asin}: {result}")
#    return result
    price_str = get_stat_value(stats, 'current', 20, divisor=100, is_price=True)
    logging.debug(f"Used, very good - Current for ASIN {asin}: Using stats.current[20], result: {price_str}")
    return {'Used, very good - Current': price_str}
# Used, very good - Current ends

# Used, very good - 30 days avg.,
# Used, very good - 60 days avg.,
# Used, very good - 90 days avg.,
# Used, very good - 180 days avg.,
# Used, very good - 365 days avg.,
# Used, very good - Lowest,
# Used, very good - Lowest 365 days,
# Used, very good - Highest,
# Used, very good - Highest 365 days,
# Used, very good - 90 days OOS,
# Used, very good - Stock,

# Used, very good - 365 days avg. starts
def used_very_good_365_days_avg(product):
    """
    Retrieves the 365-day average 'Used - Very Good' price from product stats.
    Corresponds to stats.avg365[20].
    Prices are in cents, converted to dollars. Returns '-' if data is unavailable or invalid.
    """
    asin = product.get('asin', 'unknown')
    price_str = '-'
    source_index = 20 # Index for 'Used - Very Good' in stats.avg365

    try:
        stats = product.get('stats', {})
        if not stats:
            logging.warning(f"ASIN {asin}: 'stats' object missing for used_very_good_365_days_avg.")
            return {"Used, very good - 365 days avg.": "-"}

        avg365_array = stats.get('avg365', [])
        logging.debug(f"ASIN {asin} - used_very_good_365_days_avg: stats.avg365 raw: {avg365_array}")

        if len(avg365_array) > source_index:
            price_cents = avg365_array[source_index]
            logging.debug(f"ASIN {asin}: Raw value at stats.avg365[{source_index}] for Used, very good: {price_cents}")
            
            if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
                try:
                    price_str = f"${price_cents / 100:.2f}"
                    logging.info(f"Used, very good - 365 days avg. for ASIN {asin}: Using stats.avg365[{source_index}], value: {price_str}")
                except Exception as e:
                    logging.error(f"Used, very good - 365 days avg. for ASIN {asin}: Error formatting price {price_cents}: {e}. Setting to '-'.")
                    price_str = '-'
            else:
                logging.warning(f"Used, very good - 365 days avg. for ASIN {asin}: Invalid or non-positive price at stats.avg365[{source_index}] ({price_cents}). Setting to '-'")
                price_str = '-'
        else:
            logging.warning(f"Used, very good - 365 days avg. for ASIN {asin}: stats.avg365 array is too short (len {len(avg365_array)}) to access index {source_index}. Setting to '-'")
            price_str = '-'
            
    except Exception as e:
        logging.error(f"ASIN {asin}: Unexpected error in used_very_good_365_days_avg: {str(e)}")
        price_str = "-"
    
    return {"Used, very good - 365 days avg.": price_str}
# Used, very good - 365 days avg. ends

# Used, good - Current starts
# Retrieves the 'Used - Good' price. Experimental: using stats.current[21]. Previously stats.current[6].
def used_good(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
#    result = {
#        'Used, good - Current': get_stat_value(stats, 'current', 6, divisor=100, is_price=True)
#    }
#    logging.debug(f"used_good result for ASIN {asin}: {result}")
#    return result
    price_str = get_stat_value(stats, 'current', 21, divisor=100, is_price=True)
    logging.debug(f"Used, good - Current for ASIN {asin}: Using stats.current[21], result: {price_str}")
    return {'Used, good - Current': price_str}
# Used, good - Current ends

# Used, good - 30 days avg.,
# Used, good - 60 days avg.,
# Used, good - 90 days avg.,
# Used, good - 180 days avg.,

# Used, good - 365 days avg. starts
def used_good_365_days_avg(product_data):
    """
    Retrieves the 365-day average 'Used - Good' price from product stats.
    Corresponds to stats.avg365[21].
    Prices are in cents, converted to dollars. Returns '-' if data is unavailable or invalid.
    """
    asin = product_data.get('asin', 'unknown')
    price_str = '-'
    source_index = 21 # Index for 'Used - Good' in stats.avg365

    try:
        stats = product_data.get('stats', {})
        if not stats:
            logging.warning(f"ASIN {asin}: 'stats' object missing for used_good_365_days_avg.")
            return {"Used, good - 365 days avg.": "-"}

        avg365_array = stats.get('avg365', [])
        logging.debug(f"ASIN {asin} - used_good_365_days_avg: stats.avg365 raw: {avg365_array}")

        if len(avg365_array) > source_index:
            price_cents = avg365_array[source_index]
            logging.debug(f"ASIN {asin}: Raw value at stats.avg365[{source_index}] for Used, good: {price_cents}")
            
            if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
                try:
                    price_str = f"${price_cents / 100:.2f}"
                    logging.info(f"Used, good - 365 days avg. for ASIN {asin}: Using stats.avg365[{source_index}], value: {price_str}")
                except Exception as e:
                    logging.error(f"Used, good - 365 days avg. for ASIN {asin}: Error formatting price {price_cents}: {e}. Setting to '-'.")
                    price_str = '-'
            else:
                logging.warning(f"Used, good - 365 days avg. for ASIN {asin}: Invalid or non-positive price at stats.avg365[{source_index}] ({price_cents}). Setting to '-'")
                price_str = '-'
        else:
            logging.warning(f"Used, good - 365 days avg. for ASIN {asin}: stats.avg365 array is too short (len {len(avg365_array)}) to access index {source_index}. Setting to '-'")
            price_str = '-'
            
    except Exception as e:
        logging.error(f"ASIN {asin}: Unexpected error in used_good_365_days_avg: {str(e)}")
        price_str = "-"
    
    return {"Used, good - 365 days avg.": price_str}
# Used, good - 365 days avg. ends

# Used, good - Lowest,
# Used, good - Lowest 365 days,
# Used, good - Highest,
# Used, good - Highest 365 days,
# Used, good - 90 days OOS,
# Used, good - Stock,

# Used, acceptable - Current starts
def used_acceptable(product):
    asin = product.get('asin', 'unknown')
    stats = product.get('stats', {})
    price = stats.get('current', [None] * 23)[22]
    if price is None or price <= 0:
        logging.warning(f"No valid Used - Acceptable price for ASIN {asin}")
        return {'Used, acceptable - Current': '-'}
    formatted = f"${price / 100:.2f}"
    logging.debug(f"Used, acceptable - Current result for ASIN {asin}: {formatted}")
    return {'Used, acceptable - Current': formatted}
# Used, acceptable - Current ends

# Used, acceptable - 30 days avg.,
# Used, acceptable - 60 days avg.,
# Used, acceptable - 90 days avg.,
# Used, acceptable - 180 days avg.,
# Used, acceptable - 365 days avg.,
# Used, acceptable - Lowest,
# Used, acceptable - Lowest 365 days,
# Used, acceptable - Highest,
# Used, acceptable - Highest 365 days,
# Used, acceptable - 90 days OOS,
# Used, acceptable - Stock,

# List Price - Current starts
# Retrieves List Price. Experimental: using stats.current[4]. Previously stats.current[8].
def list_price(product):
    stats = product.get('stats', {})
    asin = product.get('asin', 'unknown')
    current = stats.get('current', [-1] * 20)
    value = current[4] if len(current) > 4 else -1 # < --- changed to 4 from 8
    logging.debug(f"List Price - Current - raw value={value}, current array={current}, stats_keys={list(stats.keys())}, stats_raw={stats} for ASIN {asin}")
    if value <= 0 or value == -1:
        logging.warning(f"No valid List Price - Current (value={value}, current_length={len(current)}) for ASIN {asin}")
        return {'List Price - Current': '-'}
    try:
        formatted = f"${value / 100:.2f}"
        logging.debug(f"List Price - Current result for ASIN {asin}: {formatted}")
        return {'List Price - Current': formatted}
    except Exception as e:
        logging.error(f"list_price failed for ASIN {asin}: {str(e)}")
        return {'List Price - Current': '-'}
# List Price - Current ends

# New - 365 days avg. starts
def new_365_days_avg(product):
    """
    Retrieves the 365-day average 'New' price from product stats.
    Formats the price to two decimal places. Returns '-' if data is unavailable.
    """
    asin = product.get('asin', 'unknown')
    try:
        # The stats object contains arrays for different metrics (current, avg30, avg90, avg365, etc.)
        # Index 1 in these arrays typically corresponds to 'NEW' price.
        # Prices are usually in cents.
        avg365_prices = product.get('stats', {}).get('avg365', [])
        
        if avg365_prices and len(avg365_prices) > 1 and avg365_prices[1] is not None and avg365_prices[1] > 0:
            price_in_cents = avg365_prices[1]
            price_formatted = f"{price_in_cents / 100.0:.2f}"
            # logging.info(f"ASIN {asin}: New - 365 days avg. price found: {price_formatted}")
            return {'New - 365 days avg.': price_formatted}
        else:
            # logging.debug(f"ASIN {asin}: New - 365 days avg. price data not available or invalid. avg365_prices[1]: {avg365_prices[1] if len(avg365_prices) > 1 else 'N/A'}")
            return {'New - 365 days avg.': '-'}
    except (IndexError, TypeError, AttributeError) as e:
        # logging.warning(f"ASIN {asin}: Exception while fetching New - 365 days avg. price: {str(e)}")
        return {'New - 365 days avg.': '-'}
# New - 365 days avg. ends

# List Price - 30 days avg.,
# List Price - 60 days avg.,
# List Price - 90 days avg.,
# List Price - 180 days avg.,
# List Price - 365 days avg.,
# List Price - Lowest,
# List Price - Lowest 365 days,
# List Price - Highest,
# List Price - Highest 365 days,
# List Price - 90 days OOS,
# List Price - Stock,

# Buy Box - 365 days avg. starts
def buy_box_365_days_avg(product):
    """
    Retrieves the 365-day average Buy Box price.
    The Buy Box price usually includes shipping.
    """
    asin = product.get('asin', 'unknown')
    try:
        stats = product.get('stats', {})
        if not stats:
            # logging.warning(f"ASIN {asin}: 'stats' object missing for buy_box_365_days_avg.")
            return {'Buy Box - 365 days avg.': '-'}

        avg365 = stats.get('avg365', [])
        
        # Index 18 is BUY_BOX_SHIPPING in Keepa stats arrays based on log analysis
        buy_box_avg_index = 18

        if len(avg365) > buy_box_avg_index and avg365[buy_box_avg_index] is not None and avg365[buy_box_avg_index] > 0:
            price_in_cents = avg365[buy_box_avg_index]
            price_in_dollars = price_in_cents / 100.0
            # logging.info(f"ASIN {asin}: Buy Box - 365 days avg. found: ${price_in_dollars:.2f}")
            return {'Buy Box - 365 days avg.': f"{price_in_dollars:.2f}"}
        else:
            # logging.info(f"ASIN {asin}: Buy Box - 365 days avg. not available or invalid (avg365[{buy_box_avg_index}]). avg365 array: {avg365}")
            return {'Buy Box - 365 days avg.': '-'}

    except IndexError:
        # logging.warning(f"ASIN {asin}: IndexError accessing avg365 for Buy Box - 365 days avg. avg365 array: {stats.get('avg365', [])}")
        return {'Buy Box - 365 days avg.': '-'}
    except TypeError:
        # logging.warning(f"ASIN {asin}: TypeError accessing avg365 for Buy Box - 365 days avg. avg365 array: {stats.get('avg365', [])}")
        return {'Buy Box - 365 days avg.': '-'}
    except Exception as e:
        # logging.error(f"ASIN {asin}: Unexpected error in buy_box_365_days_avg: {str(e)}")
        return {'Buy Box - 365 days avg.': '-'}
# Buy Box - 365 days avg. ends

# New, 3rd Party FBA - 365 days avg. starts
def new_3rd_party_fba_365_days_avg(product_data):
    """
    Retrieves the average price of new 3rd party FBA offers over the last 365 days.
    Corresponds to stats.avg365[10].
    Prices are in cents, converted to dollars. Returns '-' if data is unavailable or invalid.
    """
    asin = product_data.get('asin', 'unknown')
    try:
        stats = product_data.get('stats', {})
        if not stats:
            logging.warning(f"ASIN {asin}: 'stats' object missing for new_3rd_party_fba_365_days_avg.")
            return {"New, 3rd Party FBA - 365 days avg.": "-"}

        avg365_array = stats.get('avg365', [])
        logging.debug(f"ASIN {asin} - new_3rd_party_fba_365_days_avg: stats.avg365 raw: {avg365_array}")

        # Index 10 is assumed for "New, 3rd Party FBA" average price
        fba_avg_index = 10

        if len(avg365_array) > fba_avg_index and \
           avg365_array[fba_avg_index] is not None and \
           isinstance(avg365_array[fba_avg_index], (int, float)) and \
           avg365_array[fba_avg_index] > 0:
            
            price_in_cents = avg365_array[fba_avg_index]
            price_in_dollars = price_in_cents / 100.0
            formatted_price = f"{price_in_dollars:.2f}" # Format to ensure two decimal places
            logging.info(f"ASIN {asin}: New, 3rd Party FBA - 365 days avg. found: ${formatted_price}")
            return {"New, 3rd Party FBA - 365 days avg.": formatted_price}
        else:
            logging.info(f"ASIN {asin}: New, 3rd Party FBA - 365 days avg. not available or invalid (avg365[{fba_avg_index}]). avg365 array: {avg365_array}")
            return {"New, 3rd Party FBA - 365 days avg.": "-"}

    except IndexError:
        logging.warning(f"ASIN {asin}: IndexError accessing avg365 for New, 3rd Party FBA - 365 days avg. avg365 array: {product_data.get('stats', {}).get('avg365', [])}")
        return {"New, 3rd Party FBA - 365 days avg.": "-"}
    except TypeError:
        logging.warning(f"ASIN {asin}: TypeError accessing avg365 for New, 3rd Party FBA - 365 days avg. avg365 array: {product_data.get('stats', {}).get('avg365', [])}")
        return {"New, 3rd Party FBA - 365 days avg.": "-"}
    except Exception as e:
        logging.error(f"ASIN {asin}: Unexpected error in new_3rd_party_fba_365_days_avg: {str(e)}")
        return {"New, 3rd Party FBA - 365 days avg.": "-"}
# New, 3rd Party FBA - 365 days avg. ends

# Buy Box Used - 365 days avg. starts
def buy_box_used_365_days_avg(product_data):
    """
    Retrieves the 365-day average "Buy Box Used" price from product stats.
    Assumes index 32 in stats.avg365 corresponds to this value.
    Prices are in cents, converted to dollars. Returns '-' if data is unavailable or invalid.
    """
    asin = product_data.get('asin', 'unknown')
    price_str = '-'
    source_index = 32 # Hypothetical index for 'Buy Box Used - 365 days avg.'

    try:
        stats = product_data.get('stats', {})
        if not stats:
            logging.warning(f"ASIN {asin}: 'stats' object missing for buy_box_used_365_days_avg.")
            return {"Buy Box Used - 365 days avg.": "-"}

        avg365_array = stats.get('avg365', [])
        logging.debug(f"ASIN {asin} - buy_box_used_365_days_avg: stats.avg365 raw: {avg365_array}")

        if len(avg365_array) > source_index:
            price_cents = avg365_array[source_index]
            logging.debug(f"ASIN {asin}: Raw value at stats.avg365[{source_index}]: {price_cents}")
            
            if price_cents is not None and isinstance(price_cents, (int, float)) and price_cents > 0:
                try:
                    price_str = f"${price_cents / 100:.2f}"
                    logging.info(f"Buy Box Used - 365 days avg. for ASIN {asin}: Using stats.avg365[{source_index}], value: {price_str}")
                except Exception as e:
                    logging.error(f"Buy Box Used - 365 days avg. for ASIN {asin}: Error formatting price {price_cents}: {e}. Setting to '-'.")
                    price_str = '-'
            else:
                logging.warning(f"Buy Box Used - 365 days avg. for ASIN {asin}: Invalid or non-positive price at stats.avg365[{source_index}] ({price_cents}). Setting to '-'")
                price_str = '-'
        else:
            logging.warning(f"Buy Box Used - 365 days avg. for ASIN {asin}: stats.avg365 array is too short (len {len(avg365_array)}) to access index {source_index}. Setting to '-'")
            price_str = '-'
            
    except Exception as e:
        logging.error(f"ASIN {asin}: Unexpected error in buy_box_used_365_days_avg: {str(e)}")
        price_str = "-"
    
    return {"Buy Box Used - 365 days avg.": price_str}
# Buy Box Used - 365 days avg. ends

#### END of stable_products.py ####