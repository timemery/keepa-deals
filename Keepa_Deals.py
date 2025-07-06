# Keepa_Deals.py 
# (Last update: Version 5.1)

# Chunk 1 starts: 
# Added argparse
import json, csv, logging, sys, requests, urllib.parse, time, argparse, math
from retrying import retry
from stable_deals import validate_asin, fetch_deals_for_deals
from field_mappings import FUNCTION_LIST
import os
CSV_PATH = os.path.join(os.path.dirname(__file__), "Keepa_Deals_Export.csv")

# Get the main script logger instance
logger = logging.getLogger('KeepaDeals') # Use the same logger as in main()

# --- Jules: Local Quota Management Constants & State ---
MAX_QUOTA_TOKENS = 300
HOURLY_REFILL_PERCENTAGE = 0.05
TOKEN_COST_PER_ASIN = 2 # Corrected based on research: using 'offers' or 'buybox' param costs 2 tokens. We use both.
MIN_QUOTA_THRESHOLD_BEFORE_PAUSE = 25 # Increased from 10 to 25 for a larger buffer
QUOTA_REFILL_INTERVAL_SECONDS = 3600  # 1 hour
DEFAULT_LOW_QUOTA_PAUSE_SECONDS = 900 # 15 minutes (remains the same for now)

# Initialize global state variables for quota management
# These will be modified by functions and within the main loop.
# Consider refactoring to a class if state management becomes too complex.
current_available_tokens = MAX_QUOTA_TOKENS
last_refill_calculation_time = time.time() # Initialize to script start time

# Global dictionary to track attempts for fetch_product retries
fetch_product_attempts = {}

# --- Jules: Additional Throttling & Logging Constants ---
MIN_TIME_SINCE_LAST_CALL_SECONDS = 60 # Minimum quiet time before a new API call (Increased due to no rate limit headers)
# Single ASIN fetch delays
POST_FETCH_SUCCESS_DELAY_SECONDS = 5  
POST_FETCH_ERROR_DELAY_SECONDS = 20 
# Batch ASIN fetch delays
POST_BATCH_SUCCESS_DELAY_SECONDS = 2
POST_BATCH_ERROR_DELAY_SECONDS = 10
# Initialize global state for pre-emptive delay
LAST_API_CALL_TIMESTAMP = 0 
# --- End Additional Throttling & Logging Constants ---
# --- End Local Quota Management ---

# Logging for terminal and file output - starts
# import sys # sys is already imported globally
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Terminal output
        logging.FileHandler('debug_log.txt')  # File output
    ]
)
# Force flush for real-time output
logging.getLogger().handlers[0].flush = sys.stdout.flush

#kept but commented out in case the new one above doesn't work.
#logging.basicConfig(
#    level=logging.INFO,
#    format='%(asctime)s - %(message)s',
#    handlers=[
#        logging.StreamHandler(),  # Terminal output
#        logging.FileHandler('debug_log.txt')  # File output
#    ]
#)
#logger = logging.getLogger(__name__)
# Logging for terminal and file output - ends

# Command-line arguments
parser = argparse.ArgumentParser(description="Keepa Deals Script")
parser.add_argument("--no-cache", action="store_true", help="Force fresh Keepa API calls")
# args = parser.parse_args() # Moved to main()

# Logging - removed this one since we have a new/better one above
#logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

# Cache config and headers
try:
    with open('config.json') as f:
        config = json.load(f)
        api_key = config['api_key']
        print(f"API key loaded: {api_key[:5]}...")
    with open('headers.json') as f:
        HEADERS = json.load(f)
        logging.debug(f"Loaded headers: {len(HEADERS)} fields")
        print(f"Headers loaded: {len(HEADERS)} fields")
except Exception as e:
    logging.error(f"Startup failed: {str(e)}")
    print(f"Startup failed: {str(e)}")
    sys.exit(1)
# Chunk 1 ends

# Chunk 2 starts
# 2025-05-20: Removed &buyBox=1 from fetch_product URL (commit 95aac66e) to fix Amazon - Current, but stats.current[10] still -1 for ASIN 150137012X despite $6.26 offer. Reverted to commit 31cb7bee setup. Pivoted to New - Current.
# 2025-05-22: Updated offers=100, enhanced logging (commit a03ceb87).
# 2025-05-22: Switched to Python client, offers=100 (commit 69d2801d).
# 2025-05-22: Reverted to HTTP, offers=100, added Python client fallback (commit e1f6f52e).
# 2025-05-22: Increased timeout=60, wait_fixed=10000, sleep=2 to fix timeouts for ASINs 1848638930, B0CS6RL7D6, B0C1VSRNNH.
# 2025-05-26: Added --no-cache flag to force fresh API calls.
# It's hard to inject attempt numbers directly when using the @retry decorator from the 'retrying' library
# without access to its internal state or a more complex setup.
# However, we can log entry and exit/exceptions to see if it's being called multiple times for an ASIN.

# Let's add a global or a class member to track attempts per ASIN if we want to be more explicit,
# but for now, just more detailed logging within the function might reveal multiple calls.

# We will add a specific log at the very start of the function call.
# If the @retry decorator calls this function multiple times for the same ASIN,
# we will see this log message repeated for that ASIN.

@retry(stop_max_attempt_number=3, wait_fixed=10000)
def fetch_product(asin, days=365, offers=100, rating=1, history=1):
    global current_available_tokens # Moved to top of function for all reads/writes
    # Increment and log attempt number for this ASIN
    # This requires a way to store attempt counts across calls triggered by @retry for the same ASIN.
    # A simple global dictionary can serve this purpose for now.
    global fetch_product_attempts
    if asin not in fetch_product_attempts:
        fetch_product_attempts[asin] = 0
    fetch_product_attempts[asin] += 1
    attempt_num = fetch_product_attempts[asin]

    logger.info(f"fetch_product: Attempt #{attempt_num} for ASIN {asin} (days={days}, offers={offers}, rating={rating}, history={history}, no_cache={args.no_cache})")

    if not validate_asin(asin):
        # Reset attempt count for this ASIN if it fails validation before any actual attempt
        fetch_product_attempts[asin] = 0 # Or handle as needed - this error is pre-API call
        logging.error(f"Invalid ASIN format: {asin}")
        print(f"Invalid ASIN format: {asin}")
        # Consistent return for validation failure
        rate_limit_info_on_error = {'limit': None, 'remaining': None, 'reset': None, 'error_status_code': 'VALIDATION_ERROR'}
        return {'stats': {'current': [-1] * 30}, 'asin': asin, 'error': True, 'status_code': 'VALIDATION_ERROR', 'message': 'Invalid ASIN format'}, rate_limit_info_on_error
    
    # print(f"Fetching ASIN {asin}...") # Replaced by logger above
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}&stats={days}&offers={offers}&rating={rating}&history={history}&stock=1&buybox=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}
    try:
        logger.debug(f"fetch_product (Attempt #{attempt_num}): Making HTTP GET request for ASIN {asin} to {url}")
        response = requests.get(url, headers=headers, timeout=60)
        
        logger.debug(f"fetch_product (Attempt #{attempt_num}): Response status {response.status_code} for ASIN {asin}")
        # Removed logging of ALL RESPONSE HEADERS as Keepa is confirmed not to send rate limit headers we were looking for.
        response.raise_for_status() # Will raise HTTPError for 4xx/5xx, which is a RequestException

        # If raise_for_status() doesn't raise, then status_code is 200 or similar non-error
        fetch_product_attempts[asin] = 0 # Reset on success
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"No product data for ASIN {asin} despite 2xx status.")
            print(f"No product data for ASIN {asin} despite 2xx status.")
            rate_limit_info_on_error = { # Simplified rate_limit_info
                'limit': None, 'remaining': None, 'reset': None,
                'error_status_code': response.status_code
            }
            return {'stats': {'current': [-1] * 30}, 'asin': asin, 'error': True, 'status_code': response.status_code, 'message': 'No product data found in response'}, rate_limit_info_on_error
        
        product = products[0]
        stats = product.get('stats', {})
        current = stats.get('current', [-1] * 30)
        offers = product.get('offers', []) if product.get('offers') is not None else []
        # Reduced verbosity for INFO log, moved raw stats to DEBUG
        logging.info(f"HTTP Stats for ASIN {asin}: Found product data. current_array_length={len(current)}, offers_count={len(offers)}. Stat keys: {list(stats.keys())}")
        logger.debug(f"HTTP Stats for ASIN {asin}: current_data={current}, stats_raw={stats}")
        if len(current) < 11:
            logging.warning(f"Short current array for ASIN {asin}: {current}")
        if current[1] == -1:
            logging.warning(f"Invalid Amazon - Current price for ASIN {asin}: current[1]={current[1]}")
        
        # Simplified rate_limit_info as Keepa does not send these headers.
        rate_limit_info = {'limit': None, 'remaining': None, 'reset': None}
        
        # All logic related to parsing Keepa's x-rate-limit-* headers,
        # token discrepancy checks, and dynamic token adjustments has been removed
        # as it's confirmed Keepa does not provide these headers.

        return product, rate_limit_info
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Fetch failed for ASIN {asin}: {str(e)}")
        print(f"HTTP Fetch failed for ASIN {asin}: {str(e)}")
        status_code = e.response.status_code if e.response is not None else None
        # Simplified rate_limit_info for errors
        rate_limit_info = {
            'limit': None, 'remaining': None, 'reset': None,
            'error_status_code': status_code
        }
        # Removed specific logging for x-rate-limit-remaining on 429, as it's not expected.
        # General 429 logging remains useful.
        if status_code == 429:
             logger.error(f"ASIN {asin} - 429 ERROR. Script tokens at time of call: {current_available_tokens:.2f}.")
        return {'stats': {'current': [-1] * 30}, 'asin': asin, 'error': True, 'status_code': status_code}, rate_limit_info
    except Exception as e:
        logging.error(f"Generic Fetch failed for ASIN {asin}: {str(e)}")
        print(f"Generic Fetch failed for ASIN {asin}: {str(e)}")
        # For other exceptions, we might not have response headers
        rate_limit_info = {'limit': None, 'remaining': None, 'reset': None, 'error_status_code': None}
        return {'stats': {'current': [-1] * 30}, 'asin': asin, 'error': True, 'status_code': None}, rate_limit_info

# --- Jules: Quota Management Function ---
def update_and_check_quota(logger_instance):
    """
    Updates the available token count based on hourly refill and pauses if tokens are low.
    Uses and modifies global variables: current_available_tokens, last_refill_calculation_time.
    """
    global current_available_tokens
    global last_refill_calculation_time

    # Log entry state immediately
    logger_instance.debug(f"Quota Check (entry): Current available tokens: {current_available_tokens:.2f}, Last refill calc time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_refill_calculation_time))}")

    current_time = time.time()
    time_elapsed_seconds = current_time - last_refill_calculation_time
    
    if time_elapsed_seconds >= QUOTA_REFILL_INTERVAL_SECONDS:
        intervals_passed = int(time_elapsed_seconds // QUOTA_REFILL_INTERVAL_SECONDS)
        
        if intervals_passed > 0:
            refill_amount_per_interval = MAX_QUOTA_TOKENS * HOURLY_REFILL_PERCENTAGE
            total_refilled = intervals_passed * refill_amount_per_interval
            
            # Store tokens before refill for logging
            tokens_before_refill = current_available_tokens
            current_available_tokens += total_refilled
            if current_available_tokens > MAX_QUOTA_TOKENS:
                current_available_tokens = MAX_QUOTA_TOKENS
            
            # Advance last_refill_calculation_time by the exact number of intervals processed
            original_last_refill_time = last_refill_calculation_time
            last_refill_calculation_time += intervals_passed * QUOTA_REFILL_INTERVAL_SECONDS
            
            logger_instance.info(
                f"Quota Refill: Passed {intervals_passed} hour(s) since last calc (from {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(original_last_refill_time))}). "
                f"Tokens before: {tokens_before_refill:.2f}. Added {total_refilled:.2f}. Tokens after: {current_available_tokens:.2f}. "
                f"New last refill calc time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_refill_calculation_time))}"
            )
        # If no refill happened, this debug log confirms current token state before threshold check
        else:
            logger_instance.debug(f"Quota Check: No refill interval passed. Current tokens: {current_available_tokens:.2f}")
    
    logger_instance.debug(f"Quota Check (after refill calc): Current available tokens: {current_available_tokens:.2f}") # Changed to debug

    # Proactive Pause Logic
    if current_available_tokens < MIN_QUOTA_THRESHOLD_BEFORE_PAUSE:
        logger_instance.warning(
            f"Low quota: {current_available_tokens:.2f} tokens remaining, which is below threshold {MIN_QUOTA_THRESHOLD_BEFORE_PAUSE}. "
            f"Pausing for {DEFAULT_LOW_QUOTA_PAUSE_SECONDS / 60:.1f} minutes."
        )
        time.sleep(DEFAULT_LOW_QUOTA_PAUSE_SECONDS)
        
        logger_instance.info(f"Quota: Pause complete. Attempting to re-check quota and potential refills...")
        update_and_check_quota(logger_instance) # Recursive call to re-evaluate after pause
    
    # This function doesn't return anything; it modifies globals and may pause.
# --- End Quota Management Function ---

# --- Jules: Batch Product Fetch Function ---
@retry(stop_max_attempt_number=3, wait_fixed=15000) # Increased wait for batch calls
def fetch_product_batch(asins_list, days=365, offers=100, rating=1, history=1):
    global current_available_tokens # For logging current token state if 429 occurs
    
    if not asins_list:
        logger.warning("fetch_product_batch called with an empty list of ASINs.")
        return [], {'requestTokens': 0, 'tokensLeft': None, 'refillIn': None, 'refillRate': None, 'error_status_code': 'EMPTY_ASIN_LIST'}, 0

    logger.info(f"fetch_product_batch: Attempting to fetch batch of {len(asins_list)} ASINs: {','.join(asins_list[:3])}...")

    # ASIN validation should ideally happen before forming batches, but double-check here if necessary.
    # For now, assuming valid ASINs are passed.

    comma_separated_asins = ','.join(asins_list)
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={comma_separated_asins}&stats={days}&offers={offers}&rating={rating}&history={history}&stock=1&buybox=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'} # Standard User-Agent

    try:
        logger.debug(f"fetch_product_batch: Making HTTP GET request for {len(asins_list)} ASINs to {url}")
        response = requests.get(url, headers=headers, timeout=120) # Increased timeout for batch calls
        
        logger.debug(f"fetch_product_batch: Response status {response.status_code} for ASINs {','.join(asins_list[:3])}...")
        response.raise_for_status()

        data = response.json()
        
        # Extract token and rate limit information if available
        api_info = {
            'requestTokens': data.get('requestTokens'), # This is the key field we need to check
            'tokensLeft': data.get('tokensLeft'),     # Not expected for this endpoint
            'refillIn': data.get('refillIn'),         # Not expected
            'refillRate': data.get('refillRate'),       # Not expected
            'error_status_code': None
        }
        
        actual_token_cost = 0
        if api_info['requestTokens'] is not None:
            actual_token_cost = int(api_info['requestTokens'])
            logger.info(f"Batch API call cost {actual_token_cost} tokens according to 'requestTokens' field.")
        else:
            # Estimate cost if not provided - this will be refined based on user testing
            actual_token_cost = len(asins_list) * TOKEN_COST_PER_ASIN # Fallback estimation
            logger.warning(f"'requestTokens' field not found in batch response. Using estimated cost: {actual_token_cost} tokens.")

        products_data = data.get('products', [])
        if not products_data and len(asins_list) > 0:
            logger.error(f"No product data in batch response for ASINs {','.join(asins_list[:3])}... despite 2xx status.")
            # Return a list of error objects, one for each ASIN requested in the batch
            error_products = [{'asin': asin, 'error': True, 'status_code': response.status_code, 'message': 'No product data found in batch response'} for asin in asins_list]
            return error_products, api_info, actual_token_cost
        
        # TODO: Potentially map products back to original ASINs if order is not guaranteed,
        # or if some ASINs in the request might be missing from the response.
        # For now, assuming the 'products' array corresponds to the requested ASINs.
        # If an ASIN in the request yields no data from Keepa, it might just be omitted from the 'products' array.
        # We need to ensure that the main loop can handle this (e.g. by creating placeholders for missing ASINs).

        logger.info(f"Successfully fetched data for {len(products_data)} products in batch for {len(asins_list)} requested ASINs.")
        return products_data, api_info, actual_token_cost

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP Fetch failed for batch ASINs {','.join(asins_list[:3])}...: {str(e)}")
        status_code = e.response.status_code if e.response is not None else None
        api_info_on_error = {'requestTokens': None, 'tokensLeft': None, 'refillIn': None, 'refillRate': None, 'error_status_code': status_code}
        
        estimated_cost_on_error = len(asins_list) * TOKEN_COST_PER_ASIN # Use estimation for cost logging on error
        if status_code == 429:
             logger.error(f"Batch ASINs - 429 ERROR. Script tokens at time of call: {current_available_tokens:.2f}.")
        
        # Return a list of error objects for each ASIN in the batch
        error_products = [{'asin': asin, 'error': True, 'status_code': status_code, 'message': str(e)} for asin in asins_list]
        return error_products, api_info_on_error, estimated_cost_on_error # Return estimated cost for accounting purposes

    except Exception as e:
        logger.error(f"Generic Fetch failed for batch ASINs {','.join(asins_list[:3])}...: {str(e)}")
        api_info_on_error = {'requestTokens': None, 'tokensLeft': None, 'refillIn': None, 'refillRate': None, 'error_status_code': None}
        estimated_cost_on_error = len(asins_list) * TOKEN_COST_PER_ASIN
        error_products = [{'asin': asin, 'error': True, 'status_code': 'GENERIC_ERROR', 'message': str(e)} for asin in asins_list]
        return error_products, api_info_on_error, estimated_cost_on_error
# --- End Batch Product Fetch Function ---


# Chunk 2 ends

# Global args variable, to be initialized in main
args = None
# The old current_rate_limit_info global and its constants (RATE_LIMIT_REMAINING_THRESHOLD, MIN_TIME_BETWEEN_HEADER_CHECKS_SECONDS)
# are no longer needed with the new local quota system. They are removed.

# Chunk 3 starts
def write_csv(rows, deals, diagnostic=False):
    logger.info(f"Entering write_csv. Number of deals to process: {len(deals)}. Number of rows generated: {len(rows)}.")
    if len(deals) != len(rows) and not diagnostic:
        logger.warning(f"Mismatch in write_csv: len(deals) is {len(deals)} but len(rows) is {len(rows)}. CSV might be incomplete or misaligned.")

    try:
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS) # HEADERS is global
            if diagnostic:
                writer.writerow(['No deals fetched'] + ['-'] * (len(HEADERS) - 1))
                logger.info(f"Diagnostic CSV written: Keepa_Deals_Export.csv")
                print(f"Diagnostic CSV written: Keepa_Deals_Export.csv")
            else:
                # Ensure we only try to zip up to the shorter of the two lists if there's a mismatch,
                # though ideally they should be the same length due to placeholder logic.
                num_to_write = min(len(deals), len(rows))
                if len(deals) != len(rows): # Log if we are truncating due to mismatch
                     logger.warning(f"write_csv: Writing {num_to_write} rows due to length mismatch between deals ({len(deals)}) and rows ({len(rows)}).")

                for i in range(num_to_write):
                    deal_obj = deals[i]
                    row_content = rows[i]
                    asin_from_deal = deal_obj.get('asin', 'UNKNOWN_DEAL_ASIN')
                    asin_from_row = row_content.get('ASIN', 'UNKNOWN_ROW_ASIN')

                    # Log a summary of the row being written - changed to DEBUG
                    non_hyphen_row_items = {k: v for k, v in row_content.items() if v != '-'}
                    logger.debug(f"Writing CSV row for ASIN (from deal obj): {asin_from_deal}, ASIN (from row obj): {asin_from_row}. Non-hyphen count: {len(non_hyphen_row_items)}. Keys: {list(non_hyphen_row_items.keys())}")
                    if asin_from_deal != asin_from_row and asin_from_row not in asin_from_deal : # Check if row ASIN is a placeholder like INVALID_ASIN_SKIPPED_...
                        logger.warning(f"ASIN mismatch when writing CSV: Deal ASIN is '{asin_from_deal}', Row ASIN is '{asin_from_row}'.")

                    try:
                        # row_data = row_content.copy() # No need to copy if we're just reading
                        # missing_headers = [h for h in HEADERS if h not in row_data] # Not strictly necessary if row_data.get is used
                        # if missing_headers:
                        #     logger.warning(f"Missing headers in row_content for ASIN {asin_from_row}: {missing_headers[:5]}")
                        
                        # print(f"Writing row for ASIN {asin_from_row}...") # Console print might be too verbose now
                        writer.writerow([row_content.get(header, '-') for header in HEADERS])
                        # logger.debug(f"Wrote row to CSV for ASIN {asin_from_row}") # Can be very verbose
                    except Exception as e:
                        logger.error(f"Failed to write row to CSV for ASIN {asin_from_row} (from deal: {asin_from_deal}): {str(e)}")
                        # Optionally, write a row of hyphens for this failed write
                        # writer.writerow([asin_from_row_or_deal] + ['ERROR_WRITING_ROW'] * (len(HEADERS)-1) )
        
        logger.info(f"CSV written: Keepa_Deals_Export.csv with {num_to_write if not diagnostic else 0} data rows.")
        print(f"CSV written: Keepa_Deals_Export.csv")
    except Exception as e:
        logger.error(f"Failed to write CSV Keepa_Deals_Export.csv: {str(e)}")
        print(f"Failed to write CSV Keepa_Deals_Export.csv: {str(e)}")
# Chunk 3 ends

# Chunk 4 starts
def main():
    global args, LAST_API_CALL_TIMESTAMP # Added LAST_API_CALL_TIMESTAMP to globals used in main
    args = parser.parse_args() # Initialize global args
    logger = logging.getLogger('KeepaDeals') # Obtain logger instance
    
    LAST_API_CALL_TIMESTAMP = time.time() # Initialize to script start time, or 0 if preferred to ensure first call isn't delayed by this. Let's use current time.

    try:
# Logging stuff - starts
        logger.info("Starting Keepa_Deals...") # Use logger instance
        print("Starting Keepa_Deals...", flush=True)
        time.sleep(2)
        
        all_deals = []
        page = 0
        while True:
            logger.info(f"Fetching deals page {page}...")
            print(f"Fetching deals page {page}...", flush=True)
            deals_page = fetch_deals_for_deals(page) # Consider passing args.no_cache if fetch_deals_for_deals needs it
            if not deals_page:
                logger.info(f"No more deals found on page {page}.")
                print(f"No more deals found on page {page}.", flush=True)
                break
            all_deals.extend(deals_page)
            logger.info(f"Fetched {len(deals_page)} deals from page {page}. Total deals so far: {len(all_deals)}")
            print(f"Fetched {len(deals_page)} deals from page {page}. Total deals so far: {len(all_deals)}", flush=True)
            page += 1
            time.sleep(1) # Add a small delay between page fetches if needed

        deals = all_deals # Use all_deals for processing
        
        # TEMPORARY: Limit to 100 deals for faster testing
        MAX_DEALS_TO_PROCESS_FOR_TESTING = 10
        if len(deals) > MAX_DEALS_TO_PROCESS_FOR_TESTING:
            logger.warning(f"TEMPORARY TEST LIMIT: Processing only the first {MAX_DEALS_TO_PROCESS_FOR_TESTING} of {len(deals)} deals.")
            print(f"TEMPORARY TEST LIMIT: Processing only the first {MAX_DEALS_TO_PROCESS_FOR_TESTING} of {len(deals)} deals.", flush=True)
            deals_to_process = deals[:MAX_DEALS_TO_PROCESS_FOR_TESTING]
        else:
            deals_to_process = deals
        # END TEMPORARY LIMIT

        rows = []
        if not deals_to_process: # Check deals_to_process instead of deals
            logger.warning("No deals fetched or all filtered out by temporary limit, writing diagnostic CSV") # Use logger instance
            print("No deals fetched, writing diagnostic CSV", flush=True)
            write_csv([], [], diagnostic=True)
            return
        logger.debug(f"Deals ASINs: {[d.get('asin', '-') for d in deals_to_process[:5]]}") # Use logger instance, refer to deals_to_process
        print(f"Deals ASINs: {[d.get('asin', '-') for d in deals_to_process[:5]]}", flush=True)
        logger.info(f"Starting ASIN processing, found {len(deals_to_process)} deals (after potential temporary limit)") # Use logger instance
        print(f"Starting ASIN processing, found {len(deals_to_process)} deals (after potential temporary limit)", flush=True)
# Logging stuff - ends

        # --- Batch Processing Logic ---
        MAX_ASINS_PER_BATCH = 100 # Keepa API limit for product endpoint
        
        valid_deals_to_process = []
        for deal_idx, deal_obj in enumerate(deals_to_process):
            asin = deal_obj.get('asin', '-')
            if not validate_asin(asin):
                logger.warning(f"Skipping invalid ASIN '{asin}' from deal object: {deal_obj}")
                # Add placeholder for invalid ASIN to maintain row count alignment with original deals_to_process list
                placeholder_row = {'ASIN': f"INVALID_ASIN_SKIPPED_{asin[:10]}"}
                for header_key in HEADERS:
                    if header_key not in placeholder_row:
                        placeholder_row[header_key] = '-'
                rows.append(placeholder_row) # Add placeholder to the final rows list
            else:
                # Store the original deal object with its index for later association
                valid_deals_to_process.append({'original_index': deal_idx, 'asin': asin, 'deal_obj': deal_obj})

        logger.info(f"Collected {len(valid_deals_to_process)} valid ASINs for batch processing.")

        # Create batches of ASINs
        asin_batches = []
        for i in range(0, len(valid_deals_to_process), MAX_ASINS_PER_BATCH):
            batch_deals = valid_deals_to_process[i:i + MAX_ASINS_PER_BATCH]
            asin_batches.append(batch_deals)

        logger.info(f"Created {len(asin_batches)} batches for API calls.")

        all_fetched_products_map = {} # To store fetched product data by ASIN

        for batch_idx, current_batch_deals in enumerate(asin_batches):
            batch_asins = [d['asin'] for d in current_batch_deals]
            logger.info(f"Processing Batch {batch_idx + 1}/{len(asin_batches)} with {len(batch_asins)} ASINs: {batch_asins}")

            # --- Quota Management & Throttling (Per Batch) ---
            update_and_check_quota(logger)
            
            current_time = time.time()
            time_since_last_call = current_time - LAST_API_CALL_TIMESTAMP
            if time_since_last_call < MIN_TIME_SINCE_LAST_CALL_SECONDS: # This constant might need adjustment for batch calls
                wait_duration = MIN_TIME_SINCE_LAST_CALL_SECONDS - time_since_last_call
                logger.info(f"Pre-emptive pause for batch: Last call was {time_since_last_call:.2f}s ago. Waiting for {wait_duration:.2f}s.")
                time.sleep(wait_duration)

            # Call the actual batch fetching function
            # Parameters like days, offers, etc., are passed with default values for now.
            # These can be made dynamic if needed per batch.
            batch_product_data_list, api_info, actual_batch_cost = fetch_product_batch(batch_asins)
            
            LAST_API_CALL_TIMESTAMP = time.time()
            global current_available_tokens

            # Check if the batch fetch itself had a critical error (e.g., HTTP error)
            # fetch_product_batch returns a list of error-like dicts if the whole call fails.
            # A more robust check might be needed if partial success is possible at HTTP level.
            # For now, if the first item has a 'status_code' that's not None and not 200, assume batch failure.
            # Or, if api_info indicates an error status code directly from the batch call.
            
            batch_had_critical_error = False
            if api_info.get('error_status_code') and api_info.get('error_status_code') != 200:
                batch_had_critical_error = True
                logger.error(f"Batch API call for ASINs {batch_asins[:3]}... failed with status code: {api_info.get('error_status_code')}.")

            if not batch_had_critical_error:
                current_available_tokens -= actual_batch_cost
                logger.info(f"Tokens consumed for BATCH. Cost: {actual_batch_cost}. Tokens remaining: {current_available_tokens:.2f}.")
                logger.debug(f"Pausing for {POST_BATCH_SUCCESS_DELAY_SECONDS}s after successful batch fetch.")
                time.sleep(POST_BATCH_SUCCESS_DELAY_SECONDS)
            else:
                logger.error(f"Batch fetch critically failed for ASINs: {batch_asins[:3]}... Token NOT consumed by main loop (cost was {actual_batch_cost}, error status: {api_info.get('error_status_code')}).")
                logger.debug(f"Pausing for {POST_BATCH_ERROR_DELAY_SECONDS}s after failed batch fetch.")
                time.sleep(POST_BATCH_ERROR_DELAY_SECONDS)
                # Populate all_fetched_products_map with error objects for this batch
                for deal_info in current_batch_deals:
                    # Use the error structure returned by fetch_product_batch if available, else generic
                    asin_error_obj = next((p for p in batch_product_data_list if isinstance(p, dict) and p.get('asin') == deal_info['asin']), None)
                    if asin_error_obj and asin_error_obj.get('error'):
                        all_fetched_products_map[deal_info['asin']] = asin_error_obj
                    else: # Generic error if specific one not found (should not happen if fetch_product_batch is consistent)
                        all_fetched_products_map[deal_info['asin']] = {'asin': deal_info['asin'], 'error': True, 'status_code': api_info.get('error_status_code', 'BATCH_CALL_FAILED'), 'message': 'Batch API call failed'}
                continue # Move to the next batch

            # Store successfully fetched or individually errored (but batch call was OK) products in the map
            # The batch_product_data_list might contain a mix if Keepa processes some ASINs and errors on others within a 200 OK response.
            # Or, if fetch_product_batch synthesizes error objects for ASINs not found in a successful response.
            temp_product_map = {p['asin']: p for p in batch_product_data_list if isinstance(p, dict) and 'asin' in p}

            for deal_info in current_batch_deals:
                asin_to_map = deal_info['asin']
                if asin_to_map in temp_product_map:
                    all_fetched_products_map[asin_to_map] = temp_product_map[asin_to_map]
                else:
                    # This ASIN was in the request but not in the response products list from a (nominally) successful batch call
                    logger.warning(f"ASIN {asin_to_map} was requested in batch but not found in response products. Marking as error.")
                    all_fetched_products_map[asin_to_map] = {'asin': asin_to_map, 'error': True, 'status_code': 'MISSING_IN_BATCH_RESPONSE', 'message': 'ASIN not found in successful batch response products list.'}
            
            # Handle 429 specifically (though fetch_product_batch also logs it)
            # This check might be redundant if batch_had_critical_error covers it.
            if api_info.get('error_status_code') == 429:
                logger.error(f"Received 429 (Too Many Requests) for BATCH {batch_asins[:3]}... Initiating 1-hour recovery pause.")
                # This pause might be better handled within fetch_product_batch or as part of general error handling for batch.
                # For now, keeping a similar pattern.
                time.sleep(QUOTA_REFILL_INTERVAL_SECONDS) 
                update_and_check_quota(logger)


        # --- Process all deals using the fetched product data ---
        # Iterate through the original deals_to_process to maintain order and include placeholders for skipped ASINs
        temp_rows_data = [] # Temporary list to hold processed row data with original indices

        for deal_info in valid_deals_to_process: # These are only the deals for which we attempted a fetch
            original_deal_obj = deal_info['deal_obj']
            asin = deal_info['asin']
            
            product = all_fetched_products_map.get(asin)

            if not product or product.get('error'): # This now correctly catches errors from all_fetched_products_map
                logger.error(f"Incomplete or error in product data for ASIN {asin}. Product: {product}")
                placeholder_row_content = {'ASIN': asin}
                for header_key in HEADERS:
                    if header_key not in placeholder_row_content:
                        placeholder_row_content[header_key] = '-'
                temp_rows_data.append({'original_index': deal_info['original_index'], 'data': placeholder_row_content})
                continue

            # Jules: Modified for debugging FBA Pick&Pack Fee - Log raw product data for a specific ASIN
            TEST_ASIN_FOR_RAW_LOG = '1562243179' # Target ASIN for raw data logging
            if asin == TEST_ASIN_FOR_RAW_LOG:
                if product and isinstance(product, dict) and not product.get('error'):
                    logger.info(f"RAW_PRODUCT_DATA_{asin}: {json.dumps(product)}")
                else:
                    logger.info(f"RAW_PRODUCT_DATA_{asin}: Product data error/missing for raw log. Data: {product}")
            
            # Logging for Last Used price update from product_data (already adapted for product structure)
            try:
                # The product structure from batch might be directly the item, not nested under 'products'[0] like single fetch.
                # Adjusting path if product is the direct item from batch.
                # fetch_product_batch returns a list of product items.
                # The 'product' variable here IS one of those items.
                if product and isinstance(product, dict) and \
                   'csv' in product and isinstance(product['csv'], list) and \
                   len(product['csv']) > 2 and \
                   isinstance(product['csv'][2], list) and \
                   len(product['csv'][2]) > 0:
                    last_used_entry = product['csv'][2][-1]
                    if isinstance(last_used_entry, list) and len(last_used_entry) > 0:
                        logger.info(f"ASIN: {asin} - Last Used price update from product_data.csv[2]: {last_used_entry[0]}")
                # Warnings for missing paths handled by individual functions or get_stat_value
            except (KeyError, IndexError, TypeError) as e:
                 logger.warning(f"ASIN: {asin} - Error accessing product_data.csv[2] for Used price: {e}")

            row = {}
            try:
                for header, func in zip(HEADERS, FUNCTION_LIST):
                    if func:
                        try:
                            input_data_for_func = product # Default to product data
                            if header in ['Deal found', 'last update', 'last price change']:
                                # These functions need the original deal object and the fetched product data
                                result = func(original_deal_obj, config, logger, product)
                            elif header == 'Percent Down 90': # Example: if it needs deal_obj and product
                                result = func(product) # Assuming it's updated to only need product or deal is merged in
                            else: # Most functions take only product data
                                result = func(input_data_for_func)
                            
                            logger.debug(f"ASIN {asin}, Header: {header}, Func: {func.__name__}, Result: {result}")
                            row.update(result)
                        except Exception as e:
                            logger.error(f"Function {func.__name__} failed for ASIN {asin}, header '{header}': {e}")
                            row[header] = '-'
                
                non_hyphen_items = {k: v for k, v in row.items() if v != '-'}
                logger.debug(f"ASIN {asin}: PRE-APPEND main row. Non-hyphen count: {len(non_hyphen_items)}. Keys: {list(non_hyphen_items.keys())}")
                if not non_hyphen_items and asin == product.get('asin'):
                    logger.warning(f"ASIN {asin}: Row for valid product is all hyphens. Error: {product.get('error')}, Status: {product.get('status_code')}")
                
                temp_rows_data.append({'original_index': deal_info['original_index'], 'data': row})

            except Exception as e:
                logger.error(f"Error processing ASIN {asin} (outer loop): {e}")
                placeholder_row_content = {'ASIN': asin}
                else:
                    logger.error(f"Invalid product data structure in batch response: {product_data}")


        # --- Process all deals using the fetched product data ---
        # Iterate through the original deals_to_process to maintain order and include placeholders for skipped ASINs
        temp_rows_data = [] # Temporary list to hold processed row data with original indices

        for deal_info in valid_deals_to_process: # These are only the deals for which we attempted a fetch
            original_deal_obj = deal_info['deal_obj']
            asin = deal_info['asin']
            
            product = all_fetched_products_map.get(asin)

            if not product or product.get('error'):
                logger.error(f"Failed to fetch or error in product data for ASIN {asin}. Product: {product}")
                placeholder_row_content = {'ASIN': asin}
                for header_key in HEADERS:
                    if header_key not in placeholder_row_content:
                        placeholder_row_content[header_key] = '-'
                temp_rows_data.append({'original_index': deal_info['original_index'], 'data': placeholder_row_content})
                continue

            # Jules: Modified for debugging FBA Pick&Pack Fee - Log raw product data for a specific ASIN
            TEST_ASIN_FOR_RAW_LOG = '1562243179' # Target ASIN for raw data logging
            if asin == TEST_ASIN_FOR_RAW_LOG:
                if product and isinstance(product, dict) and not product.get('error'):
                    logger.info(f"RAW_PRODUCT_DATA_{asin}: {json.dumps(product)}")
                else:
                    logger.info(f"RAW_PRODUCT_DATA_{asin}: Product data error/missing for raw log. Data: {product}")
            
            # Logging for Last Used price update from product_data (already adapted for product structure)
            try:
                if product and isinstance(product, dict) and \
                   product.get('products') and isinstance(product['products'], list) and \
                   len(product['products']) > 0 and isinstance(product['products'][0], dict) and \
                   'csv' in product['products'][0] and isinstance(product['products'][0]['csv'], list) and \
                   len(product['products'][0]['csv']) > 2 and \
                   isinstance(product['products'][0]['csv'][2], list) and \
                   len(product['products'][0]['csv'][2]) > 0:
                    
                    # Get the last entry from the "Used" price history (index 2 for USED)
                    last_used_entry = product['products'][0]['csv'][2][-1]
                    if isinstance(last_used_entry, list) and len(last_used_entry) > 0:
                        last_used_price_ts_minutes = last_used_entry[0]
                        logger.info(f"ASIN: {asin} - Last Used price update from product_data.csv[2]: {last_used_price_ts_minutes}")
                    else:
                        logger.warning(f"ASIN: {asin} - Last Used price entry in product_data.csv[2] is not a valid list or is empty.")
                else:
                    logger.warning(f"ASIN: {asin} - Could not retrieve valid product_data.csv[2] path for Used price history.")
            except (KeyError, IndexError, TypeError) as e:
                logger.warning(f"ASIN: {asin} - Could not retrieve last Used price update from product_data.csv[2]. Error: {type(e).__name__} - {e}")
            
            row = {}
            try:
                # Process all functions using FUNCTION_LIST
                for header, func in zip(HEADERS, FUNCTION_LIST):
                    if func:
                        try:
                            # Determine input_data based on header
                            if header in ['Deal found', 'last update', 'last price change']:
                                input_data = deal
                            else:
                                input_data = product

                            # Call func with appropriate arguments
                            if header == 'last update' or header == 'last price change': # Modified condition
                                # input_data is 'deal', 'product' is the fetched product data
                                # Both 'last update' and 'last price change' now expect product_data
                                result = func(input_data, config, logger, product)
                            elif header == 'Deal found':
                                # input_data is 'deal'
                                result = func(input_data, config, logger)
                            else: # For all other functions, input_data is 'product'
                                result = func(input_data)
                                
                            logger.debug(f"Header: {header}, Function: {func.__name__}, Result: {result}, Row before: {row}") # Use logger instance
                            row.update(result)
                            logger.debug(f"Row after update for {header}: {row}") # Use logger instance
                        except Exception as e:
                            logger.error(f"Function {func.__name__} failed for ASIN {asin} processing header '{header}': {str(e)}") # Use logger instance
                            row[header] = '-'
                
                # Detailed logging before appending the main data row - changed to DEBUG
                non_hyphen_items = {k: v for k, v in row.items() if v != '-'}
                logger.debug(f"ASIN {asin}: PRE-APPEND main row. Non-hyphen count: {len(non_hyphen_items)}. Keys: {list(non_hyphen_items.keys())}")
                if not non_hyphen_items and asin == product.get('asin'): # If row is all hyphens but product was supposed to be valid
                    logger.warning(f"ASIN {asin}: Row for a seemingly valid product is all hyphens before append. Product error flag: {product.get('error')}, Product status: {product.get('status_code')}")

                rows.append(row)
                logger.debug(f"ASIN {asin}: POST-APPEND main row. `rows` list length: {len(rows)}") # Changed to DEBUG

            except Exception as e:
                logger.error(f"Error processing ASIN {asin} (outer loop): {str(e)}") # Use logger instance
                # Ensure a placeholder is added if this generic error occurs for an ASIN
                # so row count matches deal count for deals_to_process
                placeholder_row = {'ASIN': asin}
                for header_key in HEADERS:
                    if header_key not in placeholder_row:
                        placeholder_row[header_key] = '-'
                rows.append(placeholder_row)
                continue
        write_csv(rows, deals_to_process) # Use deals_to_process here
        logger.info("Writing CSV...") # Use logger instance
        print("Writing CSV...")
        logger.info("Script completed!") # Use logger instance
        print("Script completed!")
        print(f"Processed ASINs: {[row.get('ASIN', '-') for row in rows]}")
    except Exception as e:
        logger.error(f"Main failed: {str(e)}") # Use logger instance
        print(f"Main failed: {str(e)}")
        sys.exit(1)
# Chunk 4 ends

if __name__ == "__main__":
    main()

#### END of Keepa_Deals.py ####