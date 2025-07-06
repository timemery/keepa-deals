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
MAX_QUOTA_TOKENS = 300 # This might be dynamically updated if API provides it, but keep as a fallback/default.
HOURLY_REFILL_PERCENTAGE = 0.05 # This is a client-side estimation, API's refillRate might be more accurate.
# TOKEN_COST_PER_ASIN = 2 # No longer used for decrementing; cost is informational from batch response.
MIN_QUOTA_THRESHOLD_BEFORE_PAUSE = 25 # Increased from 10 to 25 for a larger buffer
QUOTA_REFILL_INTERVAL_SECONDS = 3600  # 1 hour (used for client-side refill estimate if API data is missing)
DEFAULT_LOW_QUOTA_PAUSE_SECONDS = 900 # 15 minutes (remains the same for now)

# Initialize global state variables for quota management
# These will be modified by functions and within the main loop.
# Consider refactoring to a class if state management becomes too complex.
current_available_tokens = MAX_QUOTA_TOKENS
last_refill_calculation_time = time.time() # Initialize to script start time

# Global dictionary to track attempts for fetch_product retries
fetch_product_attempts = {}

# --- Jules: Additional Throttling & Logging Constants (Old - To be removed or revised) ---
# MIN_TIME_SINCE_LAST_CALL_SECONDS = 60 # Replaced by MIN_TIME_BETWEEN_BATCH_CALLS_SECONDS
# POST_FETCH_SUCCESS_DELAY_SECONDS = 5  # No longer primary mechanism
# POST_FETCH_ERROR_DELAY_SECONDS = 20 # No longer primary mechanism
# Initialize global state for pre-emptive delay
LAST_API_CALL_TIMESTAMP = 0 # Timestamp of the last API (batch) call
# --- End Additional Throttling & Logging Constants ---

# --- Jules: Batch Processing Constants ---
BATCH_SIZE = 100 # Max ASINs per batch call (Keepa supports up to 100)
MIN_TIME_BETWEEN_BATCH_CALLS_SECONDS = 30 # Initial conservative delay between batch calls
# --- End Batch Processing Constants ---
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

# @retry(stop_max_attempt_number=3, wait_fixed=10000) # Single ASIN fetch_product is no longer used.
# def fetch_product(asin, days=365, offers=100, rating=1, history=1): # ... entire old function removed ...
#     pass # Placeholder for removed function

# --- Jules: Quota Management Function (Revised for Batch Processing) ---
def update_and_check_quota(logger_instance):
    """
    Checks available token count and pauses if tokens are low.
    Relies on `current_available_tokens` being updated from API responses primarily.
    The refill calculation here is a fallback/client-side estimation, mainly for long pauses.
    """
    global current_available_tokens
    global last_refill_calculation_time

    logger_instance.info(f"Quota Check (entry): Current available tokens: {current_available_tokens:.2f}, Last client-side refill calc time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_refill_calculation_time))}")

    # Client-side refill calculation (primarily for during long pauses like DEFAULT_LOW_QUOTA_PAUSE_SECONDS)
    # This is a less critical part now that tokensLeft from API is the main source of truth.
    current_time = time.time()
    time_elapsed_seconds = current_time - last_refill_calculation_time
    
    if time_elapsed_seconds >= QUOTA_REFILL_INTERVAL_SECONDS:
        intervals_passed = int(time_elapsed_seconds // QUOTA_REFILL_INTERVAL_SECONDS)
        if intervals_passed > 0:
            refill_amount_per_interval = MAX_QUOTA_TOKENS * HOURLY_REFILL_PERCENTAGE
            total_refilled_estimate = intervals_passed * refill_amount_per_interval
            
            tokens_before_client_refill = current_available_tokens
            current_available_tokens += total_refilled_estimate
            if current_available_tokens > MAX_QUOTA_TOKENS: # Cap at max quota
                current_available_tokens = MAX_QUOTA_TOKENS
            
            last_refill_calculation_time += intervals_passed * QUOTA_REFILL_INTERVAL_SECONDS
            logger_instance.info(
                f"Quota Refill (Client-side Est.): {intervals_passed} hour(s) passed since last client calc. "
                f"Tokens before est. refill: {tokens_before_client_refill:.2f}. Estimated add: {total_refilled_estimate:.2f}. Tokens after est.: {current_available_tokens:.2f}. "
                f"New last client-side refill calc time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_refill_calculation_time))}"
            )
    
    logger_instance.info(f"Quota Check (after potential client-side refill calc): Current available tokens: {current_available_tokens:.2f}")

    # Proactive Pause Logic (Primary role of this function now)
    if current_available_tokens < MIN_QUOTA_THRESHOLD_BEFORE_PAUSE:
        logger_instance.warning(
            f"Low quota: {current_available_tokens:.2f} tokens remaining, below threshold {MIN_QUOTA_THRESHOLD_BEFORE_PAUSE}. "
            f"Pausing for {DEFAULT_LOW_QUOTA_PAUSE_SECONDS / 60:.1f} minutes."
        )
        time.sleep(DEFAULT_LOW_QUOTA_PAUSE_SECONDS)
        
        # After the pause, recalculate client-side estimate for tokens that might have refilled *during* the pause.
        # The next API call will provide the authoritative `tokensLeft`.
        logger_instance.info(f"Quota: Pause complete. Recalculating client-side token estimate for time elapsed during pause.")
        
        time_elapsed_during_pause_and_more = time.time() - last_refill_calculation_time
        if time_elapsed_during_pause_and_more >= QUOTA_REFILL_INTERVAL_SECONDS:
            intervals_passed_during_pause = int(time_elapsed_during_pause_and_more // QUOTA_REFILL_INTERVAL_SECONDS)
            if intervals_passed_during_pause > 0:
                refill_during_pause_estimate = intervals_passed_during_pause * (MAX_QUOTA_TOKENS * HOURLY_REFILL_PERCENTAGE)
                tokens_before_pause_refill_est = current_available_tokens
                current_available_tokens += refill_during_pause_estimate
                if current_available_tokens > MAX_QUOTA_TOKENS: current_available_tokens = MAX_QUOTA_TOKENS
                last_refill_calculation_time += intervals_passed_during_pause * QUOTA_REFILL_INTERVAL_SECONDS
                logger_instance.info(
                    f"Quota: Client-side refill calculated for pause duration. Prev tokens: {tokens_before_pause_refill_est:.2f}. Estimated add: {refill_during_pause_estimate:.2f}. "
                    f"Tokens now (est.): {current_available_tokens:.2f}. New last client calc: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_refill_calculation_time))}"
                )
        logger_instance.info(f"Quota Check (post-pause, pre-API call): Current available tokens (client est.): {current_available_tokens:.2f}")
    # This function doesn't return anything; it modifies globals and may pause.
# --- End Quota Management Function (Revised for Batch Processing) ---

# --- Jules: New Batch Fetching Function ---
def fetch_product_batch(asin_list, api_key_param, logger_instance):
    """
    Fetches product data for a list of ASINs in a single batch API call.
    
    Args:
        asin_list (list): A list of ASIN strings.
        api_key_param (str): The Keepa API key.
        logger_instance (logging.Logger): Logger instance for logging.
        
    Returns:
        tuple: (list_of_product_data, tokens_left, refill_in_ms, refill_rate, request_tokens_cost)
               Returns ([], None, None, None, None) on failure.
    """
    if not asin_list:
        logger_instance.warning("fetch_product_batch called with an empty ASIN list.")
        return [], None, None, None, None

    asins_str = ",".join(asin_list)
    # Standard parameters from the single fetch_product function
    # stats=365, offers=100, rating=1, history=1, stock=1, buybox=1
    url = (f"https://api.keepa.com/product?key={api_key_param}&domain=1&asin={asins_str}"
           f"&stats=365&offers=100&rating=1&history=1&stock=1&buybox=1")

    logger_instance.info(f"fetch_product_batch: Requesting data for {len(asin_list)} ASINs: {asins_str[:100]}...") # Log first 100 chars of ASIN string
    logger_instance.debug(f"fetch_product_batch: URL: {url}")

    try:
        response = requests.get(url, headers={'User-Agent': 'Keepa_Deals_Batch/1.0', 'Accept-Encoding': 'gzip'}, timeout=90) # Increased timeout for batch
        logger_instance.debug(f"fetch_product_batch: Response status {response.status_code} for ASINs {asins_str[:50]}...")
        
        response.raise_for_status()  # Raises HTTPError for 4xx/5xx status codes

        data = response.json()
        
        products = data.get('products', [])
        tokens_left = data.get('tokensLeft')
        refill_in_ms = data.get('refillIn')
        refill_rate = data.get('refillRate')
        request_tokens_cost = data.get('requestTokens') # Cost of this specific batch call

        if request_tokens_cost is None: # If API doesn't return requestTokens, log a warning.
            logger_instance.warning("fetch_product_batch: 'requestTokens' not found in API response. Token cost for this batch is unknown from API.")
        
        if not products and len(asin_list) > 0 : # If no products array but we requested ASINs
             logger_instance.warning(f"fetch_product_batch: No 'products' array in response for ASINs {asins_str[:50]}... despite 2xx status. Full response keys: {list(data.keys())}")


        # Log token information received
        logger_instance.info(
            f"fetch_product_batch: API Response Tokens: left={tokens_left}, refillIn(ms)={refill_in_ms}, "
            f"refillRate={refill_rate}, requestCost={request_tokens_cost}"
        )

        return products, tokens_left, refill_in_ms, refill_rate, request_tokens_cost

    except requests.exceptions.HTTPError as e:
        logger_instance.error(f"fetch_product_batch: HTTP Error for ASINs {asins_str[:50]}... - {str(e)}")
        if e.response is not None:
            logger_instance.error(f"fetch_product_batch: Response status code: {e.response.status_code}")
            logger_instance.error(f"fetch_product_batch: Response text: {e.response.text[:200]}...") # Log first 200 chars of error response
            if e.response.status_code == 429:
                # Specific handling or just logging is fine, main loop will also see error
                logger_instance.error(f"fetch_product_batch: Received 429 (Too Many Requests).")
        return [], None, None, None, None # Consistent error return
        
    except requests.exceptions.RequestException as e:
        logger_instance.error(f"fetch_product_batch: Request Exception for ASINs {asins_str[:50]}... - {str(e)}")
        return [], None, None, None, None
    except json.JSONDecodeError as e:
        logger_instance.error(f"fetch_product_batch: JSONDecodeError for ASINs {asins_str[:50]}... - {str(e)}. Response text: {response.text[:200]}...")
        return [], None, None, None, None
    except Exception as e:
        logger_instance.error(f"fetch_product_batch: Generic Exception for ASINs {asins_str[:50]}... - {str(e)}")
        return [], None, None, None, None
# --- End New Batch Fetching Function ---

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

                    # Log a summary of the row being written
                    non_hyphen_row_items = {k: v for k, v in row_content.items() if v != '-'}
                    logger.info(f"Writing CSV row for ASIN (from deal obj): {asin_from_deal}, ASIN (from row obj): {asin_from_row}. Non-hyphen count: {len(non_hyphen_row_items)}. Keys: {list(non_hyphen_row_items.keys())}")
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
    global args, LAST_API_CALL_TIMESTAMP, current_available_tokens, last_refill_calculation_time # Added more globals
    args = parser.parse_args() # Initialize global args
    logger = logging.getLogger('KeepaDeals') # Obtain logger instance
    
    LAST_API_CALL_TIMESTAMP = time.time() # Initialize to script start time

    try:
        logger.info("Starting Keepa_Deals...")
        print("Starting Keepa_Deals...", flush=True)
        time.sleep(1) # Reduced initial sleep
        
        all_deals = []
        page = 0
        while True:
            logger.info(f"Fetching deals page {page}...")
            # print(f"Fetching deals page {page}...", flush=True) # Less verbose console for deal fetching
            deals_page = fetch_deals_for_deals(page) 
            if not deals_page:
                logger.info(f"No more deals found on page {page}.")
                break
            all_deals.extend(deals_page)
            logger.info(f"Fetched {len(deals_page)} deals from page {page}. Total deals so far: {len(all_deals)}")
            page += 1
            time.sleep(0.5) # Small delay between deal page fetches

        # TEMPORARY: Limit deals for faster testing (adjust as needed for batch testing)
        # MAX_DEALS_TO_PROCESS_FOR_TESTING = 100 # Original single ASIN test limit
        MAX_DEALS_TO_PROCESS_FOR_TESTING = 250 # For testing a few batches
        if len(all_deals) > MAX_DEALS_TO_PROCESS_FOR_TESTING:
            logger.warning(f"TEMPORARY TEST LIMIT: Processing only the first {MAX_DEALS_TO_PROCESS_FOR_TESTING} of {len(all_deals)} deals.")
            deals_to_process_master_list = all_deals[:MAX_DEALS_TO_PROCESS_FOR_TESTING]
        else:
            deals_to_process_master_list = all_deals
        # END TEMPORARY LIMIT

        rows = []
        if not deals_to_process_master_list:
            logger.warning("No deals fetched or all filtered out by temporary limit, writing diagnostic CSV")
            write_csv([], [], diagnostic=True)
            return

        logger.info(f"Collected {len(deals_to_process_master_list)} deals to process (after potential temporary limit).")

        # Create a dictionary to map ASINs back to their original deal objects
        # This is important because the batch API returns product data which needs to be re-associated
        # with the deal-specific information (like 'Deal found', 'last update' from deal object).
        # We filter for valid ASINs here before adding to the dict.
        asin_to_deal_map = {}
        valid_asins_to_fetch = []
        for deal_obj in deals_to_process_master_list:
            asin = deal_obj.get('asin')
            if validate_asin(asin):
                if asin not in asin_to_deal_map: # Avoid duplicates, process first occurrence
                    asin_to_deal_map[asin] = deal_obj
                    valid_asins_to_fetch.append(asin)
                else:
                    logger.info(f"Duplicate ASIN {asin} found in deals list, will process based on first occurrence.")
            else:
                logger.warning(f"Invalid ASIN '{asin}' found in deals list. Skipping. Deal: {deal_obj.get('title', 'N/A')}")
                # Add placeholder for invalid ASIN from the master list to maintain CSV structure if needed,
                # or decide to only include successfully processed ASINs in CSV.
                # For now, we'll create placeholders for these pre-fetch validation failures.
                placeholder_row = {'ASIN': f"INVALID_ASIN_SKIPPED_{str(asin)[:20]}"}
                for header_key in HEADERS:
                    if header_key not in placeholder_row: placeholder_row[header_key] = '-'
                rows.append(placeholder_row)
        
        logger.info(f"Processing {len(valid_asins_to_fetch)} unique and valid ASINs in batches.")

        for i in range(0, len(valid_asins_to_fetch), BATCH_SIZE):
            asin_batch_list = valid_asins_to_fetch[i:i + BATCH_SIZE]
            batch_number = (i // BATCH_SIZE) + 1
            total_batches = (len(valid_asins_to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"Processing Batch {batch_number}/{total_batches} with {len(asin_batch_list)} ASINs.")

            # --- Quota & Delay Management for Batch ---
            update_and_check_quota(logger) # Check quota BEFORE making a call

            current_time_for_batch = time.time()
            time_since_last_batch_call = current_time_for_batch - LAST_API_CALL_TIMESTAMP
            if time_since_last_batch_call < MIN_TIME_BETWEEN_BATCH_CALLS_SECONDS:
                wait_duration = MIN_TIME_BETWEEN_BATCH_CALLS_SECONDS - time_since_last_batch_call
                logger.info(f"Batch Pre-emptive Pause: Last batch call was {time_since_last_batch_call:.2f}s ago. Waiting for {wait_duration:.2f}s.")
                time.sleep(wait_duration)
            
            # --- Fetch Batch Data ---
            # api_key is global, logger is local 'logger'
            batch_products_data, api_tokens_left, api_refill_in, api_refill_rate, api_request_cost = fetch_product_batch(asin_batch_list, api_key, logger)
            LAST_API_CALL_TIMESTAMP = time.time() # Update timestamp *after* the batch call attempt

            # --- Update Token Count from API Response ---
            if api_tokens_left is not None:
                logger.info(f"Batch API Call Token Update: Previous tokens: {current_available_tokens:.2f}. API reported tokensLeft: {api_tokens_left}. Updating.")
                current_available_tokens = float(api_tokens_left) # Trust API response
                # Log other token info if available
                if api_refill_in is not None:
                    logger.info(f"Batch API Call Token Info: refillIn(ms): {api_refill_in}, refillRate: {api_refill_rate}")
                if api_request_cost is not None:
                    logger.info(f"Batch API Call Token Info: This batch call cost: {api_request_cost} tokens.")
                else: # Fallback if requestTokens is not in response (as seen in one test)
                    # This is a less accurate fallback. The primary reliance is on tokensLeft.
                    # If requestTokens is reliably absent, this part needs rethinking or removal.
                    # For now, only log if it's missing.
                    logger.warning("`requestTokens` was not available from `fetch_product_batch` response. Cannot confirm actual cost via this metric.")
            else:
                logger.warning("Batch API Call: `tokensLeft` not available from `fetch_product_batch`. Cannot update token count from API.")
                # If tokensLeft is None, it implies an error in fetch_product_batch before/during API call or parsing.
                # The function fetch_product_batch should log this.
                # We might need to decrement tokens based on expected cost if the call was likely made but parsing failed.
                # However, if fetch_product_batch returns [], None, ... it usually means a call failure where tokens might not have been consumed.
                # For now, if tokensLeft is None, we don't adjust current_available_tokens, assuming the error prevented consumption or API didn't provide data.


            # --- Process Products in the Batch ---
            if not batch_products_data and isinstance(batch_products_data, list): # Empty list means error or no products
                logger.error(f"Batch {batch_number}/{total_batches}: No product data returned or error in fetch_product_batch. Skipping processing for this batch's ASINs: {', '.join(asin_batch_list[:3])}...")
                # Add placeholders for all ASINs in this failed batch
                for asin_in_failed_batch in asin_batch_list:
                    placeholder_row = {'ASIN': asin_in_failed_batch, 'Error': 'Batch fetch failed'}
                    for header_key in HEADERS:
                        if header_key not in placeholder_row: placeholder_row[header_key] = '-'
                    rows.append(placeholder_row)
                
                # Check for 429 specifically (fetch_product_batch logs it, but main loop might need to react)
                # This part is tricky as fetch_product_batch returns generic error indicators.
                # We'd need more specific error propagation if we want a long pause here for 429.
                # For now, the MIN_TIME_BETWEEN_BATCH_CALLS_SECONDS and quota check should handle most throttling.
                # If a 429 did occur, tokensLeft *should* be low, triggering update_and_check_quota's pause.
                continue # Move to the next batch

            # Create a map of ASIN -> product_data for easier lookup from the batch results
            product_data_map = {p.get('asin'): p for p in batch_products_data if p.get('asin')}

            for asin_in_batch in asin_batch_list: # Iterate through the ASINs we *requested* for this batch
                product = product_data_map.get(asin_in_batch)
                deal_obj_for_asin = asin_to_deal_map.get(asin_in_batch) # Get the original deal object

                if not deal_obj_for_asin: # Should not happen if valid_asins_to_fetch was built correctly
                    logger.error(f"Critical error: No deal_obj found in asin_to_deal_map for ASIN {asin_in_batch}. Skipping.")
                    placeholder_row = {'ASIN': asin_in_batch, 'Error': 'Internal map error'}
                    for header_key in HEADERS:
                        if header_key not in placeholder_row: placeholder_row[header_key] = '-'
                    rows.append(placeholder_row)
                    continue

                if not product:
                    logger.warning(f"No product data returned in batch for ASIN {asin_in_batch}. Adding placeholder.")
                    # This can happen if an ASIN is invalid but was part of a successful batch call,
                    # or if Keepa doesn't have data for it. Tokens for this ASIN were still consumed.
                    placeholder_row = {'ASIN': asin_in_batch, 'Error': 'No data in batch response'}
                    for header_key in HEADERS:
                        if header_key not in placeholder_row: placeholder_row[header_key] = '-'
                    rows.append(placeholder_row)
                    continue
                
                # --- Standard Product Processing Logic (adapted from old loop) ---
                logger.info(f"Processing data for ASIN {asin_in_batch} from batch {batch_number}/{total_batches}")

                # Error flag in product data from Keepa (e.g. if ASIN is invalid on their end)
                if product.get('error') or 'stats' not in product: 
                    logger.error(f"Incomplete or error in product data for ASIN {asin_in_batch}. Product: {str(product)[:200]}")
                    placeholder_row = {'ASIN': asin_in_batch, 'Error': 'API product error or no stats'}
                    for header_key in HEADERS:
                        if header_key not in placeholder_row: placeholder_row[header_key] = '-'
                    rows.append(placeholder_row)
                    continue

                # Log raw product data for specific ASIN (for debugging, if needed)
                # TEST_ASIN_FOR_RAW_LOG = '1562243179' 
                # if asin_in_batch == TEST_ASIN_FOR_RAW_LOG:
                #    logger.info(f"RAW_PRODUCT_DATA_{asin_in_batch}: {json.dumps(product)}")

                row = {}
                try:
                    for header, func in zip(HEADERS, FUNCTION_LIST):
                        if func:
                            try:
                                input_data_for_func = None
                                # Determine input_data based on header (deal_obj_for_asin or product)
                                if header in ['Deal found', 'last update', 'last price change']:
                                    # These functions might need both deal and product, ensure they are updated
                                    # For now, assuming 'last update' and 'last price change' use product,
                                    # and 'Deal found' uses deal_obj_for_asin.
                                    if header == 'Deal found':
                                        input_data_for_func = deal_obj_for_asin
                                        result = func(input_data_for_func, config, logger)
                                    else: # 'last update', 'last price change'
                                        input_data_for_func = deal_obj_for_asin # Original first arg
                                        # These functions expect (deal, config, logger, product_data)
                                        result = func(input_data_for_func, config, logger, product)
                                else:
                                    input_data_for_func = product # All other functions use product data
                                    result = func(input_data_for_func)
                                    
                                # logger.debug(f"ASIN {asin_in_batch} - Header: {header}, Func: {func.__name__}, Result: {result}")
                                row.update(result)
                            except Exception as e:
                                logger.error(f"Function {func.__name__} failed for ASIN {asin_in_batch} (header '{header}'): {str(e)}")
                                row[header] = '-'
                    
                    non_hyphen_items = {k: v for k, v in row.items() if v != '-'}
                    logger.info(f"ASIN {asin_in_batch}: Processed. Row non-hyphen count: {len(non_hyphen_items)}. Keys: {list(non_hyphen_items.keys())[:5]}")
                    rows.append(row)

                except Exception as e:
                    logger.error(f"Error processing functions for ASIN {asin_in_batch}: {str(e)}")
                    placeholder_row = {'ASIN': asin_in_batch, 'Error': 'Function processing error'}
                    for header_key in HEADERS:
                        if header_key not in placeholder_row: placeholder_row[header_key] = '-'
                    rows.append(placeholder_row)
                    continue
            # --- End of loop for ASINs within a batch ---
        # --- End of loop for batches ---

        # Pass the original deals_to_process_master_list for length comparison, or adjust write_csv if only `rows` is primary
        # For now, to match original logic, we need a list of deal-like objects that correspond to `rows`.
        # This is complex because rows are generated from valid_asins_to_fetch + placeholders for initial invalid.
        # Simplest for now: write_csv should primarily rely on `rows` and derive its "deals" count from there.
        # Or, we construct a "processed_deals_corresponding_to_rows" list.
        # Let's adjust write_csv to be more flexible or pass only rows.
        # For now, let's try to maintain some semblance of the original call.
        # The `rows` list now contains dicts for all ASINs attempted (valid, invalid, fetch errors).
        # The `deals_to_process_master_list` was the original source.
        # A more robust write_csv would just take `rows` and `HEADERS`.
        
        # Create a simple list of deal-like objects for write_csv, matching the structure of `rows`
        # This ensures that the number of "deals" matches the number of "rows" for write_csv's internal checks.
        # Each item in `rows` should have an 'ASIN' key.
        pseudo_deals_for_csv = [{'asin': r.get('ASIN', 'UNKNOWN')} for r in rows]
        write_csv(rows, pseudo_deals_for_csv)

        logger.info("Script completed!")
        print("Script completed!")
        # print(f"Processed ASINs: {[row.get('ASIN', '-') for row in rows]}") # Can be very long
    except Exception as e:
        logger.error(f"Main failed: {str(e)}", exc_info=True) # Add exc_info for traceback
        print(f"Main failed: {str(e)}")
        sys.exit(1)
# Chunk 4 ends

if __name__ == "__main__":
    main()

#### END of Keepa_Deals.py ####