## Timestamp Handling Notes (from Task starting ~June 24-25, 2025)

When working with timestamp fields like 'last update' and 'last price change', the goal is to reflect the most recent relevant event as accurately as possible, aligning with user expectations from observing Keepa.com.

**For 'last_update':**
This field should represent the most recent time any significant data for the product/deal was updated by Keepa. It considers:
1.  `product_data['products'][0]['lastUpdate']` (general product data update from /product endpoint).
2.  `deal_object.get('lastUpdate')` (general deal data update from /deal endpoint).
3.  `product_data.get('stats', {}).get('lastOffersUpdate')` (when offers were last refreshed from /product endpoint stats).
The function should take the maximum valid (recent) timestamp from these three sources.

**For 'last_price_change' (specifically for Used items, excluding 'Acceptable'):**
This field aims to find the most recent price change for relevant used conditions.
1.  **Primary Source (`product_data.csv`):** Check historical data for 'USED' (`csv[2]`), 'USED_LIKE_NEW' (`csv[6]`), 'USED_VERY_GOOD' (`csv[7]`), and 'USED_GOOD' (`csv[8]`). Select the most recent valid timestamp from these.
2.  **Fallback Source (`deal_object.currentSince`):** If CSV data is insufficient, check `currentSince[2]` (Used), `currentSince[19]` (Used-LikeNew), `currentSince[20]` (Used-VeryGood), and `currentSince[21]` (Used-Good). Additionally, if `deal_object.current[14]` indicates the Buy Box is 'Used', also consider `currentSince[32]` (buyBoxUsedPrice timestamp). Select the most recent valid timestamp from this combined pool.

**General Timestamp Conversion:**
All Keepa minute timestamps should be converted to datetime objects using `KEEPA_EPOCH = datetime(2011, 1, 1)`, then localized from naive UTC to aware UTC (`timezone('UTC').localize(dt)`), and finally converted to 'America/Toronto' (`astimezone(TORONTO_TZ)`), formatted as '%Y-%m-%d %H:%M:%S'. Timestamps <= 100000 are generally considered invalid/too old.

## My Learnings from Investigating "Used, acceptable - Current" (Date: YYYY-MM-DD)

**Task Context:** I investigated discrepancies in the "Used, acceptable - Current" column of `Keepa_Deals_Export.csv`. It turned out some prices were incorrect due to historical fallback logic in the script.

**My Key Learnings & Principles:**

1.  **Dev Logs are Crucial Historical Records:**
    *   The `API_Dev_Log_v4.txt` was invaluable in understanding the evolution of the code and the rationale behind previous fixes. It helped me determine that the issue you described (incorrect prices due to fallbacks) had likely already been addressed.
    *   **Principle:** I've learned to always consult available development logs or historical documentation first. They can provide context, prevent redundant work, and reveal if a current problem is a regression or a known past issue.

2.  **Verify Current Code Against Problem Description:**
    *   You reported "fallbacks" as the cause of incorrect pricing. My analysis of the *current* `used_acceptable` function in `stable_products.py` showed that such fallback logic had been removed, and it was strictly using `stats.current[22]`.
    *   **Principle:** When investigating a bug, especially one described as "familiar," I now know to always compare the current state of the relevant code modules with the problem description. The problem might have been fixed in a previous iteration.

3.  **Distinguish Between Script Logic Errors and API Data Discrepancies:**
    *   The `used_acceptable` function now directly uses `stats.current[22]`. If price discrepancies persist for this field *after* confirming the code strictly uses this index, the issue likely lies with the data provided by the Keepa API for that specific index and ASIN, or a mismatch in interpretation between the API field and the website's display.
    *   **Principle:** I've learned to clearly differentiate between errors caused by the script's data processing/fallback logic and potential inconsistencies or specificities in the data received from external APIs. A fix for one might not address the other.

4.  **Simplify to Reduce Errors:**
    *   The original problem was caused by complex fallback logic pulling from "vaguely related fields." The fix, implemented prior to this task and confirmed during it, was to simplify the function to use a single, specific API data point (`stats.current[22]`)
    *   **Principle:** When dealing with API data, aiming for direct mapping to the most accurate known field, rather than implementing complex fallbacks, can often lead to more reliable and maintainable code. Fallbacks, if not carefully managed, can obscure data issues or introduce subtle errors.

5.  **Task-Specific Client Avoidance:**
    *   Your request specified avoiding the Keepa Python client. The existing solution (direct HTTP requests and JSON parsing) met this requirement.
    *   **Principle:** I will always adhere to specified constraints, such as avoiding particular libraries or clients, if feasible and clearly stated in the task.

**Outcome of this Investigation:**
*   I confirmed that the `used_acceptable` function in `stable_products.py` was already updated to use `stats.current[22]` without fallbacks, addressing the core issue.
*   No code changes were needed for this specific problem as the fix was pre-existing.
*   This investigation highlighted the importance of dev logs and careful analysis of the current codebase against historical issues.

## Task: FIX (second attempt) the “New, 3rd Party FBA - Lowest” Column (Solved June 2025)

**Core Problem:** The "New, 3rd Party FBA - Lowest" column in `Keepa_Deals_Export.csv` was showing incorrect data or hyphens.

**Key Learnings & Solutions:**

1.  **Understanding Keepa API Data Structures is Crucial:**
    *   `product['offers']`: This array contains information about *current* marketplace offers (up to the limit specified by the `&offers=` parameter in the API call). Each object within this array typically has an `offer.get('price')` field representing the current price of that specific offer.
    *   `product['stats']`: This object contains aggregated historical and current statistics. For specific price types (like 'New, 3rd Party FBA'), it has sub-arrays for `current`, `min`, `max`, `avg`, etc.
        *   `product.stats.min[INDEX][1]`: Provides the *historical minimum price* recorded by Keepa for the price type at `INDEX` (e.g., index 10 is for 'New, 3rd Party FBA') within the requested `stats` history period. The value is `[timestamp, price_in_cents]`, so `[1]` accesses the price.
        *   `product.stats.current[INDEX]`: Provides the *current price* for the price type at `INDEX`.
    *   `offer['offerCSV']` (within `product['offers']`): This field within an individual offer object in the `product['offers']` array is **not a simple array of current offer details**. Instead, it's a list of historical data points for *that specific offer's listing history* (often `[timestamp, price, shipping, stock, condition_code, ...]`, repeated). Using `offerCSV[0]` to get a current price is incorrect as it will likely be a timestamp.

2.  **Clarifying "Lowest":**
    *   The definition of "Lowest" (or any similar term) is critical. Does it mean lowest *currently active offer* or lowest *historical recorded price*?
    *   For this, "New, 3rd Party FBA - Lowest" was successfully implemented by targeting the *historical minimum* using `product.stats.min[10][1]`.
    *   If the goal were the *current lowest active offer*, iterating through `product.offers` and using `offer.get('price')` (while filtering for New, 3rd Party, FBA) would be the method, but this might not reflect true historical lows if that offer isn't currently active or among the top N offers retrieved.

3.  **Iterative Debugging & Log Analysis:**
    *   Detailed logging within data processing functions (showing inputs, intermediate values, and decisions) is invaluable for diagnosing issues, especially when dealing with complex API responses.
    *   Providing specific ASINs and their expected values, along with relevant log excerpts, significantly speeds up the debugging process.

4.  **API Parameter Impact (`fetch_product`):**
    *   The `offers=N` parameter in the `/product` API call limits the number of *current* offers returned in the `product.offers` array. It does not directly impact the `product.stats` data (like `stats.min`).
    *   The `stats=DAYS` parameter determines the historical window for aggregated statistics like `stats.min`.

5.  **Safe Data Access:**
    *   When accessing nested data from API responses (e.g., `product.get('stats', {}).get('min', [])`), always use safe access methods (like `.get()` with defaults) and check for the existence and type of data before trying to use it to prevent `KeyError` or `TypeError` exceptions. Check list lengths before accessing indices.

**Initial Missteps & Corrections:**

*   An early attempt incorrectly tried to use `offer['offerCSV'][0]` from within a `product['offers']` item as the current price, leading to errors because this field often contains a timestamp or historical data, not the current offer price directly.
*   Overly restrictive price validation (e.g., `MAX_REALISTIC_PRICE_CENTS`) can sometimes mask underlying issues if legitimate (though perhaps outlier) data is filtered out. It was helpful to temporarily remove these to diagnose the core problem.

**Final Successful Approach for 'New, 3rd Party FBA - Lowest':**

The solution was to modify `stable_products.py -> new_3rd_party_fba_lowest(product)` to use `product.stats.min[10][1]` to get the historical lowest price, ensuring safe access and correct formatting.

## Task: Fix "New, 3rd Party FBM - Current" Column (June 2025)

**Objective:** Ensure the "New, 3rd Party FBM - Current" column in `Keepa_Deals_Export.csv` accurately reflects the price shown on Keepa.com for this specific metric.

**Key Learnings & Conventions Established:**

1.  **Prioritize Direct `stats` Fields:**
    *   For aggregated price data like "New, 3rd Party FBM - Current (including shipping)", the Keepa API (via direct HTTP `product` endpoint with `stats` parameter) often provides this directly within the `product['stats']['current']` array at a specific index.
    *   Through analysis of logs and comparison with Keepa.com, `product['stats']['current'][7]` was identified as the direct source for "New, 3rd Party FBM - Current price, including shipping".
    *   **Convention:** When aiming to match a specific Keepa-displayed aggregate value, the first approach should be to identify and use the corresponding index in `product['stats']['current']` (or other relevant `stats` arrays like `avg30`, `avg90`, etc.). These fields represent Keepa's own calculated values.

2.  **Strict Data Sourcing for Specific Columns:**
    *   To ensure a column *exactly* matches a specific Keepa field (e.g., "New, 3rd Party FBM - Current"), the script should *only* use the identified direct source (like `stats.current[7]`).
    *   If this direct source is invalid (e.g., -1, null, or unavailable), the column should output "-" rather than falling back to parsing general offers. This prevents populating the column with data that, while potentially related (e.g., a general lowest FBM offer), isn't what Keepa designates for that specific field, thus maintaining data integrity for that column's definition.

3.  **Offer Parsing as a Fallback (for broader "lowest price" type columns):**
    *   For columns intended to find the *absolute lowest* offer of a certain type (e.g., "New, 3rd Party FBA - Lowest"), iterating through the `product['offers']` array is necessary.
    *   **`offerCSV` Complexity:** The `offerCSV` field within an individual offer object is a flat list representing historical data points for that offer. Each data point can have a variable number of elements (not always simple `[timestamp, price, shipping]` triplets). The most recent *actual live price and shipping* for an offer are typically found by looking at the latest entries in `offerCSV` or by using the direct `offer['price']` and `offer['shippingCost']` fields.
    *   **Parsing `offerCSV`:** If parsing `offerCSV` for current price/shipping:
        *   The last elements often represent the latest state. For example, if an entry is `[ts, price, shipping]`, then `offer_csv[-2]` might be price and `offer_csv[-1]` shipping for the most recent data point in that array.
        *   This requires careful handling of array length and potential variations in the number of elements per `offerCSV` entry.
    *   Direct `offer['price']` and `offer['shippingCost']` can serve as fallbacks or primary sources if `offerCSV` is not used or proves too complex for reliable current-price extraction for a given offer type.

4.  **Identifying Offer Characteristics:**
    *   **Condition:** `offer.get('condition') == 1` is a reliable check for "New" items.
    *   **FBM vs. FBA:** `offer.get('isFBA', False)` (checking for `False` for FBM, `True` for FBA) is generally used. The default to `False` if the key is missing helps catch FBM offers where the flag might not be explicit.
    *   **Seller ID:** `offer.get('sellerId')` can be used to exclude Amazon (`ATVPDKIKX0DER`) for 3rd party offers.

5.  **Logging for Debugging:**
    *   Detailed logging within data processing functions is crucial, especially when dealing with complex API responses.
    *   Logs should indicate:
        *   The source of the data being used (e.g., "from stats.current[X]", "from offerCSV", "from direct offer.price").
        *   Key intermediate values (e.g., extracted price, shipping before calculation).
        *   Reasons for offers being included or excluded from consideration.
        *   The final value chosen for a field.

6.  **API Request Parameters:**
    *   The `fetch_product` function uses `offers=100` and `stats=365` (or other day counts). The `stats` parameter is necessary to populate `product['stats']`. The `offers` parameter controls how many individual current marketplace offers are returned in `product['offers']`.

**Implication for Future Tasks:**
When a new column is required, or an existing one is incorrect:
*   First, determine if Keepa provides this as a direct aggregated field (likely in `stats.current` or similar). This is the preferred method for matching specific Keepa-displayed values.
*   If not, or if the goal is to find the "absolute lowest of all available offers" of a type, then careful parsing of `product['offers']` is needed, paying attention to `condition`, `isFBA`, `sellerId`, and robustly extracting price + shipping (likely from `offerCSV`'s latest entry or direct `price`/`shippingCost` fields).
*   Always verify against Keepa.com, and use detailed logging to trace the script's logic.

## Character Encoding and Symbol Compatibility (Task: Percent Down 365, YYYY-MM-DD)

**Learning:** When implementing features that involve special characters or symbols (e.g., arrows like ⇧⇩ for indicating price changes), always consider cross-platform compatibility and potential rendering issues in different environments (terminals, CSV viewers, etc.).

**Problem:** Initial implementation of "Percent Down 365" used Unicode arrows (U+21E7 ⇧, U+21E9 ⇩). These did not render correctly in the user's local testing environment, appearing as garbled characters (e.g., ‚á©).

**Solution & Principle:**
*   Switched to universally compatible ASCII symbols: "+" for an increase (price above average) and "-" for a decrease (price below average). No symbol is used for a 0% difference.
*   **Principle:** Prioritize widely supported character sets (like ASCII or well-tested UTF-8 subsets) for textual indicators unless specific rich text rendering is guaranteed and tested across all target environments. If special symbols are desired, ensure they are tested for compatibility or provide simpler fallbacks.

## CSV Data Interpretation Notes

*   **Date Formatting in Viewing Software:** Be aware that spreadsheet programs (like Microsoft Excel, Google Sheets, etc.) often automatically interpret columns containing date-like strings (e.g., "YYYY-MM-DD", "YYYY-MM") as dates. Their default display formatting for these dates might differ from the raw string in the CSV file (e.g., "1985-06" in the CSV might be displayed as "Jun-85" in Excel). Always verify the raw CSV content in a text editor if precise string formatting is critical and appears different in a spreadsheet.

## Keepa API `stats` Object Insights (Specifically `avg...` arrays)

When working with aggregated statistical arrays from the Keepa API `/product` endpoint (e.g., `stats.current`, `stats.avg30`, `stats.avg90`, `stats.avg365`), the indices for different price types can be specific and sometimes require empirical verification if not explicitly documented for every array type.

**Learnings from "Buy Box - 365 days avg." Investigation (YYYY-MM-DD):**

*   **Initial Assumption:** Index `10` in `stats.avg365` was initially thought to be for "Buy Box - 365 days avg."
*   **Verification:** User feedback and direct log analysis of the `stats_raw.avg365` array for ASIN `0262611317` revealed:
    *   `avg365[10]` corresponded to "New, 3rd Party FBA - 365 days avg." (value: `11460` or $114.60).
    *   `avg365[18]` corresponded to "Buy Box - 365 days avg." (value: `6916` or $69.16). This matched user observations on Keepa.com.

**Key Takeaway:**
*   While there are common patterns for indices (e.g., 0 for AMAZON, 1 for NEW, 2 for USED), specific types like "Buy Box Shipping" or nuanced FBA/FBM averages can have distinct indices.
*   For `avg...` arrays (like `avg365`), **index 18** was confirmed to provide the "Buy Box (including shipping) Average" for the respective period.
*   Always verify assumptions about indices by:
    1.  Consulting any available explicit Keepa documentation for that specific array.
    2.  If unclear, inspect the raw JSON response from the API (specifically the `stats` object and its arrays like `current`, `avg30`, `avg90`, `avg365`) for a known ASIN where the desired data point is visible on Keepa.com. This allows mapping the observed value to its position in the array.
    3.  Cross-reference with field names provided in the `csv` field of the product data, as these often map directly to indices (e.g., `csv[18]` might be Buy Box price history).

**Example `avg365` array structure snippet for ASIN `0262611317`:**
`[-1, 9021 (Amazon), 2742 (Used), ..., 9257 (New FBM), ..., 11460 (New FBA), ..., 6916 (Buy Box Shipping), ...]`
*(Note: This is a simplified representation; always refer to the full array for accurate indexing.)*






