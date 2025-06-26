# stable_calculations.py
# (Last update: Version 5)

import logging

# Percent Down 365 starts
def percent_down_365(product):
    """
    Calculates the percentage difference between the current used price and the 
    365-day average used price. Prepends symbols for above/below average.
    """
    asin = product.get('asin', 'unknown')
    logging.debug(f"percent_down_365 input for ASIN {asin}: product data received.")

    stats = product.get('stats', {})
    if not stats:
        logging.warning(f"ASIN {asin}: 'stats' object is missing or empty. Cannot calculate Percent Down 365.")
        return {'Percent Down 365': '-'}

    current_used_price_raw = stats.get('current', [])
    avg_365_price_raw = stats.get('avg365', [])

    # Index 2 is for 'USED' price
    current_used = -1
    if len(current_used_price_raw) > 2 and current_used_price_raw[2] is not None:
        current_used = current_used_price_raw[2]
    
    avg_365 = -1
    if len(avg_365_price_raw) > 2 and avg_365_price_raw[2] is not None:
        avg_365 = avg_365_price_raw[2]

    logging.debug(f"ASIN {asin}: Raw current_used (stats.current[2]): {current_used}, Raw avg_365 (stats.avg365[2]): {avg_365}")

    if avg_365 <= 0 or current_used < 0: # current_used can be 0 if item is free, but avg_365 should be positive
        logging.warning(f"ASIN {asin}: Invalid or missing prices for Percent Down 365 calculation. current_used: {current_used}, avg_365: {avg_365}. Returning '-'")
        return {'Percent Down 365': '-'}

    try:
        # Calculate percentage difference
        # Formula: ((avg - current) / avg) * 100 gives % down from average
        # If current > avg, this will be negative, meaning it's % *up* from average.
        
        percentage_diff = ((avg_365 - current_used) / avg_365) * 100
        
        symbol = ""
        # Note: The problem description asks for "-" if below average, and "+" if above.
        # The current `percentage_diff` calculation: ((avg - current) / avg) * 100
        # - If current < avg (e.g. current=80, avg=100), diff = ((100-80)/100)*100 = 20%. We want "-20%".
        # - If current > avg (e.g. current=120, avg=100), diff = ((100-120)/100)*100 = -20%. We want "+20%".
        # - If current == avg, diff = 0. We want "0%".

        if current_used < avg_365: # Current is below average, show as negative percentage
            symbol = "-" 
            # percentage_diff is already positive (e.g., 20.0 for 20% down)
        elif current_used > avg_365: # Current is above average, show as positive percentage
            symbol = "+"
            percentage_diff = abs(percentage_diff) # Make it positive (e.g. abs(-20.0) = 20.0 for 20% up)
        # If current_used == avg_365, percentage_diff will be 0. Symbol remains "".

        # Format to zero decimal places
        formatted_percentage = f"{percentage_diff:.0f}%"
        
        # Prepend symbol only if it's explicitly set (for + or -)
        # If symbol is "", it means it's 0%, so no sign needed.
        if symbol:
            result_str = f"{symbol}{formatted_percentage}"
        else: # This case is for 0%
            result_str = formatted_percentage 

        logging.info(f"ASIN {asin}: Percent Down 365 calculated. Current: {current_used/100:.2f}, Avg365: {avg_365/100:.2f}, Result: {result_str}")
        return {'Percent Down 365': result_str}

    except ZeroDivisionError:
        logging.error(f"ASIN {asin}: ZeroDivisionError in percent_down_365 (avg_365 was {avg_365}). Returning '-'")
        return {'Percent Down 365': '-'}
    except Exception as e:
        logging.error(f"ASIN {asin}: Exception in percent_down_365: {str(e)}. current_used: {current_used}, avg_365: {avg_365}. Returning '-'")
        return {'Percent Down 365': '-'}
# Percent Down 365 ends

### END of stable_calculations.py ### 