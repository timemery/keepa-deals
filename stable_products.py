# stable_products.py
import requests
import logging
from retrying import retry

# Shared globals
API_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212'}

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_asin(asin, api_key):
    if not validate_asin(asin):  # Use stable_deals.py function
        return {'ASIN': '-'}
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"get_asin response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"get_asin request failed for ASIN {asin}: {response.status_code}")
            return {'ASIN': '-'}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"get_asin no product data for ASIN {asin}")
            return {'ASIN': '-'}
        asin_value = products[0].get('asin', '-')
        logging.debug(f"get_asin result for ASIN {asin}: {asin_value}")
        return {'ASIN': f'="{asin_value}"' if asin_value != '-' else '-'}
    except Exception as e:
        logging.error(f"get_asin fetch failed for ASIN {asin}: {str(e)}")
        return {'ASIN': '-'}

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def get_title(asin, api_key):
    if not validate_asin(asin):
        return {'Title': '-'}
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"get_title response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"get_title request failed for ASIN {asin}: {response.status_code}")
            return {'Title': '-'}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"get_title no product data for ASIN {asin}")
            return {'Title': '-'}
        title = products[0].get('title', '-')
        logging.debug(f"get_title result for ASIN {asin}: {title[:50]}")
        return {'Title': title}
    except Exception as e:
        logging.error(f"get_title fetch failed for ASIN {asin}: {str(e)}")
        return {'Title': '-'}

@retry(stop_max_attempt_number=3, wait_fixed=5000)
def package_quantity(asin, api_key):
    if not validate_asin(asin):
        return {'Package - Quantity': '-'}
    url = f"https://api.keepa.com/product?key={api_key}&domain=1&asin={asin}"
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=30)
        logging.debug(f"package_quantity response status for ASIN {asin}: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"package_quantity request failed for ASIN {asin}: {response.status_code}")
            return {'Package - Quantity': '-'}
        data = response.json()
        products = data.get('products', [])
        if not products:
            logging.error(f"package_quantity no product data for ASIN {asin}")
            return {'Package - Quantity': '-'}
        quantity = products[0].get('packageQuantity', -1)
        if quantity == 0:
            logging.warning(f"Package Quantity is 0 for ASIN {asin}, defaulting to 1")
            quantity = 1
        logging.debug(f"package_quantity result for ASIN {asin}: {quantity}")
        return {'Package - Quantity': str(quantity) if quantity != -1 else '-'}
    except Exception as e:
        logging.error(f"package_quantity fetch failed for ASIN {asin}: {str(e)}")
        return {'Package - Quantity': '-'}