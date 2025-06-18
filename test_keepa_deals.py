# test_keepa_deals.py

import unittest
from unittest.mock import patch, MagicMock

# Assuming stable_products.py is in the same directory or accessible in PYTHONPATH
from stable_products import buy_box_current, get_asin, get_title # Import other functions if they are used by buy_box_current or for context
from Keepa_Deals import CSV_PATH, HEADERS, FUNCTION_LIST # For context if main script parts were to be tested

class TestKeepaDeals(unittest.TestCase):

    def test_buy_box_current_correctly_uses_buyBoxPrice(self):
        # Mock product data for ASIN 1531025773 based on provided debug log
        mock_product_data = {
            'asin': '1531025773',
            'title': "An Introduction to Law, Law Study, and the Lawyer's Role",
            'stats': {
                'current': [3762, 3700, 2421, 359235, 3800, -1, -1, 4638, -1, -1, 3700, 4, 2, -1, -1, 6, 50, 2, 3762, -1, -1, 2421, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 2421, -1],
                'buyBoxPrice': 3762, # Correct Buy Box Price
                # Other stats fields can be added if necessary for complete simulation
            }
            # Other product fields can be added if buy_box_current or its dependencies use them
        }
        
        result = buy_box_current(mock_product_data)
        expected_price = '$37.62'
        self.assertEqual(result.get('Buy Box - Current'), expected_price, 
                         f"Expected Buy Box price {expected_price} for ASIN 1531025773, got {result.get('Buy Box - Current')}")

    def test_buy_box_current_handles_missing_buyBoxPrice(self):
        # Mock product data where buyBoxPrice is missing
        mock_product_data_missing = {
            'asin': 'TESTASIN001',
            'title': "Test Product Missing buyBoxPrice",
            'stats': {
                'current': [-1, 3000, 2000, 100000, 5000, -1, -1, 3000, -1, -1, 3000, 1, 1, -1, -1, 1, 1, 1, 3000, -1, -1, 2000, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 2000, -1],
                # buyBoxPrice is intentionally missing
            }
        }
        
        result = buy_box_current(mock_product_data_missing)
        self.assertEqual(result.get('Buy Box - Current'), '-', 
                         f"Expected '-' when buyBoxPrice is missing, got {result.get('Buy Box - Current')}")

    def test_buy_box_current_handles_invalid_buyBoxPrice(self):
        # Mock product data where buyBoxPrice is invalid (e.g., -1)
        mock_product_data_invalid = {
            'asin': 'TESTASIN002',
            'title': "Test Product Invalid buyBoxPrice",
            'stats': {
                'current': [-1, 3000, 2000, 100000, 5000, -1, -1, 3000, -1, -1, 3000, 1, 1, -1, -1, 1, 1, 1, 3000, -1, -1, 2000, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 2000, -1],
                'buyBoxPrice': -1 
            }
        }
        
        result = buy_box_current(mock_product_data_invalid)
        self.assertEqual(result.get('Buy Box - Current'), '-', 
                         f"Expected '-' when buyBoxPrice is -1, got {result.get('Buy Box - Current')}")

    def test_buy_box_current_handles_zero_buyBoxPrice(self):
        # Mock product data where buyBoxPrice is zero
        mock_product_data_zero = {
            'asin': 'TESTASIN003',
            'title': "Test Product Zero buyBoxPrice",
            'stats': {
                'current': [-1, 3000, 2000, 100000, 5000, -1, -1, 3000, -1, -1, 3000, 1, 1, -1, -1, 1, 1, 1, 3000, -1, -1, 2000, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 2000, -1],
                'buyBoxPrice': 0
            }
        }
        
        result = buy_box_current(mock_product_data_zero)
        self.assertEqual(result.get('Buy Box - Current'), '-',
                         f"Expected '-' when buyBoxPrice is 0, got {result.get('Buy Box - Current')}")

if __name__ == '__main__':
    # Create a dummy config.json and headers.json if stable_products.py tries to load them globally (though it shouldn't for these unit tests)
    # For the functions being tested (buy_box_current), these global loads are not directly influential,
    # but it's good practice if the module has global-level file I/O.
    # However, based on stable_products.py content, only Keepa_Deals.py loads these.
    
    # Setup basic logging for tests to see debug/info messages from the function
    import logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

# To run these tests from the command line:
# python -m unittest test_keepa_deals.py
