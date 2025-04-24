import keepa
import logging
logging.basicConfig(filename='test_fields_log.txt', level=logging.DEBUG)

api = keepa.Keepa('bg9037ndr2jrlore45acr8a3gustia0tusdfk5e54g1le917nspnk9jiktp7b08b')  # From config.json
query = {
    "page": 0, "domainId": "1", "includeCategories": [283155], "priceTypes": [2],
    "currentRange": [2000, 30100], "warehouseConditions": [2, 3, 4, 5]
}
response = api.deals(query)
for product in response['dr']:
    if product['asin'] == '1499810695':
        stats = product.get('stats', {})
        logging.debug(f"ASIN 1499810695: stats.current={stats.get('current', [-1] * 30)}")
        logging.debug(f"ASIN 1499810695: stats.avg90={stats.get('avg90', [-1] * 30)}")
        logging.debug(f"ASIN 1499810695: stats.avg365={stats.get('avg365', [-1] * 30)}")
        break