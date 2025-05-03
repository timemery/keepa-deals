# inspect_keepa_stats.py
import json
import logging
import requests
import sys

# Logging
logging.basicConfig(
    filename="debug_log.txt",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s",
)

try:
    # Load API key
    with open("config.json") as f:
        config = json.load(f)
        api_key = config["api_key"]
except Exception as e:
    logging.error(f"Failed to load config.json: {str(e)}")
    sys.exit(1)

def fetch_product_stats(asin, days=90, offers=20, rating=1):
    """Fetch product stats from Keepa /product endpoint."""
    url = (
        f"https://api.keepa.com/product?key={api_key}&domain=1"
        f"&asin={asin}&stats={days}&offers={offers}&rating={rating}"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212"
    }
    logging.debug(f"Fetching product stats for ASIN {asin}: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        logging.debug(f"Response status: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"Request failed: {response.status_code}, {response.text}")
            return None
        data = response.json()
        products = data.get("products", [])
        if not products:
            logging.error(f"No product data for ASIN {asin}")
            return None
        product = products[0]
        stats = product.get("stats", {})
        current = stats.get("current", [-1] * 30)
        logging.debug(f"Raw stats.current for ASIN {asin}: {current[:20]}")
        return stats
    except Exception as e:
        logging.error(f"Request failed for ASIN {asin}: {str(e)}")
        return None

def map_stats_current(stats):
    """Map stats.current indices to Keepa fields."""
    current = stats.get("current", [-1] * 30)
    mappings = [
        (0, "Amazon Price"),
        (1, "Marketplace New (Buy Box)"),
        (2, "Sales Rank"),
        (3, "Marketplace Used"),
        (10, "Buy Box (Alt)"),
        (17, "Number of Reviews"),
        (18, "Buy Box Used"),
    ]
    logging.info("Mapping stats.current indices:")
    for index, label in mappings:
        value = current[index] if index < len(current) else -1
        formatted = (
            f"${value / 100:.2f}"
            if value > 0 and index in [0, 1, 3, 10, 18]
            else str(value)
        )
        logging.info(f"stats.current[{index}] ({label}): {formatted}")

def main():
    try:
        logging.info("Starting Keepa stats inspection...")
        asin = "1499810695"  # Test ASIN from your note
        stats = fetch_product_stats(asin)
        if stats:
            map_stats_current(stats)
        else:
            logging.error("No stats retrieved")
        logging.info("Inspection completed!")
    except Exception as e:
        logging.error(f"Inspection failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()