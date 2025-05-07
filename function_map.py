# function_map.py
from stable import (
    get_stat_value, get_title, get_asin, sales_rank_current, used_current,
    sales_rank_30_days_avg, sales_rank_90_days_avg, sales_rank_180_days_avg,
    sales_rank_365_days_avg, package_quantity, package_weight, package_height,
    package_length, package_width, used_like_new, used_very_good, used_good,
    used_acceptable, new_3rd_party_fbm_current, list_price
)
from Keepa_Deals import (
    new_3rd_party_fbm, used_like_new as deals_used_like_new,
    used_very_good as deals_used_very_good, used_good as deals_used_good,
    used_acceptable as deals_used_acceptable, used_offer_count_current
)

FUNCTION_MAP = {
    "Percent Down 90": None,
    "Avg. Price 90": None,
    # ... (same as previous FUNCTION_MAP, 192 mappings)
    "Used Offer Count - Current": used_offer_count_current,
    "Used Offer Count - 30 days avg.": None,
    "Used Offer Count - 60 days avg.": None,
    "Used Offer Count - 90 days avg.": None,
    "Used Offer Count - 180 days avg.": None,
    "Used Offer Count - 365 days avg.": None
}
# header_map.py
# Placeholder to maintain project structure