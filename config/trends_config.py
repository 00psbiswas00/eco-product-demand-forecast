# config/trends_config.py

from config.sustainability_tags import SUSTAINABILITY_TAGS

# Flatten all keywords from the sustainability mapping
TRENDS_KEYWORDS = sorted(
    {kw for keywords in SUSTAINABILITY_TAGS.values() for kw in keywords}
)


# Trends API settings
TRENDS_TIMEFRAME = "today 12-m"
TRENDS_BATCH_SIZE = 5