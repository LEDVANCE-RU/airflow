from datetime import timedelta

LAST_CHECK_KEY = 'last_zeroed_stock_check'
DEFAULT_STOCK_HISTORY_HORIZON = timedelta(days=30)
ARRIVAL_DOC_OBSOLESCENCE_THRESHOLD = timedelta(days=365)