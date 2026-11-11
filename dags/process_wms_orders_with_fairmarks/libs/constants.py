from datetime import timedelta

SFTP_FILENAME_PATTERN = r'.*(Prihod|Rashod).*\.txt'
LAST_SFTP_WMS_ORDERS_READ_KEY = 'sftp_wms_orders_read_at'

SFTP_IN_DIRPATH = '/FileStock/IN'
SFTP_OUT_DIRPATH = '/FileStock/OUT'

SMB_IN_DIRPATH = '/IN'
SMB_OUT_DIRPATH = '/OUT'

TRG_HEADERS = ['EAN', 'КИЗ', 'КИТУ1', 'КИТУ2']
FAIRMARK_ARTICLE_PATTERN = r'\(?01\)?0*(\d+)\(?21\)?.*'

SMB_FILES_RETENTION_THRESHOLD = timedelta(days=90)
