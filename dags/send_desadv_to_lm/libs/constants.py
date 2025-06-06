import os
import sys
import datetime
from enum import StrEnum


class OrderTypes(StrEnum):
    XD = 'xd'
    BBXD = 'bbxd'


PACKAGE_TYPES = {'201', '202', '200', 'RO', 'TRE', 'EH', 'PC', 'PX'}

ROOT_DIR_PATH = os.path.dirname(os.path.abspath(sys.argv[0]))
CONFIG_PATH = os.path.join(ROOT_DIR_PATH, 'config.yaml')

DEFAULT_INPUT_DIR_NAME = 'input'
LOG_DIR_NAME = 'logs'
ERROR_DIR_NAME = 'errors'
PROCESSED_DIR_NAME = 'processed'

LOG_FILE_NAME = f'{datetime.date.today().strftime('%Y.%m.%d')}.log'

SELLER_GLN = '4058075000001'
SHIP_TO_GLN = '4607075269224'

IN_DATE_FORMAT = '%d.%m.%Y'
IN_DATETIME_FORMAT = '%d.%m.%Y %H:%M:%S'
IN_PACKAGES_WORKSHEET = 'Упаковки'
IN_ITEMS_WORKSHEET = 'Позиции'

IN_VALUES_DELIMITER = ';'
