import re

SENDER = 'info@service.lemanapro.ru'
SUBJECT_PATTERN = 'Новый заказ '

ORDER_NUM_HEADER = re.compile('.*Номер отправления.*')
DELIVERY_DATE_HEADER = re.compile('.*Дата доставки.*')
ARTICLE_PATTERN = re.compile(r'Артикул: (\d+)')
PRICE_PATTERN = re.compile(r'(\d+\.?\d*)\xa0руб\.')
QTY_PATTERN = re.compile(r'(\d+)\xa0шт\.')
ORDER_NUM_PREFIX = '№ '

IN_DATE_FORMAT = '%Y-%m-%d'

TR = 'tr'