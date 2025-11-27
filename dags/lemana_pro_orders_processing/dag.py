import re
from datetime import datetime

import bs4
from attr import dataclass

from exchangelib import DELEGATE, Credentials, Account, Configuration, EWSDateTime, UTC

from constants import TZ_UTC

HOST = 'imap.lancloud.ru'
PORT = 993
USERNAME = ''
PASSWORD = ''

SENDER = ''
SUBJECT_PATTERN = 'Новый заказ '

ORDER_NUM_HEADER = 'Номер отправления'
DELIVERY_DATE_HEADER = 'Дата доставки'
ARTICLE_PATTERN = re.compile(r'Артикул: (\d+)')
PRICE_PATTERN = re.compile(r'(\d+\.?\d*)\xa0руб\.')
QTY_PATTERN = re.compile(r'(\d+)\xa0шт\.')
ORDER_NUM_PREFIX = '№ '

IN_DATE_FORMAT = '%Y-%m-%d'

TR = 'tr'

@dataclass
class Item:
    article: str
    price: float
    qty: int


@dataclass
class Order:
    num: str
    delivery_date: datetime
    items: list[Item]


class OrderParser:
    def __init__(self):
        self._soup = None

    def parse(self, html_contents: str) -> Order:
        self._soup = bs4.BeautifulSoup(html_contents, features="lxml")
        order_num_header_node = self._soup.find(string=ORDER_NUM_HEADER).find_parent(TR).find_next(TR)
        order_num = order_num_header_node.get_text(strip=True).replace(ORDER_NUM_PREFIX, '')

        delivery_date_header_node = self._soup.find(string=DELIVERY_DATE_HEADER).find_parent(TR).find_next(TR)
        delivery_date = datetime.strptime(delivery_date_header_node.get_text(strip=True), IN_DATE_FORMAT)

        items = self._parse_items()

        return Order(
            num = order_num,
            delivery_date=delivery_date,
            items=items
        )

    def _parse_items(self) -> list[Item]:
        values = []
        for pattern in [ARTICLE_PATTERN, PRICE_PATTERN, QTY_PATTERN]:
            nodes = self._soup.find_all(string=pattern)
            values.append([re.search(pattern, n.get_text(strip=True)).group(1) for n in nodes])

        return [Item(article=v[0], price=float(v[1]), qty=int(v[2])) for v in zip(*values)]


credentials = Credentials(USERNAME, PASSWORD)
config = Configuration(server='mail.lancloud.ru', credentials=credentials)
account = Account(USERNAME,
                  config=config,
                  credentials=credentials,
                  autodiscover=False,
                  access_type=DELEGATE)

parser = OrderParser()

for item in account.inbox.filter(
        sender=SENDER,
        subject__startswith=SUBJECT_PATTERN,
        datetime_received__range=(
            EWSDateTime.from_datetime(datetime(2025, 11, 27, 13, 43, 13, tzinfo=TZ_UTC)),
            EWSDateTime.now(UTC)
        )
).order_by("-datetime_received"):
    # print(item.subject, item.sender, item.datetime_received)
    order = parser.parse(item.body)
