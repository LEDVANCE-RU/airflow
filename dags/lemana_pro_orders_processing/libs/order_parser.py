import re
from dataclasses import dataclass, field
from datetime import datetime

import bs4
from dataclasses_json import dataclass_json, config
from marshmallow import fields

from constants import TZ_MSK
from lemana_pro_orders_processing.libs.constants import ORDER_NUM_HEADER, ORDER_NUM_PREFIX, DELIVERY_DATE_HEADER, \
    IN_DATE_FORMAT, ARTICLE_PATTERN, PRICE_PATTERN, QTY_PATTERN, TR


datetime_iso_metadata = config(
    encoder=datetime.isoformat,
    decoder=datetime.fromisoformat,
    mm_field=fields.DateTime(format='iso')
)

@dataclass_json
@dataclass
class Item:
    article: str
    price: float
    qty: int


@dataclass_json
@dataclass
class Order:
    num: str
    items: list[Item]
    received_at: datetime = field(metadata=datetime_iso_metadata)
    delivery_date: datetime = field(metadata=datetime_iso_metadata)


class OrderParser:
    def __init__(self):
        self._soup = None

    def parse(self, html_contents: str, received_at: datetime) -> Order:
        self._soup = bs4.BeautifulSoup(html_contents, features="lxml")
        order_num_header_node = self._soup.find(string=ORDER_NUM_HEADER).find_parent(TR).find_next(TR)
        order_num = order_num_header_node.get_text(strip=True).replace(ORDER_NUM_PREFIX, '')

        delivery_date_header_node = self._soup.find(string=DELIVERY_DATE_HEADER).find_parent(TR).find_next(TR)
        delivery_date = (datetime.strptime(delivery_date_header_node.get_text(strip=True), IN_DATE_FORMAT)
                         .replace(tzinfo=TZ_MSK))

        items = self._parse_items()

        return Order(
            num = order_num,
            received_at=received_at,
            delivery_date=delivery_date,
            items=items
        )

    def _parse_items(self) -> list[Item]:
        values = []
        for pattern in [ARTICLE_PATTERN, PRICE_PATTERN, QTY_PATTERN]:
            nodes = self._soup.find_all(string=pattern)
            values.append([re.search(pattern, n.get_text(strip=True)).group(1) for n in nodes])

        return [Item(article=v[0],
                     price=float(v[1]),
                     qty=int(v[2])) for v in zip(*values)]

