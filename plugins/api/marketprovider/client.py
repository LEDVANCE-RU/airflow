import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import requests


@dataclass
class _RequestParams:
    _BASE_URL = 'https://api.marketprovider.ru/api/v1'

    method: Callable
    endpoint: str
    token: str
    query: dict = field(default_factory=dict)
    body: dict = field(default_factory=dict)
    offset: int = None
    limit: int = None
    paginate: bool = False

    @property
    def headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "x-api-key": self.token
        }

    @property
    def url(self) -> str:
        return f"{self._BASE_URL}/{self.endpoint}"

    @property
    def request_args(self) -> dict:
        return dict(url=self.url, data=json.dumps(self.body), headers=self.headers)


class MarketProviderApiClient:
    def __init__(self, token: str):
        self.token = token

    def _send_request(self, params: _RequestParams):
        if params.offset is not None:
            params.body['offset'] = params.offset
        if params.limit is not None:
            params.body['limit'] = params.limit
        if params.paginate:
            return self._paginate_requests(params)
        else:
            resp = params.method(**params.request_args)
            return resp.json()

    def _paginate_requests(self, params: _RequestParams):
        page = 0
        while True:
            params.body['offset'] = params.body['limit'] * page
            resp = params.method(**params.request_args)
            result = resp.json()
            if result.get('items'):
                page += 1
                yield resp.json()
            else:
                break

    def get_catalogs(self, **kwargs):
        params = _RequestParams(
            method = requests.post,
            endpoint='categories/list',
            token=self.token,
            **self._unpack_pagination_data(**kwargs)
        )
        return self._send_request(params)

    def get_products_short(self, category_id: int, **kwargs):
        params = _RequestParams(
            method = requests.post,
            endpoint=f'categories/{category_id}/products-short-info',
            token=self.token,
            **self._unpack_pagination_data(**kwargs)
        )
        return self._send_request(params)

    def get_products_full(self, category_id: int, product_ids: list[int] = None, **kwargs):
        body = {}
        if product_ids:
            body['filters'] = {'productIds': product_ids}
        params = _RequestParams(
            method = requests.post,
            endpoint=f'categories/{category_id}/products-full-info',
            token=self.token,
            body=body,
            **self._unpack_pagination_data(**kwargs)
        )
        return self._send_request(params)

    @staticmethod
    def _unpack_pagination_data(**kwargs) -> dict:
        return {
            'limit': kwargs.get('limit', 100),
            'offset': kwargs.get('offset', 0),
            'paginate': kwargs.get('paginate', False)
        }


if __name__ == '__main__':
    mp = MarketProviderApiClient('')
    catalogs = mp.get_catalogs(limit=1000, offset=0)

    gen_products_short = mp.get_products_short(22302, paginate=True, limit=1000)
    products_short = []
    product_ids = []
    for product_batch in gen_products_short:
        items = product_batch['items']
        for item in items:
            updated_at = datetime.fromisoformat(item['updatedAt'])
            if datetime(2025, 12, 12, tzinfo=ZoneInfo("UTC")) <= updated_at < datetime(2026, 1, 12, tzinfo=ZoneInfo("UTC")):
                product_ids.append(item['id'])

    if product_ids:
        gen_products_full = mp.get_products_full(22302, product_ids=product_ids, paginate=True, limit=250)
        products_full = []
        for p in gen_products_full:
            products_full.append(p)

    pass
