import json
from dataclasses import dataclass, field
from typing import Callable

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
            resp.raise_for_response()
            return resp.json()

    def _paginate_requests(self, params: _RequestParams):
        page = 0
        while True:
            params.body['offset'] = params.body['limit'] * page
            resp = params.method(**params.request_args)
            resp.raise_for_response()
            result = resp.json()
            if result.get('items'):
                page += 1
                yield resp.json()
            else:
                break

    def get_categories(self, **kwargs):
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
