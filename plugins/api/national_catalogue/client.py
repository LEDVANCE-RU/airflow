import logging
import time
from dataclasses import dataclass, field
from typing import Callable

import requests
from tenacity import retry, retry_if_exception, wait_exponential, before_sleep_log, stop_after_delay

from api.national_catalogue.constants import RATE_LIMIT_WINDOW

logger = logging.getLogger(__name__)


# filter for HTTP-error 429 ("Too Many Requests for url")
def is_429_error(exception):
    return (
        isinstance(exception, requests.exceptions.HTTPError)
        and exception.response is not None
        and exception.response.status_code == 429
    )


@dataclass
class _RequestParams:
    _BASE_URL = 'https://апи.национальный-каталог.рф/v3'

    method: Callable
    endpoint: str
    query: dict = field(default_factory=dict)
    offset: int = 0
    limit: int = 1
    paginate: bool = False

    @property
    def url(self) -> str:
        return f"{self._BASE_URL}/{self.endpoint}"

    @property
    def request_args(self) -> dict:
        return dict(url=self.url, params=self.query)


class NationalCatalogueApiClient:
    def __init__(self, api_key: str):
        self._api_key = api_key

    @retry(
        retry=retry_if_exception(is_429_error),
        stop=stop_after_delay(RATE_LIMIT_WINDOW),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    def _send_request(self, params: _RequestParams):
        params.query['apikey'] = self._api_key
        if params.offset is not None:
            params.query['offset'] = params.offset
        if params.limit is not None:
            params.query['limit'] = params.limit
        resp = params.method(**params.request_args)
        resp.raise_for_status()
        return resp.json()

    def list_products(self,
                      owner_inn: int | None = None,
                      brand_id: int | None = None,
                      cat_id: int | None = None,
                      offset: int = 0,
                      limit: int = 100):
        query = {
            'owner_inn': owner_inn,
            'brand_id': brand_id,
            'cat_id': cat_id
        }
        query = {k: v for k, v in query.items() if v is not None}
        request_params = _RequestParams(
            requests.get,
            endpoint="etagslist",
            query=query,
            offset=offset,
            limit=limit
        )
        resp = self._send_request(request_params)
        return resp

    def iter_all_products(self,
                          owner_inn: int | None = None,
                          brand_id: int | None = None,
                          cat_id: int | None = None):
        request_delay = (5 * 60) * (1 / 500)
        offset = 0
        limit = 100
        while True:
            resp = self.list_products(owner_inn=owner_inn, brand_id=brand_id, cat_id=cat_id, offset=offset, limit=limit)
            yield resp
            if len(resp['result']['goods']) < limit:
                break
            time.sleep(request_delay)
            offset += limit

    def get_product_details(self, good_id: int):
        params = _RequestParams(
            requests.get,
            endpoint="product",
            query={"good_id": good_id}
        )
        resp = self._send_request(params)
        return resp
