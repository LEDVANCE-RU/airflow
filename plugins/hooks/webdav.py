from urllib.parse import urljoin

from airflow.hooks.base import BaseHook
from webdav3.client import Client


class WebDAVHook(BaseHook):
    def __init__(self, conn_id='webdav_default'):
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self) -> Client:
        conn = self.get_connection(self.conn_id)
        options = {
            'webdav_hostname': conn.host,
            'webdav_login': conn.login,
            'webdav_password': conn.password,
            'webdav_extra': conn.extra_dejson
        }
        return Client(options)

    def get_full_path(self, relative_path):
        conn = self.get_connection(self.conn_id)
        full_path = urljoin(conn.host.rstrip('/') + '/', relative_path.lstrip('/'))
        return full_path