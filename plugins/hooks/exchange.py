from airflow.hooks.base import BaseHook
from exchangelib import Credentials, Configuration, Account, DELEGATE


class ExchangeHook(BaseHook):
    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        self.account = None

    def get_conn(self) -> Account:
        conn = self.get_connection(self.conn_id)
        credentials = Credentials(conn.login, conn.password)
        config = Configuration(server=conn.host, credentials=credentials)
        account = Account(conn.login,
                          config=config,
                          credentials=credentials,
                          autodiscover=False,
                          access_type=DELEGATE)
        return account

    def iter_inbox(self, **kwargs):
        return (item for item in self.account.inbox.filter(**kwargs).order_by("-datetime_received"))
