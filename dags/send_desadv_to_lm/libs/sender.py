import datetime
import logging
import os

import pandas

from send_desadv_to_lm.libs.constants import OrderTypes
from send_desadv_to_lm.libs.eancom_creator import DesadvCreator
from send_desadv_to_lm.libs.esphere_client import ESphereClient
from send_desadv_to_lm.libs.field_map import BaseFields


class DesadvSender:
    def __init__(self, order_type: OrderTypes,
                 wsdl_path: str, wsdl_username: str, wsdl_password: str, partner_gln: str,
                 test_mode: bool = False):
        self.client = ESphereClient(wsdl_path, wsdl_username, wsdl_password)
        self.order_type = order_type
        self.partner_gln = partner_gln
        self.test_mode = test_mode

    def send(self, fp: str, outputs_dir_path: str = None):
        df = pandas.read_parquet(fp)
        for num in df[BaseFields.INVOICE].unique():
            if pandas.isna(num):
                continue
            df_inv = df[df[BaseFields.INVOICE] == num]
            exp_order, invoice = tuple(df_inv.iloc[0][[BaseFields.EXPENDITURE_ORDER, BaseFields.INVOICE]])
            logging.info("Формируется DESADV %s для расходного ордера %s...", invoice, exp_order)
            try:
                desadv_creator = DesadvCreator(df_inv)
                desadv = desadv_creator.generate()
                if outputs_dir_path:
                    with open(
                        os.path.join(
                            outputs_dir_path,
                            f"{datetime.datetime.today().strftime('%Y%m%d_%H%M%S')}_{exp_order}_{invoice}.xml"
                        ), 'w'
                    ) as f:
                        f.write(str(desadv))
                if not self.test_mode:
                    self.client.send_desadv(self.partner_gln, str(desadv))
            except Exception as e:
                logging.exception(e)
                raise
            logging.info("DESADV %s для расходного ордера %s отправлен.", invoice, exp_order)
