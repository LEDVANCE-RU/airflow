import json
import logging
import os.path
import re
from datetime import datetime

import pandas
from airflow.providers.samba.hooks.samba import SambaHook
from paramiko.sftp_client import SFTPClient
from smbprotocol.exceptions import SMBOSError

from constants import TZ_MSK
from db_model.db_broker import DbBroker
from process_wms_orders_with_fairmarks.libs.constants import TRG_HEADERS, FAIRMARK_ARTICLE_PATTERN, \
    LAST_SFTP_WMS_ORDERS_READ_KEY


def parse_marks(items: list[dict], level: int = 0):
    if not items:
        return None
    marks = []
    for item in items:
        mark = item['ЗначениеШтрихкода']
        subitems = item.get('ВложенныеШтрихкоды')
        if subitems:
            child_marks = parse_marks(subitems, level+1)
            for child in child_marks:
                if not isinstance(child, tuple):
                    child = (child,)
                marks.append((mark,) + child)
        else:
            article = re.findall(FAIRMARK_ARTICLE_PATTERN, mark)
            marks.append((mark, article[0] if article else ''))
    return marks if level>0 else [m[::-1] for m in marks]


def transform_file(sftp_client: SFTPClient, sftp_fpath: str,
                   smb_session: SambaHook, smb_out_dpath: str) -> int:
    with sftp_client.open(sftp_fpath) as fo:
        out_fname = os.path.splitext(os.path.basename(sftp_fpath))[0]
        out_fpath = os.path.join(smb_out_dpath, f'{out_fname}.xlsx')
        try:
            smb_session.stat(out_fpath)
            logging.warning("Файл '%s' уже существует. Пропускаем...", out_fpath)
            return 0
        except SMBOSError:
            pass

        d = json.load(fo)
        file_attrs = fo.stat()
        mtime_ts = file_attrs.st_mtime
        if not (d.get('ОрдерПроконтролирован') or d.get("ПриходныйОрдерПринят")):
            return mtime_ts
        # items = [(item['Номенклатура']['Артикул'], item['Количество']) for item in d['Строки']]
        # df_items = pandas.DataFrame(items, columns=['Артикул', 'Количество'])
        marks = parse_marks(d['ТЧ_ШтрихкодыУпаковок'])
        df_marks = pandas.DataFrame(marks)
        df_marks.columns = TRG_HEADERS[:len(df_marks.columns)]

    with smb_session.open_file(out_fpath, mode='wb') as out_fo:
        with pandas.ExcelWriter(out_fo) as writer:
            #df_items.to_excel(writer, sheet_name="Товары", index=False)
            df_marks.to_excel(writer, sheet_name="Коды маркировки", index=False)

    return mtime_ts


def transform_files(sftp_client: SFTPClient, sftp_fnames: list[str], sftp_dpath: str,
                    smb_hook: SambaHook, smb_out_dpath: str):
    with smb_hook as smb_session:
        smb_session.makedirs(smb_out_dpath, exist_ok=True)
        max_mtime_ts = 0
        for sftp_fname in sftp_fnames:
            sftp_fpath = os.path.join(sftp_dpath, sftp_fname)
            try:
                mtime_ts = transform_file(sftp_client, sftp_fpath, smb_session, smb_out_dpath)
            except Exception as e:
                logging.exception(e)
                continue
            max_mtime_ts = max(max_mtime_ts, mtime_ts)

    if not max_mtime_ts:
        return

    db_broker = DbBroker()
    db_broker.set_runtime_state(LAST_SFTP_WMS_ORDERS_READ_KEY,
                                datetime.fromtimestamp(max_mtime_ts, TZ_MSK),
                                commit=True)
