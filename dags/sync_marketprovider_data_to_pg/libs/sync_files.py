import logging
import os
import posixpath
import tempfile
import urllib.parse as url_parse

import pandas
import requests
from paramiko.sftp_client import SFTPClient

from db_model.db_broker import DbBroker
from db_model.marketprovider.model import Product
from sync_marketprovider_data_to_pg.libs.constants import DEST_FILES_ROOT_DIRNAME


def upload_data(sftp_client: SFTPClient):
    db_broker = DbBroker()
    stmt = db_broker.get_marketprovider_products(return_stmt=True)
    df = pandas.read_sql(stmt, db_broker.session.connection())

    # timezone-aware objects are not supported by Excel,
    # thus convert all datetime values with tz to UTC and then remove timezone info
    for col in df.select_dtypes(include=['datetimetz']).columns:
        if df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        temp_filepath = tmp.name

    try:
        df.to_excel(tmp.name, index=False, sheet_name='Products')
        logging.info(f"Product data exported to '%s'", temp_filepath)
        remote_filepath = 'export.xlsx'
        sftp_client.put(temp_filepath, remote_filepath)
        logging.info("Product data uploaded to %s on SFTP-server", remote_filepath)
    finally:
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)


def upload_files(sftp_client: SFTPClient):
    # check if directory for files exists
    _mkdir_if_not_exists(sftp_client, DEST_FILES_ROOT_DIRNAME)
    sftp_client.chdir(DEST_FILES_ROOT_DIRNAME)

    db_broker = DbBroker()
    for f in db_broker.get_marketprovider_product_files_to_download():
        product_id = getattr(f, Product.id.key)
        subdir_name = str(product_id)
        main_image_url = getattr(f, Product.main_image_url.key)
        # create subdir if not exists
        _mkdir_if_not_exists(sftp_client, subdir_name)
        parsed_url = url_parse.urlsplit(main_image_url)
        query = url_parse.parse_qs(parsed_url.query)
        remote_filepath = url_parse.unquote(query['path'][0])
        ext = os.path.splitext(remote_filepath)[1]
        with requests.get(main_image_url, stream=True) as resp:
            resp.raise_for_status()
            size = int(resp.headers.get('content-length', 0))
            remote_relpath = posixpath.join(subdir_name, f'main_image{ext}')
            sftp_client.putfo(resp.raw, remote_relpath, size)
        db_broker.update_marketprovider_product_main_image_relpath(product_id, remote_relpath)

        size_kb = size / 1024
        logging.info('Main image (%.1f KB) of product %s synced to SFTP', size_kb, product_id)


def _mkdir_if_not_exists(sftp_client: SFTPClient, dir_relpath: str):
    try:
        sftp_client.stat(dir_relpath)
    except IOError:
        sftp_client.mkdir(dir_relpath)
