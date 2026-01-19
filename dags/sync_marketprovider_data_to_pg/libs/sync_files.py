import logging
import os
import posixpath
import urllib.parse as url_parse

import requests
from paramiko.sftp_client import SFTPClient

from db_model.db_broker import DbBroker
from db_model.marketprovider.model import Product
from sync_marketprovider_data_to_pg.libs.constants import DEST_FILES_ROOT_DIRNAME


def download_files(sftp_client: SFTPClient):
    # check if directory for files exists
    try:
        sftp_client.stat(DEST_FILES_ROOT_DIRNAME)
    except IOError:
        sftp_client.mkdir(DEST_FILES_ROOT_DIRNAME)
    sftp_client.chdir(DEST_FILES_ROOT_DIRNAME)

    db_broker = DbBroker()
    for f in db_broker.get_marketprovider_product_files_to_download():
        product_id = getattr(f, Product.id.key)
        subdir_name = str(product_id)
        main_image_url = getattr(f, Product.main_image_url.key)
        # create subdir if not exists
        try:
            sftp_client.stat(subdir_name)
        except IOError:
            sftp_client.mkdir(subdir_name)
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
