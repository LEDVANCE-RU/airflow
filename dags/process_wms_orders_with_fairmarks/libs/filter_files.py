from datetime import datetime, timedelta
import re

from paramiko.sftp_client import SFTPClient

from constants import TZ_MSK
from db_model.db_broker import DbBroker
from process_wms_orders_with_fairmarks.libs.constants import SFTP_FILENAME_PATTERN
from process_wms_orders_with_fairmarks.libs.utils import get_last_read_runtime_variable_name


def filter_files(sftp_client: SFTPClient, sftp_dpath: str):
    db_broker = DbBroker()
    last_read = db_broker.get_runtime_state(get_last_read_runtime_variable_name(sftp_dpath))
    last_read_dt = last_read.value_ts if last_read else datetime.now(tz=TZ_MSK) - timedelta(days=7)
    sftp_filenames = [f.filename for f in sftp_client.listdir_attr(sftp_dpath)
                      if re.match(SFTP_FILENAME_PATTERN, f.filename)
                      and datetime.fromtimestamp(f.st_mtime, TZ_MSK) > last_read_dt]
    return sftp_filenames
