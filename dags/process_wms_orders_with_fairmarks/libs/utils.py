from process_wms_orders_with_fairmarks.libs.constants import LAST_SFTP_WMS_ORDERS_READ_KEY


def get_last_read_runtime_variable_name(sftp_dpath: str):
    return f"{LAST_SFTP_WMS_ORDERS_READ_KEY}:{sftp_dpath}"