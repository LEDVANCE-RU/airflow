import json
import logging
import os
import time

from airflow import DAG
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.sdk import task, Variable
from datetime import datetime

from constants import TZ_MSK
from send_desadv_to_lm.libs.constants import PROCESSED_DIR_NAME
from send_desadv_to_lm.libs.report_reader import ReportReader
from send_desadv_to_lm.libs.sender import DesadvSender

TRANSFORMED_FILENAME_SUFFIX = '_t.parquet'


def get_local_tmp_dir_path():
    return os.path.join(Variable.get('tmp_dir_path'), 'desadv_lm')


def get_smb_hook():
    return SambaHook('smb_desadv_input')


def get_input_dir_path():
    return Variable.get('lm_desadv_sender_input_dir_path')


with DAG(
    dag_id="send_desadv_to_lm",
    start_date=datetime(2025, 5, 1, tzinfo=TZ_MSK),
    schedule=None,
    catchup=False,
) as dag:
    @task()
    def download_task() -> str:
        local_dp = get_local_tmp_dir_path()
        remote_dp = get_input_dir_path()
        os.makedirs(local_dp, exist_ok=True)
        smb_hook = get_smb_hook()
        file_paths_map = dict()
        for f in smb_hook.scandir(remote_dp, '*.xlsx'):
            local_fp = os.path.join(local_dp, f.name)
            remote_fp = os.path.join(remote_dp, f.name)
            logging.info("Copying %s to %s...", remote_fp, local_fp)
            with smb_hook.open_file(remote_fp, 'rb') as src_f:
                with open(local_fp, 'wb') as dest_f:
                    dest_f.writelines(src_f.readlines())
                    dest_f.flush()
            file_paths_map[remote_fp] = local_fp

        out_fp = os.path.join(local_dp, f"{time.time()}_file_paths_map.json")
        with open(out_fp, 'w') as f:
            json.dump(file_paths_map, f)

        return out_fp


    @task()
    def transform_task(map_fp: str) -> str:
        transformed_file_paths_map = dict()
        with open(map_fp, 'r') as f:
            file_paths_map = json.load(f)
        for remote_fp, local_fp in file_paths_map.items():
            rr = ReportReader(local_fp, Variable.get('lm_desadv_sender_order_type'))
            df = rr.parse()
            transformed_fp = f"{local_fp}{TRANSFORMED_FILENAME_SUFFIX}"
            df.to_parquet(transformed_fp)
            transformed_file_paths_map[remote_fp] = transformed_fp
            os.remove(local_fp)
        os.remove(map_fp)

        out_fp = os.path.join(get_local_tmp_dir_path(), f"{time.time()}_file_paths_map.json")
        with open(out_fp, 'w') as f:
            json.dump(transformed_file_paths_map, f)

        return out_fp

    @task
    def send_task(map_fp: str):
        with open(map_fp, 'r') as f:
            transformed_file_paths_map = json.load(f)
        sender = DesadvSender(
            order_type=Variable.get('lm_desadv_sender_order_type'),
            wsdl_path=Variable.get('esphere_wsdl_path'),
            wsdl_username=Variable.get('esphere_username'),
            wsdl_password=Variable.get('esphere_password'),
            partner_gln=Variable.get('lm_gln'),
            test_mode=Variable.get('lm_desadv_sender_test_mode').strip().lower() == 'true'
        )

        for remote_fp, transformed_local_fp in transformed_file_paths_map.items():
            save_outputs = Variable.get('lm_desadv_sender_save_outputs').strip().lower() == 'true'
            outputs_dir_path = os.path.dirname(transformed_local_fp) if save_outputs else None
            sender.send(transformed_local_fp, outputs_dir_path=outputs_dir_path)

            os.remove(transformed_local_fp)
            remote_processed_dir_path = os.path.join(get_input_dir_path(), PROCESSED_DIR_NAME)
            remote_processed_fp = os.path.join(
                str(remote_processed_dir_path),
                f"{datetime.today().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(remote_fp)}"
            )

            smb_hook = get_smb_hook()
            smb_hook.makedirs(remote_processed_dir_path, exist_ok=True)
            smb_hook.rename(remote_fp, remote_processed_fp)
            logging.info("Файл '%s' перемещен в '%s'", remote_fp, remote_processed_fp)

        os.remove(map_fp)

    src_map_fp = download_task()
    transformed_map_fp = transform_task(src_map_fp)
    send_task(transformed_map_fp)
