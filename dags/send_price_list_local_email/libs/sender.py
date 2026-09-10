import logging
import os

from hooks.smtp import Attachment, MimeAppTypeMap, SmtpExtHook

from send_price_list_local_email.libs.sharepoint import find_local_file

SMTP_CONN_ID = 'smtp_sys_tech'


def send_price_list_email(client, folder_info: dict, recipients: dict, tmp_dir: str):
    """Скачивает файл с 'LOCAL' в названии из folder_info['folder_path'] и
    отправляет его вложением на адреса из recipients ({'to': [...], 'cc': [...], 'bcc': [...]})."""
    file_name = find_local_file(client, folder_info['folder_path'])
    remote_file_path = os.path.join(folder_info['folder_path'], file_name)
    local_path = os.path.join(tmp_dir, file_name)

    to = recipients.get('to') or []
    cc = recipients.get('cc') or []
    bcc = recipients.get('bcc') or []

    try:
        client.download_sync(remote_path=remote_file_path, local_path=local_path)
        logging.info("Скачан файл '%s' -> '%s'", remote_file_path, local_path)

        smtp_hook = SmtpExtHook(SMTP_CONN_ID)
        smtp_hook.send_email_smtp(
            to=to,
            cc=cc,
            bcc=bcc,
            subject=f"Прайс-лист LOCAL: {folder_info['folder_name']}",
            html_content=f"<p>Во вложении свежий прайс-лист LOCAL из папки '{folder_info['folder_name']}'.</p>",
            files=[Attachment(local_path, mime_type=MimeAppTypeMap.EXCEL)],
        )
        logging.info("Прайс-лист отправлен: to=%s cc=%s bcc=%s", to, cc, bcc)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
            logging.info("Временный файл удалён: %s", local_path)
