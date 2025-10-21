import os
from dataclasses import dataclass, InitVar, field
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import StrEnum
from typing import Iterable, Any

from airflow.providers.smtp.hooks.smtp import SmtpHook


class MimeAppTypeMap(StrEnum):
    DEFAULT = 'octet-stream'
    EXCEL = 'vnd.openxmlformats-officedocument.spreadsheetml.sheet'


@dataclass
class Attachment:
    filepath: str
    mime_type: MimeAppTypeMap | str = MimeAppTypeMap.DEFAULT
    filename: InitVar[str] = None

    _filename: str = field(init=False, repr=False)

    def __post_init__(self, filename):
        self._filename = filename if type(filename) is not property else None

    @property
    def filename(self) -> str:
        return self._filename if self._filename is not None else self.get_src_filename

    @filename.setter
    def filename(self, value: str):
        self._filename = value

    @property
    def get_src_filename(self) -> str:
        return os.path.basename(self.filepath)


class SmtpExtHook(SmtpHook):
    def send_email_smtp(self, *,
                        to: str | Iterable[str],
                        subject: str | None = None,
                        html_content: str | None = None,
                        from_email: str | None = None,
                        files: list[Attachment] | None = None,
                        dryrun: bool = False,
                        cc: str | Iterable[str] | None = None,
                        bcc: str | Iterable[str] | None = None,
                        mime_subtype: str = "mixed",
                        mime_charset: str = "utf-8",
                        custom_headers: dict[str, Any] | None = None,
                        **kwargs,):
        conn = self.get_conn()
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_email or conn.from_email

        addressee_types_map = {
            'To': to,
            'Cc': cc,
            'Bcc': bcc
        }
        recipients = []
        for k, v in addressee_types_map.items():
            if not v:
                continue
            recipients_batch = v if isinstance(v, list) else [v]
            msg[k] = ', '.join(recipients_batch)
            recipients += recipients_batch

        msg.attach(MIMEText(html_content, 'html'))

        for att in files:
            with open(att.filepath, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype=att.mime_type)
                attachment.add_header('Content-Disposition', 'attachment', filename=att.filename)
                msg.attach(attachment)

        smtp_client = conn.smtp_client
        smtp_client.sendmail(msg['From'], recipients, msg.as_string())
        smtp_client.quit()
