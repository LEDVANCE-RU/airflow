from airflow.sdk import Variable

from hooks.smtp import SmtpExtHook, Attachment, MimeAppTypeMap


EMAIL_SUBJECT = "Обнуление остатков по IC"
EMAIL_BODY = """
<html>
    <head>
        <meta charset="utf-8">
    </head>
    <body>
        <p>Добрый день!</p>
        <p>Во вложении находится перечень IC, по которым произошло обнуление остатков на складе и не ожидается 
        поступлений. Просьба проверить актуальность ВГХ по соответствующим артикулам и 
        статусов жизненного цикла IC.</p>
        <p>
            <font color="gray"><i>Данное письмо было сформировано и отправлено автоматически.</i></font>
        </p>
    </body>
</html>"""


def send_by_email(fp: str, out_filename: str):
    addressees = Variable.get('notify_about_retired_stock_emails', deserialize_json=True)  #type: dict
    smtp_hook = SmtpExtHook('smtp_sys_tech')
    smtp_hook.send_email_smtp(
        to=addressees.get('to'),
        cc=addressees.get('cc'),
        bcc=addressees.get('bcc'),
        subject=EMAIL_SUBJECT,
        html_content=EMAIL_BODY,
        files=[Attachment(fp, mime_type=MimeAppTypeMap.EXCEL, filename=out_filename)]
    )
