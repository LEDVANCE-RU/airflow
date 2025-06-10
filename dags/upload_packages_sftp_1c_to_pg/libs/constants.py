from enum import StrEnum


class ResultKeys(StrEnum):
    EXPORT = 'export'
    ISSUE_PACKAGE_DUPLICATES = 'package_duplicates'
    ISSUE_WRONG_PCE_QTY = 'wrong_pce_qty'

    @classmethod
    def issue_keys(cls) -> list:
        return [
            cls.ISSUE_PACKAGE_DUPLICATES,
            cls.ISSUE_WRONG_PCE_QTY
        ]


ISSUES_NOTIFICATION_SUBJECT = "Ошибки в мастер-данных упаковок номенклатуры"
ISSUES_NOTIFICATION_HTML = """
<html>
    <head>
        <meta charset="utf-8">
    </head>
    <body>
        <p>
            Добрый день!
        </p>
        <p>
            Обнаружены ошибки в мастер-данных упаковок номенклатуры:
            <ul>
                {issues}
            </ul>
        </p>
        <p>
            <font color="gray"><i>Данное письмо было сформировано и отправлено автоматически.</i></font>
        </p>
    </body>
</html>"""
ISSUES_NAMES_MAP = {
    ResultKeys.ISSUE_PACKAGE_DUPLICATES: 'Дубликаты уровней упаковок',
    ResultKeys.ISSUE_WRONG_PCE_QTY: 'Отсутствует штучная упаковка или неверно указана кратность штучной упаковки'
}