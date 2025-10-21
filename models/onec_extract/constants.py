import uuid
from enum import StrEnum

WAREHOUSE_OF_GOODS_UUID = uuid.UUID('c3c4db62-d07b-11ee-a0f7-9b35e1ceb5f3')

class IcLifecycleStatus(StrEnum):
    NEW = 'NEW - Новинка'
    REG = 'REG - Регулярная'
    EOL = 'EOL - Выводится'
    ARC = 'ARC - Архив'

    @classmethod
    def active_statuses(cls):
        return [
            cls.NEW,
            cls.REG,
            cls.EOL
        ]

    @classmethod
    def inactive_statuses(cls):
        return [cls.ARC]
