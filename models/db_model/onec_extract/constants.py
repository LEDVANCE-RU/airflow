import uuid
from enum import StrEnum


class WarehouseUUID:
    GOODS = uuid.UUID('c3c4db62-d07b-11ee-a0f7-9b35e1ceb5f3')
    BLOCK = uuid.UUID('2d90f1d2-12b2-11ef-9917-04421acb8ad5')
    SHORTAGES = uuid.UUID('8e846478-9750-11ef-9919-04421acb8ad5')


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
