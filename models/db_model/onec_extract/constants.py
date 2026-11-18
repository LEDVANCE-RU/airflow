import uuid
from enum import StrEnum


class WarehouseUUID:
    GOODS = uuid.UUID('c3c4db62-d07b-11ee-a0f7-9b35e1ceb5f3')
    BLOCK = uuid.UUID('2d90f1d2-12b2-11ef-9917-04421acb8ad5')
    SHORTAGES = uuid.UUID('8e846478-9750-11ef-9919-04421acb8ad5')


class NomenclatureCertificateStatusUUID:
    ACTIVE = uuid.UUID('13b1279e-83d7-45f3-9bc7-06aa286c4417')
    INACTIVE = uuid.UUID('d7bff27d-5e06-4a34-a625-58c0e27bbeb7')


class NomenclatureCertificateType(StrEnum):
    CONFORMITY = 'Сертификат соответствия'
    CONFORMITY_DECLARATION = 'Декларация о соответствии'
    HYGIENIC = 'Гигиенический сертификат'
    FIRE_SAFETY = 'Пожарный сертификат'


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
    def active_and_undef_statuses(cls):
        return [
            cls.NEW,
            cls.REG,
            cls.EOL,
            None
        ]

    @classmethod
    def inactive_statuses(cls):
        return [cls.ARC]
