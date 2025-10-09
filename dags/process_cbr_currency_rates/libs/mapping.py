from dataclasses import dataclass
from enum import StrEnum


@dataclass(frozen=True)
class Field:
    name: str
    type: str


class CbrFieldsMap(StrEnum):
    CURRENCY = 'currency'
    RATE_RUB = 'rate_rub'
    DATE = 'date'

    @classmethod
    def dest_map(cls) -> dict[str, 'Field']:
        return {
            cls.CURRENCY: Field('currency', 'character varying(20)'),
            cls.RATE_RUB: Field('rate_rub', 'numeric(20, 4)'),
            cls.DATE: Field('date', 'date'),
        }

    @classmethod
    def dest_columns(cls) -> list[str]:
        return [field.value for field in cls]

