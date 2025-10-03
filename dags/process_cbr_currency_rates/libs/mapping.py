from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    type: str


class CbrFieldsMap:
    @classmethod
    def dest_map(cls) -> dict[str, Field]:
        return {
            'currency': Field('currency', 'character varying(20)'),
            'rate_rub': Field('rate_rub', 'numeric(20, 4)'),
            'date': Field('date', 'date'),
        }

    @classmethod
    def dest_columns(cls) -> list[str]:
        return list(cls.dest_map().keys())

