from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    type: str


@dataclass(frozen=True)
class PackageFieldsMap:
    _ALLOWED_LEVELS = [
        'PCE',
        'EAN10',
        'EAN20',
        'EAN40',
        'EAN50',
        'EAN60'
    ]

    code = 'code'
    article = 'article'
    description = 'description'
    ic = 'ic'
    ean = 'ean'
    level = 'level'
    enum = 'enumerator'
    denom = 'denominator'
    qty = 'qty'

    @classmethod
    def allowed_levels(cls):
        return cls._ALLOWED_LEVELS

    @classmethod
    def src_map(cls) -> dict[str, Field]:
        return {
            'Номенклатура.Код': Field(cls.code, 'string'),
            'Номенклатура.Артикул': Field(cls.article, 'string'),
            'Номенклатура.Description': Field(cls.description, 'string'),
            'Характеристика (WA)': Field(cls.ic, 'string'),
            'Штрихкод (WA)': Field(cls.ean, 'string'),
            'Уровень упаковки (WA)': Field(cls.level, 'string'),
            'Числитель': Field(cls.enum, 'float64'),
            'Знаменатель': Field(cls.denom, 'float64')
        }

    @classmethod
    def dest_map(cls) -> dict[str, Field]:
        m = {
            cls.code: Field('code', 'varchar'),
            cls.article: Field('article', 'varchar'),
            cls.description: Field('description', 'varchar'),
            cls.ic: Field('ic', 'varchar')
        }
        for lvl in cls.allowed_levels():
            col_name = lvl.lower()
            m['.'.join((cls.ean, lvl))] = Field(col_name, 'varchar')
            m['.'.join((cls.qty, lvl))] = Field(f"{col_name}_qty", 'numeric')
        return m