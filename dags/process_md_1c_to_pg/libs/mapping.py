from dataclasses import dataclass

@dataclass(frozen=True)
class Field:
    name: str
    type: str

class MdFieldsMap:
    
    @classmethod
    def products_src_map(cls) -> dict[str, str]:
        return {
            'Вид номенклатуры': 'items_type',
            'Характеристика номенклатуры': 'ic',
            'Номенклатура или вид номенклатуры.Description': 'description',
            'Характеристика номенклатуры.Номенклатура или вид номенклатуры.Артикул': 'ean',
            'Номенклатура или вид номенклатуры.Группа.Группа.Группа.Группа': 'bu',
            'Характеристика номенклатуры.(WA) Проект IC': 'project_ic',
            'Наименование для печати': 'printname',
            'Характеристика номенклатуры.(WA) Складской статус': 'wh_status',
            'Характеристика номенклатуры.(WA) Статус жизненного цикла': 'lifecycle_status',
            'Характеристика номенклатуры.(WA) Статус локализации': 'localization_status',
            'Cтрана происхождения (WA)': 'origin_country',
            'Поставщик (WA)': 'supplier',
            'Код ТНВ ЭД (WA)': 'tnv_code',
            'Номенклатура или вид номенклатуры.Группа': 'aug_key',
            'Номенклатура или вид номенклатуры.Группа.Группа': 'ag_key',
            'Номенклатура или вид номенклатуры.Группа.Группа.Группа': 'bs_key',
            'Характеристика номенклатуры.Deletion mark': 'deletion_mark',
            'Приоритет (WA)': 'priority'
        }

    @classmethod
    def products_dest_map(cls) -> dict[str, Field]:
        return {
            'items_type': Field('items_type', 'character varying(200)'),
            'ic': Field('ic', 'character varying(200)'),
            'description': Field('description', 'character varying(200)'),
            'ean': Field('ean', 'character varying(200)'),
            'bu': Field('bu', 'character varying(200)'),
            'project_ic': Field('project_ic', 'character varying(200)'),
            'printname': Field('printname', 'character varying(200)'),
            'wh_status': Field('wh_status', 'character varying(200)'),
            'lifecycle_status': Field('lifecycle_status', 'character varying(200)'),
            'localization_status': Field('localization_status', 'character varying(200)'),
            'origin_country': Field('origin_country', 'character varying(200)'),
            'supplier': Field('supplier', 'character varying(200)'),
            'tnv_code': Field('tnv_code', 'character varying(200)'),
            'aug_key': Field('aug_key', 'character varying(200)'),
            'ag_key': Field('ag_key', 'character varying(200)'),
            'bs_key': Field('bs_key', 'character varying(200)'),
            'deletion_mark': Field('deletion_mark', 'boolean'),
            'priority': Field('priority', 'integer'),
        }

    @classmethod
    def pricelist_src_map(cls) -> dict[str, str]:
        return {
            'Артикул': 'ean',
            'Номенклатура': 'description',
            'Наименование для печати': 'printname',
            'Характеристика ценообразования': 'pricing_type',
            'Упак.': 'package',
            'Цена': 'price_federal_wo_vat',
        }

    @classmethod
    def pricelist_dest_map(cls) -> dict[str, Field]:
        return {
            'ean': Field('ean', 'character varying(200)'),
            'description': Field('description', 'character varying(200)'),
            'printname': Field('printname', 'character varying(200)'),
            'pricing_type': Field('pricing_type', 'character varying(200)'),
            'package': Field('package', 'character varying(200)'),
            'price_federal_wo_vat': Field('price_federal_wo_vat', 'numeric(10, 2)'),
        }
