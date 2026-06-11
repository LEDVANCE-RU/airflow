from dataclasses import dataclass

@dataclass(frozen=True)
class Field:
    name: str
    type: str

class SiFieldsMap:

    @classmethod
    def marked_stock_src_map(cls) -> dict[str, str]:
        return {
            'Характеристика': 'ic',
            'Артикул': 'ean',
            'Характеристика.(WA) Проект IC': 'project_ic',
            'Номенклатура': 'description',
            'Заказ на отгрузку.Давалец.Код': 'customer_id',
            'Заказ на отгрузку.Давалец.Наименование': 'customer',
            'Заказ на отгрузку.Date': 'order_date',
            'Заказ на отгрузку.Дата': 'order_date',
            'Заказ на отгрузку.Number': 'order_id',
            'Заказ на отгрузку.Номер': 'order_id',
            'Заказ на отгрузку.Номер проекта': 'crm_id',
            'Заказ на отгрузку.Соглашение.Менеджер': 'kam',
            'Заказ на отгрузку.Номер по данным клиента': 'order_id_customer',
            'Сейчас.В наличии': 'available_stock',
            'Сейчас.Отгружается': 'shipped',
            'Сейчас.В резерве': 'reserved',
            'Сейчас.Доступно': 'free_stock',
            'Сейчас.Ожидается.В резерве': 'backorder',
            'Сейчас.Ожидается.Всего.К обеспечению': 'supply_needed',
        }

    @classmethod
    def marked_stock_dest_map(cls) -> dict[str, Field]:
        return {
            'ic': Field('ic', 'VARCHAR(200)'),
            'ean': Field('ean', 'VARCHAR(200)'),
            'project_ic': Field('project_ic', 'VARCHAR(200)'),
            'description': Field('description', 'VARCHAR(200)'),
            'customer_id': Field('customer_id', 'INTEGER'),
            'customer': Field('customer', 'VARCHAR(200)'),
            'order_date': Field('order_date', 'DATE'),
            'order_id': Field('order_id', 'VARCHAR(200)'),
            'crm_id': Field('crm_id', 'VARCHAR(200)'),
            'kam': Field('kam', 'VARCHAR(200)'),
            'order_id_customer': Field('order_id_customer', 'VARCHAR(200)'),
            'available_stock': Field('available_stock', 'INTEGER'),
            'shipped': Field('shipped', 'INTEGER'),
            'reserved': Field('reserved', 'INTEGER'),
            'free_stock': Field('free_stock', 'INTEGER'),
            'backorder': Field('backorder', 'INTEGER'),
            'supply_needed': Field('supply_needed', 'INTEGER'),
        }
