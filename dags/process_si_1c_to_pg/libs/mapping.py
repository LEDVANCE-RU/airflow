from dataclasses import dataclass

@dataclass(frozen=True)
class Field:
    name: str
    type: str

class SiFieldsMap:

    @classmethod
    def stock_1c_src_map(cls) -> dict[str, str]:
        return {
            'Характеристика': 'ic',
            'Артикул': 'ean',
            'Характеристика.(WA) Проект IC': 'project_ic',
            'Номенклатура': 'description',
            'Заказ на отгрузку.Давалец.Код': 'customer_id',
            'Заказ на отгрузку.Давалец.Наименование': 'customer',
            'Заказ на отгрузку.Date': 'order_date',
            'Заказ на отгрузку.Number': 'order_id',
            'Заказ на отгрузку.Номер проекта': 'crm_id',
            'Заказ на отгрузку.Соглашение.Менеджер': 'kam',
            'Заказ на отгрузку.Номер по данным клиента': 'order_id_customer',
            'Сейчас.В наличии': 'available_stock',
            'Сейчас.Отгружается': 'shipped',
            'Сейчас.В резерве': 'reserved',
            'Сейчас.Доступно': 'free_stock',
            'Сейчас.Ожидается.В резерве': 'backorder',
            'Сейчас.К обеспечению': 'supply_needed',
            'Сейчас.Ожидается.Всего.К обеспечению': 'supply_needed',
        }

    @classmethod
    def stock_1c_dest_map(cls) -> dict[str, Field]:
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
        
    @classmethod
    def open_po_ic_src_map(cls) -> dict[str, str]:
        return {
            'Характеристика': 'ic',
            'Артикул': 'ean',
            'Заказ на отгрузку': 'shipping_order',
            'Дата доступности': 'delivery_date',
            'Заказ на поступление': 'purchase_order',
            'Номенклатура.Description': 'description',
            'Заказ на поступление.Номер': 'po_id',
            'Заказ на поступление.Контрагент': 'supplier',
            'Заказ на отгрузку.Давалец': 'customer',
            'Заказ на отгрузку.Number': 'order_id',
            'Заказ на поступление.Date': 'po_date',
            'Ожидается.Поступит': 'po_qty',
        }

    @classmethod
    def open_po_ic_dest_map(cls) -> dict[str, Field]:
        return {
            'ic': Field('ic', 'VARCHAR(200)'),
            'ean': Field('ean', 'VARCHAR(200)'),
            'shipping_order': Field('shipping_order', 'VARCHAR(200)'),
            'delivery_date': Field('delivery_date', 'DATE'),
            'purchase_order': Field('purchase_order', 'VARCHAR(200)'),
            'description': Field('description', 'VARCHAR(200)'),
            'po_id': Field('po_id', 'VARCHAR(200)'),
            'supplier': Field('supplier', 'VARCHAR(200)'),
            'customer': Field('customer', 'VARCHAR(200)'),
            'order_id': Field('order_id', 'VARCHAR(200)'),
            'po_date': Field('po_date', 'DATE'),
            'po_qty': Field('po_qty', 'INTEGER'),
        }

    @classmethod
    def transit_src_map(cls) -> dict[str, str]:
        return {
            'Приходный ордер на товары.Отправитель': 'supplier',
            'Приходный ордер на товары': 'purchasing_doc',
            'Приходный ордер на товары.(WA) Номер при создании': 'purchasing_doc_number',
            'Приходный ордер на товары.Date': 'purchasing_doc_date',
            'Приходный ордер на товары.Номер входящего документа': 'invoice_number',
            'Номенклатура': 'description',
            'Приходный ордер на товары.Дата входящего документа': 'doc_date',
            'Характеристика': 'ic',
            'Распоряжение': 'po',
            'Дата поступления': 'delivery_date',
            'Приходный ордер на товары.Статус': 'status',
            'Приходный ордер на товары.Posted': 'posted',
            'Номенклатура.Артикул': 'ean',
            'Распоряжение.Number': 'po_number',
            'Распоряжение.Date': 'po_date',
            'Количество': 'po_qty',
        }

    @classmethod
    def transit_dest_map(cls) -> dict[str, Field]:
        return {
            'supplier': Field('supplier', 'VARCHAR(200)'),
            'purchasing_doc': Field('purchasing_doc', 'VARCHAR(200)'),
            'purchasing_doc_number': Field('purchasing_doc_number', 'VARCHAR(200)'),
            'purchasing_doc_date': Field('purchasing_doc_date', 'DATE'),
            'invoice_number': Field('invoice_number', 'VARCHAR(200)'),
            'description': Field('description', 'VARCHAR(200)'),
            'doc_date': Field('doc_date', 'DATE'),
            'ic': Field('ic', 'VARCHAR(200)'),
            'po': Field('po', 'VARCHAR(200)'),
            'delivery_date': Field('delivery_date', 'DATE'),
            'status': Field('status', 'VARCHAR(200)'),
            'posted': Field('posted', 'VARCHAR(200)'),
            'ean': Field('ean', 'VARCHAR(200)'),
            'po_number': Field('po_number', 'VARCHAR(200)'),
            'po_date': Field('po_date', 'DATE'),
            'po_qty': Field('po_qty', 'INTEGER'),
        }
        
    @classmethod
    def stock_for_customer_src_map(cls) -> dict[str, str]:
        return {
            'Артикул': 'ean',
            'Номенклатура': 'description',
            'Ед. изм.': 'unit',
            'Сейчас.В наличии': 'available_stock',
            'Сейчас.Отгружается': 'shipped',
            'Сейчас.В резерве': 'reserved',
            'Сейчас.Доступно': 'free_stock',
            'Сейчас.Ожидается.В резерве': 'backorder',
            'Сейчас.К обеспечению': 'supply_needed',
        }

    @classmethod
    def stock_for_customer_dest_map(cls) -> dict[str, Field]:
        return {
            'ean': Field('ean', 'VARCHAR(200)'),
            'description': Field('description', 'VARCHAR(200)'),
            'unit': Field('unit', 'VARCHAR(200)'),
            'available_stock': Field('available_stock', 'INTEGER'),
            'shipped': Field('shipped', 'INTEGER'),
            'reserved': Field('reserved', 'INTEGER'),
            'free_stock': Field('free_stock', 'INTEGER'),
            'backorder': Field('backorder', 'INTEGER'),
            'supply_needed': Field('supply_needed', 'INTEGER'),
        } 