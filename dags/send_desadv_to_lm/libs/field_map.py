from enum import StrEnum

from bidict import bidict


class BaseFields(StrEnum):
    _PREFIX = 'B:'

    ORDER = f"{_PREFIX}order"
    CUSTOMER_ORDER_NUM = f"{_PREFIX}cust_order"
    CUSTOMER_ORDER_DATE = f"{_PREFIX}cust_order_date"
    DELIVERY_DATE = f"{_PREFIX}delivery_date"
    BUYER_GLN = f"{_PREFIX}buyer_gln"
    END_CUSTOMER_GLN = f"{_PREFIX}end_cust_gln"
    CUSTOMER_TIN = f"{_PREFIX}tin"
    CUSTOMER_ORDER_LINE = f"{_PREFIX}cust_order_line"
    ARTICLE = f"{_PREFIX}article"
    CUSTOMERS_ARTICLE = f"{_PREFIX}cust_article"
    ITEM_NAME = f"{_PREFIX}article_name"
    ISSUE = f"{_PREFIX}issue"
    ISSUE_DATE = f"{_PREFIX}issue_date"
    ISSUED_ITEM_QTY = f"{_PREFIX}issue_qty"
    INVOICE = f"{_PREFIX}invoice"
    INVOICE_DATE = f"{_PREFIX}invoice_date"
    INVOICE_SUM_WITH_VAT = f"{_PREFIX}doc_sum"
    INVOICE_VAT = f"{_PREFIX}doc_sum_vat"
    INVOICE_SUM_WITHOUT_VAT = f"{_PREFIX}doc_sum_wo_vat"
    DELIVERY_NOTE = f"{_PREFIX}delivery_note"
    EXPENDITURE_ORDER = f"{_PREFIX}exp_order"

    DELIVERY_NOTE_DATE = f"{_PREFIX}delivery_note_date"

    SELLER_GLN = f"{_PREFIX}seller_gln"
    SHIP_TO_GLN = f"{_PREFIX}ship_to_gln"

    @classmethod
    def map(cls):
        return bidict({
            "Заказ": cls.ORDER,
            "Номер по данным клиента": cls.CUSTOMER_ORDER_NUM,
            "Дата по данным клиента": cls.CUSTOMER_ORDER_DATE,
            "Дата доставки": cls.DELIVERY_DATE,
            "GLN Клиента": cls.BUYER_GLN,
            "GLN Грузополучателя": cls.END_CUSTOMER_GLN,
            "ИНН": cls.CUSTOMER_TIN,
            "№ стр. заказа": cls.CUSTOMER_ORDER_LINE,
            "Артикул номенклатуры": cls.ARTICLE,
            "Артикул партнера": cls.CUSTOMERS_ARTICLE,
            "Номенклатура": cls.ITEM_NAME,
            "РТУ": cls.ISSUE,
            "Дата": cls.ISSUE_DATE,
            "Количество": cls.ISSUED_ITEM_QTY,
            "СФ": cls.INVOICE,
            "Дата выставления": cls.INVOICE_DATE,
            "Сумма с НДС (Документа)": cls.INVOICE_SUM_WITH_VAT,
            "Сумма НДС (Документа)": cls.INVOICE_VAT,
            "Сумма (Документа)": cls.INVOICE_SUM_WITHOUT_VAT,
            "ТН": cls.DELIVERY_NOTE,
            "Номер": cls.EXPENDITURE_ORDER
        })

    @classmethod
    def src_types(cls):
        mapping = cls.map().inverse
        return {
            mapping[cls.CUSTOMER_ORDER_NUM]: 'string',
            mapping[cls.CUSTOMER_ORDER_LINE]: 'Int64',
            mapping[cls.BUYER_GLN]: 'string',
            mapping[cls.END_CUSTOMER_GLN]: 'string',
            mapping[cls.CUSTOMER_TIN]: 'string',
            mapping[cls.ARTICLE]: 'string',
            mapping[cls.CUSTOMERS_ARTICLE]: 'string',
        }

    @classmethod
    def date_fields(cls):
        return [cls.CUSTOMER_ORDER_DATE,
                cls.DELIVERY_DATE,
                cls.INVOICE_DATE]

    @classmethod
    def datetime_fields(cls):
        return [cls.ISSUE_DATE,
                cls.DELIVERY_NOTE_DATE]

    @classmethod
    def doc_num_fields(cls):
        return [cls.ORDER,
                cls.ISSUE,
                cls.INVOICE,
                cls.DELIVERY_NOTE,
                cls.EXPENDITURE_ORDER]

    @classmethod
    def types(cls):
        return {
            cls.CUSTOMER_ORDER_LINE: 'int32'
        }


class PackageFieldsBase:
    _PREFIX = 'P:'

    EXPENDITURE_ORDER = f"{_PREFIX}exp_order"
    TYPE = f"{_PREFIX}package_type"
    HEIGHT = f"{_PREFIX}height"
    LENGTH = f"{_PREFIX}length"
    WIDTH = f"{_PREFIX}width"
    WEIGHT = f"{_PREFIX}weight"
    NUM =  f"{_PREFIX}package_num"
    SSCC =  f"{_PREFIX}sscc"
    SORT_INDEX = f"{_PREFIX}sort_index"

    @classmethod
    def map(cls) -> bidict:
        return bidict({
            "Расходный ордер": cls.EXPENDITURE_ORDER,
            "Вес": cls.WEIGHT,
            "Высота": cls.HEIGHT,
            "Длина": cls.LENGTH,
            "Ширина": cls.WIDTH,
            "Тип упаковки": cls.TYPE
        })

    @classmethod
    def types(cls):
        return {
            cls.HEIGHT: 'float64',
            cls.LENGTH: 'float64',
            cls.WIDTH: 'float64',
            cls.WEIGHT: 'float64',
            cls.NUM: 'int32',
            cls.TYPE: str
        }


class PackageFieldsXD(PackageFieldsBase):
    PALLET_QTY = f"{PackageFieldsBase._PREFIX}pallet_qty"

    @classmethod
    def map(cls):
        d = super().map()
        d["Количество паллет"] = cls.PALLET_QTY
        return d

    @classmethod
    def multiple_valued_cols(cls):
        return [cls.TYPE,
                cls.LENGTH,
                cls.WIDTH,
                cls.HEIGHT,
                cls.WEIGHT]

    @classmethod
    def types(cls):
        t = super().types()
        t[cls.PALLET_QTY] = 'int32'
        return t


class PackageFieldsBBXD(PackageFieldsBase):
    @classmethod
    def map(cls):
        d = super().map()
        d["№ паллеты"] = cls.NUM
        return d



class ItemFieldsBase:
    _PREFIX = 'I:'

    EXPENDITURE_ORDER = f"{_PREFIX}exp_order"
    CUSTOMER_ORDER_LINE = f"{_PREFIX}cust_order_line"
    QTY = f"{_PREFIX}qty"
    PACKAGE_NUM = f"{_PREFIX}package_num"

    @classmethod
    def map(cls):
        return bidict({
            "Расходный ордер": cls.EXPENDITURE_ORDER,
            "№ стр. ИЗ": cls.CUSTOMER_ORDER_LINE,
            "Количество": cls.QTY,
            "№ паллеты": cls.PACKAGE_NUM
        })

    @classmethod
    def types(cls):
        return {
            cls.CUSTOMER_ORDER_LINE: 'int32',
            cls.QTY: 'float64',
            cls.PACKAGE_NUM: 'int32'
        }


class ItemFieldsXD(ItemFieldsBase):
    @classmethod
    def multiple_valued_cols(cls):
        return [cls.QTY,
                cls.PACKAGE_NUM]


class ItemFieldsBBXD(ItemFieldsBase):
    IC = f"{ItemFieldsBase._PREFIX}IC"

    @classmethod
    def map(cls):
        d = super().map()
        d["Характеристика"] = cls.IC
        return d