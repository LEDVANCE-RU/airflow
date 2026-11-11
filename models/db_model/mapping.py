from enum import StrEnum


class QuerySuccessorIcMap(StrEnum):
    IC_UUID = 'ic_uuid'
    IC = 'ic'
    IC_LIFECYCLE_STATUS = 'lifecycle_status'
    IC_PRIORITY = 'priority'
    NOMENCLATURE_UUID = 'nomenclature_uuid'
    ARTICLE = 'article'
    SUCCESSOR_IC_UUID = 'successor_ic_uuid'
    SUCCESSOR_IC = 'successor_ic'
    SUCCESSOR_IC_LIFECYCLE_STATUS = 'successor_ic_lifecycle_status'
    SUCCESSOR_IC_PRIORITY = 'successor_ic_priority'


class QueryIcPackageMap(StrEnum):
    NOMENCLATURE_UUID = 'nomenclature_uuid'
    SKU = 'sku'
    TNVED = 'tnved'
    OKPD2 = 'okpd2'
    IC_UUID = 'ic_uuid'
    IC = 'ic'
    IC_LIFECYCLE_STATUS = 'lifecycle_status'
    IC_PRIORITY = 'priority'
    CONTAINS_BATTERY = 'contains_battery'
    SUPPLIER_UUID = 'supplier_uuid'
    SUPPLIER_NAME = 'supplier_name'
    EAN = 'ean'
    LEVEL = 'level'
    QTY = 'qty'
    LENGTH = 'length'
    WIDTH = 'width'
    HEIGHT = 'height'
    WEIGHT = 'weight'
