from enum import StrEnum


class CategoryField(StrEnum):
    ID = 'id'
    PARENT_ID = 'parentId'
    NAME = 'name'
    LEVEL = 'level'
    STATUS = 'status'

    ITEMS = 'items'


class ProductField(StrEnum):
    ID = 'id'
    STATUS_ID = 'statusId'
    CATEGORY = 'category'
    CATEGORY_ID = 'id'
    CREATED_AT = 'createdAt'
    UPDATED_AT = 'updatedAt'

    ITEMS = 'items'
    GROUPS = 'groups'
    GROUP_ATTRIBUTES = 'attributes'
    GROUP_NAME = 'name'
    ATTRIBUTE_NAME = 'name'
    ATTRIBUTE_VALUES = 'values'


class ProductAttrGroupName(StrEnum):
    BASE = 'Базовые характеристики'
    HOUSING = 'Корпус лампы'
    MARKETING = 'Маркетинг'
    PRICE_STATUS = 'Прайс-статус'
    LAMP_ELECTRICAL_CHARACTERISTICS = 'Электрические характеристики лампа'
    OPTICAL_CHARACTERISTICS = 'Оптические характеристики'
    QUALITY = 'Качество'
    WARRANTY = 'Гарантия'


class ProductAttrName:
    NAME = (ProductAttrGroupName.BASE, 'Название продукта')
    MAIN_IMAGE = (ProductAttrGroupName.BASE, 'Главное изображение')
    PREDECESSOR = (ProductAttrGroupName.BASE, 'Предшественник')
    ORIGIN_COUNTRY = (ProductAttrGroupName.BASE, 'Страна производства')
    INNER_CODE = (ProductAttrGroupName.BASE, 'Внутренний артикул')
    VENDOR_CODE = (ProductAttrGroupName.BASE, 'Артикул производителя')
    EAN_UPC = (ProductAttrGroupName.BASE, 'EAN / UPC')


    SERIES = (ProductAttrGroupName.HOUSING, 'Серия L4L')
    BULB = (ProductAttrGroupName.HOUSING, 'Колба')
    HOUSING_MATERIAL = (ProductAttrGroupName.HOUSING, 'Материал корпуса')
    LAMP_CAP = (ProductAttrGroupName.HOUSING, 'Цоколь')

    MARKETING_NAME = (ProductAttrGroupName.MARKETING, 'Маркетинговое наименование')

    POWER = (ProductAttrGroupName.LAMP_ELECTRICAL_CHARACTERISTICS, 'Мощность, Вт')
    VOLTAGE = (ProductAttrGroupName.LAMP_ELECTRICAL_CHARACTERISTICS, 'Входное напряжение AC, В')

    COLOR_TEMPERATURE = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'КЦТ, Кельвин')
    LUMINOUS_FLUX = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Световой поток, люмен')
    DIMMABLE = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Диммирование')
    BEAM_ANGLE = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Угол пучка, °')
    COLOR_RENDERING_INDEX = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Индекс цветопередачи Ra, ≥')

    LIFESPAN = (ProductAttrGroupName.QUALITY, 'Срок службы, ч')

    WARRANTY_PERIOD = (ProductAttrGroupName.WARRANTY, 'Гарантия, лет')