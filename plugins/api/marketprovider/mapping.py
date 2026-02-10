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
    CERT_004 = 'Сертификат ТР 004 020'
    CERT_037 = 'Декларация 037'
    LAMP_HOUSING = 'Корпус лампы'
    LIGHT_HOUSING = 'Корпус светильника'
    LAMP_ELECTRICAL_CHARACTERISTICS = 'Электрические характеристики лампа'
    MARKETING = 'Маркетинг'
    OPTICAL_CHARACTERISTICS = 'Оптические характеристики'
    PRICE_STATUS = 'Прайс-статус'
    PROTECTION = 'Защита'
    QUALITY = 'Качество'
    WARRANTY = 'Гарантия'
    MULTIPLICITY = 'Кратность'
    LOGISTICS = 'Логистика'
    DIMENSIONS_CIRCLE = 'Габариты изделия (кругл)'
    DIMENSIONS = 'Габариты изделия'

    MARKETING_FILES = 'Маркетинг [файлы]'


class ProductAttrName:
    NAME = (ProductAttrGroupName.BASE, 'Название продукта')
    BRAND_NAME = (ProductAttrGroupName.BASE, 'Бренд')
    MAIN_IMAGE = (ProductAttrGroupName.BASE, 'Главное изображение')
    ORIGIN_COUNTRY = (ProductAttrGroupName.BASE, 'Страна производства')
    INNER_CODE = (ProductAttrGroupName.BASE, 'Внутренний артикул')
    VENDOR_CODE = (ProductAttrGroupName.BASE, 'Артикул производителя')
    EAN_UPC = (ProductAttrGroupName.BASE, 'EAN / UPC')

    CERT_004_NUM = (ProductAttrGroupName.CERT_004, 'Номер сертификата ТР 004/020')

    CERT_037_NUM = (ProductAttrGroupName.CERT_037, 'Номер декларации 037')

    BULB = (ProductAttrGroupName.LAMP_HOUSING, 'Колба')
    HOUSING_MATERIAL = (ProductAttrGroupName.LAMP_HOUSING, 'Материал корпуса')
    LAMP_TYPE = (ProductAttrGroupName.LAMP_HOUSING, 'Вид лампы')
    LAMP_CAP = (ProductAttrGroupName.LAMP_HOUSING, 'Цоколь')
    HOUSING_COLOR = (ProductAttrGroupName.LAMP_HOUSING, 'Цвет корпуса')
    DIFFUSER_TYPE = (ProductAttrGroupName.LAMP_HOUSING, 'Тип рассеивателя')

    MOUNTING_TYPE = (ProductAttrGroupName.LIGHT_HOUSING, 'Способ монтажа')

    MARKETING_NAME = (ProductAttrGroupName.MARKETING, 'Маркетинговое наименование')
    MARKETING_SERIES = (ProductAttrGroupName.MARKETING, 'Маркетинговая серия')
    SERIES_L4L = (ProductAttrGroupName.MARKETING, 'Серия L4L')

    POWER = (ProductAttrGroupName.LAMP_ELECTRICAL_CHARACTERISTICS, 'Мощность, Вт')
    VOLTAGE = (ProductAttrGroupName.LAMP_ELECTRICAL_CHARACTERISTICS, 'Входное напряжение AC, В')

    COLOR_TEMPERATURE = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'КЦТ, Кельвин')
    LUMINOUS_FLUX = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Световой поток, люмен')
    DIMMABLE = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Диммирование')
    BEAM_ANGLE = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Угол пучка, °')
    COLOR_RENDERING_INDEX = (ProductAttrGroupName.OPTICAL_CHARACTERISTICS, 'Индекс цветопередачи Ra, ≥')

    PREDECESSOR = (ProductAttrGroupName.PRICE_STATUS, 'Предшественник')
    WAREHOUSE_STATUS = (ProductAttrGroupName.PRICE_STATUS, 'Складской статус')
    LIFECYCLE_STATUS = (ProductAttrGroupName.PRICE_STATUS, 'Статус жизненного цикла')

    IP_CLASS = (ProductAttrGroupName.PROTECTION, 'Степень защиты IP')

    LIFESPAN = (ProductAttrGroupName.QUALITY, 'Срок службы, ч')

    WARRANTY_PERIOD = (ProductAttrGroupName.WARRANTY, 'Гарантия, лет')

    PCE_IN_INDIVISIBLE_PKG = (ProductAttrGroupName.MULTIPLICITY, 'Количество в неделимой упаковке, шт')
    ORDER_MULTIPLE_QTY = (ProductAttrGroupName.MULTIPLICITY, 'Кратность заказа, шт')
    ORDER_MIN_QTY = (ProductAttrGroupName.MULTIPLICITY, 'Минимальное кол-во к отгрузке, шт')

    PCE_ON_PALLET = (ProductAttrGroupName.LOGISTICS, 'Количество шт на паллете, шт')
    INDIVIDUAL_PKG_LENGTH = (ProductAttrGroupName.LOGISTICS, 'Длина индивидуальной упаковки, мм')
    INDIVIDUAL_PKG_HEIGHT = (ProductAttrGroupName.LOGISTICS, 'Высота индивидуальной упаковки, мм')
    INDIVIDUAL_PKG_WIDTH = (ProductAttrGroupName.LOGISTICS, 'Ширина индивидуальной упаковки, мм')
    INDIVIDUAL_PKG_WEIGHT = (ProductAttrGroupName.LOGISTICS,  'Масса в индивидуальной упаковке, г')
    TRANSPORT_PKG_LENGTH = (ProductAttrGroupName.LOGISTICS, 'Длина транспортной упаковки (EAN40), мм')
    TRANSPORT_PKG_HEIGHT = (ProductAttrGroupName.LOGISTICS, 'Высота транспортной упаковки (EAN40), мм')
    TRANSPORT_PKG_WIDTH = (ProductAttrGroupName.LOGISTICS, 'Ширина транспортной упаковки (EAN40), мм')
    TRANSPORT_PKG_WEIGHT = (ProductAttrGroupName.LOGISTICS,  'Масса транспортной упаковки (EAN40), г')
    PCE_IN_TRANSPORT_PKG = (ProductAttrGroupName.LOGISTICS, 'Количество в транспортной упаковке (EAN40)')

    CIRCLE_DIAMETER = (ProductAttrGroupName.DIMENSIONS_CIRCLE, 'Диаметр, мм')
    CIRCLE_HEIGHT = (ProductAttrGroupName.DIMENSIONS_CIRCLE, 'Высота, мм')
    CIRCLE_WEIGHT = (ProductAttrGroupName.DIMENSIONS_CIRCLE, 'Масса нетто, г')

    DIAMETER = (ProductAttrGroupName.DIMENSIONS, 'Диаметр, мм')
    WIDTH = (ProductAttrGroupName.DIMENSIONS, 'Ширина, мм')
    HEIGHT = (ProductAttrGroupName.DIMENSIONS, 'Высота, мм')
    LENGTH = (ProductAttrGroupName.DIMENSIONS, 'Длина, мм')
    WEIGHT = (ProductAttrGroupName.DIMENSIONS, 'Масса нетто, г')

    PRESENTATION = (ProductAttrGroupName.MARKETING_FILES, 'Презентация')
    LEAFLET = (ProductAttrGroupName.MARKETING_FILES, 'Листовка')
    PASSPORTS = (ProductAttrGroupName.MARKETING_FILES, 'Паспорт')

