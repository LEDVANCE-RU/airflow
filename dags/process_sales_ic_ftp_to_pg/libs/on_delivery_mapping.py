class OnDeliveryFieldsMap:
    @staticmethod
    def src_map() -> dict:
        return {
            'Менеджер': 'manager',
            'Клиент.Код': 'customer_id',
            'Соглашение': 'agreement',
            'Автор': 'author',
            'Номер': 'number',
            'Адрес доставки': 'delivery_address',
            'Товары.Номенклатура.Артикул': 'ean',
            'Товары.Номенклатура': 'name',
            'Товары.Номенклатура.Группа': 'aug_key',
            'Дата перехода права собственности': 'ownership_transfer_date',
            'Товары.Сумма': 'niv',
            'Товары.Количество (в единицах хранения)': 'qty',
            'Товары.Характеристика.(WA) Проект IC': 'project_ic',
        }

    @staticmethod
    def dest_columns() -> list:
        return [
            'manager',
            'customer_id',
            'agreement',
            'author',
            'number',
            'delivery_address',
            'ean',
            'name',
            'aug_key',
            'ownership_transfer_date',
            'niv',
            'qty',
            'project_ic',
        ]
