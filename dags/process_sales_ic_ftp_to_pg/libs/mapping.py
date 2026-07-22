class SalesFieldsMap:
    @staticmethod
    def src_map() -> dict:
        return {
            'Период, месяц': 'period',
            'Артикул': 'ean',
            'Номенклатура.Наименование': 'name',
            'Характеристика': 'ic',
            'Контрагент.Рабочее наименование': 'customer',
            'Контрагент.Партнер.Код': 'customer_id',
            'Соглашение': 'agreement',
            'Номенклатура.Группа': 'aug_key',
            '(WA) Проект IC': 'project_ic',
            'Количество': 'pcs',
            'Выручка': 'niv',
        }

    @staticmethod
    def dest_columns() -> list:
        return [
            'period',
            'ean',
            'name',
            'ic',
            'customer',
            'customer_id',
            'agreement',
            'aug_key',
            'project_ic',
            'pcs',
            'niv',
        ]

