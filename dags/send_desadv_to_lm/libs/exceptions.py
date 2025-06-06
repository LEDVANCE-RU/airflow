class ValidationError(Exception):
    ...


class ValuesNumDiscrepancyError(ValidationError):
    ...


class NoExpenditureOrdersFoundError(ValidationError):
    def __init__(self, expenditure_orders: list[str]):
        msg = f"Расходные ордера не найдены: {expenditure_orders}"
        super().__init__(msg)


class PackageItemsInconsistencyError(ValidationError):
    ...


class UnknownPackageTypeError(ValidationError):
    ...


class NonUniquePackageError(ValidationError):
    ...


class NonOccupiedPackageError(ValidationError):
    ...


class OrphanedItemsError(ValidationError):
    ...
