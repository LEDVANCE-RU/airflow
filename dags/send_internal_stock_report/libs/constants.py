INSERT_STOCK_REPORT_SQL = """
-- Создаем таблицу для результатов в схеме si
CREATE TABLE IF NOT EXISTS si.si_result (
    ic VARCHAR,
    ean VARCHAR,
    description VARCHAR,
    bu_with_uv VARCHAR,
    ag VARCHAR,
    aug VARCHAR,
    project_ic VARCHAR,
    wh_status VARCHAR,
    lifecycle_status VARCHAR,
    ean_group INTEGER,
    -- Стоковые показатели
    available_stock INTEGER,
    shipped_stock INTEGER,
    reserved_stock INTEGER,
    free_stock INTEGER,
    backorder_stock INTEGER,
    supply_needed_stock INTEGER,
    -- Транзитные показатели
    transit_1_10_d INTEGER,
    transit_10_20_d INTEGER,
    transit_20_30_d INTEGER,
    transit_30_45_d INTEGER,
    transit_45_60_d INTEGER,
    -- Открытые заказы по месяцам
    openpo_m INTEGER,
    openpo_m_plus_1 INTEGER,
    openpo_m_plus_2 INTEGER,
    openpo_m_plus_3 INTEGER,
    openpo_m_plus_4 INTEGER
);

-- Очищаем таблицу перед записью новых данных
TRUNCATE TABLE si.si_result;

CREATE INDEX IF NOT EXISTS idx_si_result_ic ON si.si_result(ic);
CREATE INDEX IF NOT EXISTS idx_si_result_ean ON si.si_result(ean);

-- Записываем результаты в таблицу
INSERT INTO si.si_result (
    ic, ean, description, bu_with_uv, ag, aug, project_ic, 
    wh_status, lifecycle_status, ean_group,
    available_stock, shipped_stock, reserved_stock, free_stock, backorder_stock, supply_needed_stock,
    transit_1_10_d, transit_10_20_d, transit_20_30_d, transit_30_45_d, transit_45_60_d,
    openpo_m, openpo_m_plus_1, openpo_m_plus_2, openpo_m_plus_3, openpo_m_plus_4
)
WITH products_1c AS (
    SELECT 
        ic,
        description,
        ean,
        project_ic,
        wh_status,
        lifecycle_status,
        aug_key,
        CASE 
            WHEN LEFT(ean, 1) = '4' THEN 4
            WHEN LEFT(ean, 1) = '3' THEN 3
            ELSE NULL
        END AS ean_group
    FROM md.products
    WHERE trim(aug_key) is not null
),

ag_structure AS (
    SELECT 
        aug_key,
        bu_with_uv,
        ag,
        aug
    FROM md.ag_structure
),

stock_1c AS (
    SELECT 
        ic,
        COALESCE(SUM(available_stock), 0) AS available_stock,
        COALESCE(SUM(shipped), 0) AS shipped_stock,
        COALESCE(SUM(reserved), 0) AS reserved_stock,
        COALESCE(SUM(free_stock), 0) AS free_stock,
        COALESCE(SUM(backorder), 0) AS backorder_stock,
        COALESCE(SUM(supply_needed), 0) AS supply_needed_stock
    FROM si.stock_1c
    GROUP BY ic
),

transit_data AS (
    WITH transit_base AS (
        SELECT 
            ic,
            ean,
            CASE 
                WHEN delivery_date IS NOT NULL THEN delivery_date 
                ELSE doc_date 
            END AS receipt_date,
            po_qty
        FROM si.transit
        WHERE ic IS NOT NULL AND invoice_number IS NOT NULL
    ),
    processed_transit AS (
        SELECT 
            ic,
            ean,
            CASE 
                WHEN (receipt_date - CURRENT_DATE) < 0 THEN 'arrived'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 0 AND 9 THEN '1-10 d'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 10 AND 19 THEN '10-20 d'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 20 AND 29 THEN '20-30 d'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 30 AND 44 THEN '30-45 d'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 45 AND 59 THEN '45-60 d'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 60 AND 89 THEN '60-90 d'
                WHEN (receipt_date - CURRENT_DATE) BETWEEN 90 AND 364 THEN '90-365 d'
                ELSE 'more_than_year'
            END AS diff_group,
            SUM(po_qty) AS transit_qty
        FROM transit_base
        GROUP BY ic, ean, receipt_date
    )
    SELECT 
        ic,
        COALESCE(SUM(CASE WHEN diff_group = '1-10 d' THEN transit_qty END), 0) AS transit_1_10_d,
        COALESCE(SUM(CASE WHEN diff_group = '10-20 d' THEN transit_qty END), 0) AS transit_10_20_d,
        COALESCE(SUM(CASE WHEN diff_group = '20-30 d' THEN transit_qty END), 0) AS transit_20_30_d,
        COALESCE(SUM(CASE WHEN diff_group = '30-45 d' THEN transit_qty END), 0) AS transit_30_45_d,
        COALESCE(SUM(CASE WHEN diff_group = '45-60 d' THEN transit_qty END), 0) AS transit_45_60_d
    FROM processed_transit
    GROUP BY ic
),

openpo_data AS (
    WITH open_po_base AS (
        SELECT 
            ic AS "Характеристика",
            ean AS "Артикул",
            delivery_date AS "Дата доступности",
            description AS "Номенклатура",
            po_id AS "Заказ на поступление.Номер",
            supplier AS "Заказ на поступление.Контрагент",
            po_qty AS "заказы"
        FROM si.open_po_ic
        WHERE ic IS NOT NULL AND po_qty IS NOT NULL
    ),
    
    transit_for_adjustment AS (
        SELECT 
            ic AS "Характеристика",
            purchasing_doc AS "Номер_Заказа_на_поставку",
            po_number,
            CASE 
                WHEN delivery_date IS NOT NULL THEN delivery_date 
                ELSE doc_date 
            END AS "Дата приемки на склад",
            SUM(po_qty) AS "Количество"
        FROM si.transit
        WHERE ic IS NOT NULL
        GROUP BY ic, purchasing_doc, po_number, delivery_date, doc_date
    ),
    
    adjusted_open_po AS (
        SELECT 
            op."Характеристика",
            op."Артикул",
            op."Дата доступности",
            op."Номенклатура",
            op."Заказ на поступление.Номер",
            op."Заказ на поступление.Контрагент",
            COALESCE(t."Количество", 0) AS "Транзит_Количество",
            CASE 
                WHEN COALESCE(t."Количество", 0) > op."заказы" THEN op."заказы"
                ELSE op."заказы" - COALESCE(t."Количество", 0)
            END AS "скорректированные_заказы"
        FROM open_po_base op
        LEFT JOIN transit_for_adjustment t 
            ON op."Характеристика" = t."Характеристика"
            AND op."Заказ на поступление.Номер" = t.po_number
            AND op."Дата доступности" = t."Дата приемки на склад"
    ),
    
    grouped_by_date AS (
        SELECT 
            "Дата доступности",
            "Артикул", 
            "Характеристика",
            SUM("скорректированные_заказы") AS "заказы"
        FROM adjusted_open_po
        WHERE "Дата доступности" >= CURRENT_DATE
        GROUP BY "Дата доступности", "Артикул", "Характеристика"
    ),
    
    with_month_diff AS (
        SELECT 
            "Артикул",
            "Характеристика", 
            "заказы",
            EXTRACT(YEAR FROM "Дата доступности") * 12 + EXTRACT(MONTH FROM "Дата доступности") - 
            (EXTRACT(YEAR FROM CURRENT_DATE) * 12 + EXTRACT(MONTH FROM CURRENT_DATE)) AS month_diff
        FROM grouped_by_date
    ),
    
    grouped_by_month_diff AS (
        SELECT 
            "Артикул",
            "Характеристика", 
            month_diff,
            SUM("заказы") AS "заказы"
        FROM with_month_diff
        WHERE month_diff BETWEEN 0 AND 4  -- Только M, M+1, M+2, M+3, M+4
        GROUP BY "Артикул", "Характеристика", month_diff
    )
    
    SELECT 
        "Характеристика" AS ic,
        "Артикул" AS ean,
        MAX(CASE WHEN month_diff = 0 THEN "заказы" END) AS openpo_m,
        MAX(CASE WHEN month_diff = 1 THEN "заказы" END) AS openpo_m_plus_1,
        MAX(CASE WHEN month_diff = 2 THEN "заказы" END) AS openpo_m_plus_2,
        MAX(CASE WHEN month_diff = 3 THEN "заказы" END) AS openpo_m_plus_3,
        MAX(CASE WHEN month_diff = 4 THEN "заказы" END) AS openpo_m_plus_4
    FROM grouped_by_month_diff
    GROUP BY "Характеристика", "Артикул"
),

combined_data AS (
    SELECT 
        p.ic,
        p.ean,
        p.description,
        ag.bu_with_uv,
        ag.ag,
        ag.aug,
        p.project_ic,
        p.wh_status,
        p.lifecycle_status,
        p.ean_group,
        COALESCE(s.available_stock, 0) AS available_stock,
        COALESCE(s.shipped_stock, 0) AS shipped_stock,
        COALESCE(s.reserved_stock, 0) AS reserved_stock,
        COALESCE(s.free_stock, 0) AS free_stock,
        COALESCE(s.backorder_stock, 0) AS backorder_stock,
        COALESCE(s.supply_needed_stock, 0) AS supply_needed_stock,
        COALESCE(t.transit_1_10_d, 0) AS transit_1_10_d,
        COALESCE(t.transit_10_20_d, 0) AS transit_10_20_d,
        COALESCE(t.transit_20_30_d, 0) AS transit_20_30_d,
        COALESCE(t.transit_30_45_d, 0) AS transit_30_45_d,
        COALESCE(t.transit_45_60_d, 0) AS transit_45_60_d,
        COALESCE(op.openpo_m, 0) AS openpo_m,
        COALESCE(op.openpo_m_plus_1, 0) AS openpo_m_plus_1,
        COALESCE(op.openpo_m_plus_2, 0) AS openpo_m_plus_2,
        COALESCE(op.openpo_m_plus_3, 0) AS openpo_m_plus_3,
        COALESCE(op.openpo_m_plus_4, 0) AS openpo_m_plus_4
    FROM products_1c p
    LEFT JOIN ag_structure ag ON p.aug_key = ag.aug_key
    LEFT JOIN stock_1c s ON p.ic = s.ic
    LEFT JOIN transit_data t ON p.ic = t.ic
    LEFT JOIN openpo_data op ON p.ic = op.ic
)

SELECT 
    ic,
    ean,
    description,
    bu_with_uv,
    ag,
    aug,
    project_ic,
    wh_status,
    lifecycle_status,
    ean_group,
    available_stock,
    shipped_stock,
    reserved_stock,
    free_stock,
    backorder_stock,
    supply_needed_stock,
    transit_1_10_d,
    transit_10_20_d,
    transit_20_30_d,
    transit_30_45_d,
    transit_45_60_d,
    openpo_m,
    openpo_m_plus_1,
    openpo_m_plus_2,
    openpo_m_plus_3,
    openpo_m_plus_4
FROM combined_data;
"""
