STOCK_REPORT_SQL = """
WITH stocks AS (
    SELECT
      ean,
      SUM(COALESCE(free_stock, 0)) AS avail
    FROM
      si.stock_for_customer
    GROUP BY ean
  ),
  pl AS (
    SELECT DISTINCT
      ean,
      description,
      9 AS id
    FROM md.price_list
    WHERE description != 'NaN'
    UNION
    SELECT
      ean::varchar,
      description,
      1000000
    FROM
      si.ean_add
  )
  SELECT
    p.ean::numeric,
    p.description,
    ROUND(COALESCE(s.avail, 0))::numeric
  FROM
    pl p
    LEFT JOIN stocks s ON (p.ean = s.ean)
  ORDER BY p.id;
"""
