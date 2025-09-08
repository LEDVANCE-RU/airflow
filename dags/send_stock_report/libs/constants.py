STOCK_REPORT_SQL = """
WITH stocks AS (
    SELECT
      ean::text AS ean,
      SUM(COALESCE(avail, 0)) AS avail
    FROM
      stocks.stock
    GROUP BY 1
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
    p.ean::numeric AS "EAN",
    p.description AS "Наименование",
    ROUND(COALESCE(s.avail, 0))::numeric AS "Доступно"
  FROM
    pl p
    LEFT JOIN stocks s ON (p.ean = s.ean)
  ORDER BY p.id;
"""
