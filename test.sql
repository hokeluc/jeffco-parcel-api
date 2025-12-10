#1 This query calculates the property turnover rate in various neighborhoods over the last 10 years.
WITH sales AS (
    SELECT PIN, NHDNAM, TO_DATE(SLSDT, 'MMDDYYYY') AS sale_date FROM jeffco_staging
    UNION ALL
    SELECT PIN, NHDNAM, TO_DATE(SLSDT2, 'MMDDYYYY') FROM jeffco_staging
    UNION ALL
    SELECT PIN, NHDNAM, TO_DATE(SLSDT3, 'MMDDYYYY') FROM jeffco_staging
    UNION ALL
    SELECT PIN, NHDNAM, TO_DATE(SLSDT4, 'MMDDYYYY') FROM jeffco_staging
),
recent_sales AS (
    SELECT DISTINCT PIN, NHDNAM
    FROM sales
    WHERE sale_date >= CURRENT_DATE - INTERVAL '10 years'
),
neighbors AS (
    SELECT NHDNAM, COUNT(DISTINCT PIN) AS total_properties
    FROM jeffco_staging
    GROUP BY NHDNAM
)
SELECT
    n.NHDNAM AS neighborhood,
    COUNT(rs.PIN) AS properties_sold_last_10yrs,
    n.total_properties,
    ROUND(
        COUNT(rs.PIN)::numeric / n.total_properties * 100, 2) 
        AS turnover_percent_last_10yrs
FROM neighbors n
LEFT JOIN recent_sales rs USING (NHDNAM)
GROUP BY n.NHDNAM, n.total_properties
ORDER BY turnover_percent_last_10yrs DESC;

#2 This query calculates the property turnover rate in various subdivisions over the last 10 years.
WITH sales AS (
    SELECT PIN, SUBNAM, TO_DATE(SLSDT, 'MMDDYYYY') AS sale_date
    FROM jeffco_staging
    WHERE TAXCLS LIKE '1%'
    UNION ALL
    SELECT PIN, SUBNAM, TO_DATE(SLSDT2, 'MMDDYYYY')
    FROM jeffco_staging
    WHERE TAXCLS LIKE '1%'
    UNION ALL
    SELECT PIN, SUBNAM, TO_DATE(SLSDT3, 'MMDDYYYY')
    FROM jeffco_staging
    WHERE TAXCLS LIKE '1%'
    UNION ALL
    SELECT PIN, SUBNAM, TO_DATE(SLSDT4, 'MMDDYYYY')
    FROM jeffco_staging
    WHERE TAXCLS LIKE '1%'
),
recent_sales AS (
    SELECT DISTINCT PIN, SUBNAM
    FROM sales
    WHERE sale_date >= CURRENT_DATE - INTERVAL '5 years'
),
subdivisions AS (
    SELECT SUBNAM, COUNT(DISTINCT PIN) AS total_properties
    FROM jeffco_staging
    WHERE TAXCLS LIKE '1%'
    GROUP BY SUBNAM
)
SELECT
    s.SUBNAM AS subdivision,
    COUNT(rs.PIN) AS properties_sold_last_5yrs,
    s.total_properties,
    ROUND(COUNT(rs.PIN)::numeric / s.total_properties * 100, 2) AS turnover_percent_last_5yrs
FROM subdivisions s
LEFT JOIN recent_sales rs USING (SUBNAM)
GROUP BY s.SUBNAM, s.total_properties
HAVING s.total_properties >= 20
ORDER BY turnover_percent_last_5yrs DESC;

#This query analyzes the change in total property values by neighborhood for residential properties.
SELECT
    NHDNAM AS neighborhood,
    SUM(TOTACTVAL::NUMERIC) AS total_current_value,
    SUM(PYRTOTVAL::NUMERIC) AS total_prior_value,
    SUM(TOTACTVAL::NUMERIC) - SUM(PYRTOTVAL::NUMERIC) AS value_change,
    ROUND(
        (SUM(TOTACTVAL::NUMERIC) - SUM(PYRTOTVAL::NUMERIC)) / NULLIF(SUM(PYRTOTVAL::NUMERIC), 0)::NUMERIC * 100,
        2
    ) AS value_change_pct
FROM jeffco_staging
WHERE TAXCLS LIKE '1%'
  AND TOTACTVAL IS NOT NULL
  AND PYRTOTVAL IS NOT NULL
GROUP BY NHDNAM
HAVING SUM(PYRTOTVAL::NUMERIC) > 0
ORDER BY value_change_pct DESC;



SELECT DISTINCT TAXCLS
FROM jeffco_staging
ORDER BY TAXCLS;


SELECT DISTINCT DEDTYP
FROM jeffco_staging
ORDER BY DEDTYP;




SELECT DISTINCT NHDNAM FROM jeffco_staging ORDER BY NHDNAM;

SELECT DISTINCT nhdnam, SUM(slsamt::NUMERIC) FROM jeffco_staging GROUP BY NHDNAM HAVING COUNT(*) > 1000 ORDER BY SUM DESC;

SELECT
  pin,
  objectid,
  mailstrnbr, mailstrdir, mailstrnam, mailstrtyp, mailstrsfx, mailstrunt,
  mailctynam, mailstenam, mailzip5, mailzip4
FROM kkubaska.jeffco_staging
WHERE pin = '30-152-01-140'
ORDER BY objectid;
