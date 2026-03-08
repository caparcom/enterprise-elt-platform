-- Seed dim_date for a fixed range
DECLARE @start_date DATE = '2018-01-01';
DECLARE @end_date   DATE = '2030-12-31';

;WITH n AS (
    SELECT 0 AS i
    UNION ALL
    SELECT i + 1
    FROM n
    WHERE DATEADD(DAY, i + 1, @start_date) <= @end_date
),
d AS (
    SELECT DATEADD(DAY, i, @start_date) AS [date]
    FROM n
)
INSERT INTO silver.dim_date (
    date_key, [date], [year], [quarter], [month], month_name,
    day_of_month, day_of_week, day_name, week_of_year, is_weekend
)
SELECT
    CONVERT(INT, FORMAT([date], 'yyyyMMdd')) AS date_key,
    [date],
    DATEPART(YEAR, [date]) AS [year],
    DATEPART(QUARTER, [date]) AS [quarter],
    DATEPART(MONTH, [date]) AS [month],
    DATENAME(MONTH, [date]) AS month_name,
    DATEPART(DAY, [date]) AS day_of_month,
    ((DATEPART(WEEKDAY, [date]) + @@DATEFIRST - 2) % 7) + 1 AS day_of_week,
    DATENAME(WEEKDAY, [date]) AS day_name,
    DATEPART(ISO_WEEK, [date]) AS week_of_year,
    CASE WHEN DATENAME(WEEKDAY, [date]) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS is_weekend
FROM d
OPTION (MAXRECURSION 0);