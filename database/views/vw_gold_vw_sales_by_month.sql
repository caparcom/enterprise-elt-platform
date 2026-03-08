IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

CREATE OR ALTER VIEW gold.vw_sales_by_month AS
SELECT
    dd.[year],
    dd.[quarter],
    dd.[month],
    dd.month_name,
    COUNT(*) AS total_items_sold,
    CAST(ROUND(SUM(foi.sale_price), 2) AS DECIMAL(10, 2)) AS total_revenue,
    CAST(ROUND(AVG(foi.sale_price), 2) AS DECIMAL(10, 2)) AS avg_sale_price
FROM silver.fact_order_items foi
JOIN silver.dim_date dd
    ON foi.created_date_key = dd.date_key
GROUP BY
    dd.[year],
    dd.[quarter],
    dd.[month],
    dd.month_name;
GO