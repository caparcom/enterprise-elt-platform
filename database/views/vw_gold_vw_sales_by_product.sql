IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

CREATE OR ALTER VIEW gold.vw_sales_by_product AS
SELECT
    dp.product_key,
    dp.product_id,
    dp.product_name,
    dp.product_brand,
    dp.product_category,
    dp.product_department,
    ddc.distribution_center_name,
    COUNT(*) AS total_items_sold,
    CAST(ROUND(SUM(foi.sale_price),2) AS DECIMAL(10,2)) AS total_revenue,
    CAST(ROUND(AVG(foi.sale_price),2) AS DECIMAL(10,2)) AS avg_sale_price
FROM silver.fact_order_items foi
JOIN silver.dim_product dp
    ON foi.product_key = dp.product_key
LEFT JOIN silver.dim_distribution_centers ddc
    ON dp.distribution_center_key = ddc.distribution_center_key
GROUP BY
    dp.product_key,
    dp.product_id,
    dp.product_name,
    dp.product_brand,
    dp.product_category,
    dp.product_department,
    ddc.distribution_center_name;
GO