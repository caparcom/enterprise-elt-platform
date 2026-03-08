-- dev only: full reload
TRUNCATE TABLE silver.fact_order_items;

INSERT INTO silver.fact_order_items (
    order_item_id,
    order_id,
    inventory_item_id,
    user_key,
    product_key,
    created_date_key,
    shipped_date_key,
    delivered_date_key,
    returned_date_key,
    status,
    sale_price,
    item_count,
    created_run_id
)
SELECT
    oi.id AS order_item_id,
    oi.order_id,
    oi.inventory_item_id,
    du.user_key,
    dp.product_key,
    d_created.date_key   AS created_date_key,
    d_shipped.date_key   AS shipped_date_key,
    d_delivered.date_key AS delivered_date_key,
    d_returned.date_key  AS returned_date_key,
    LTRIM(RTRIM(oi.status)) AS status,
    oi.sale_price,
    1 AS item_count,
    oi._run_id AS created_run_id
FROM bronze.order_items_raw oi
JOIN silver.dim_users du
    ON oi.user_id = du.user_id
JOIN silver.dim_product dp
    ON oi.product_id = dp.product_id
LEFT JOIN silver.dim_date d_created
    ON CAST(REPLACE(oi.created_at, ' UTC', '') AS DATE) = d_created.[date]
LEFT JOIN silver.dim_date d_shipped
    ON CAST(REPLACE(oi.shipped_at, ' UTC', '') AS DATE) = d_shipped.[date]
LEFT JOIN silver.dim_date d_delivered
    ON CAST(REPLACE(oi.delivered_at, ' UTC', '') AS DATE) = d_delivered.[date]
LEFT JOIN silver.dim_date d_returned
    ON CAST(REPLACE(oi.returned_at, ' UTC', '') AS DATE) = d_returned.[date];