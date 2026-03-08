-- for dev only, will be replacing truncation with merge/upsert
TRUNCATE TABLE silver.dim_products;

INSERT INTO silver.dim_products (
	product_id,
	product_name,
    product_brand,
    product_category,
    product_department,
    product_sku,
    product_cost,
    product_retail_price,
    distribution_center_key,
    source_distribution_center_id,
    created_run_id
)
SELECT 
    p.id AS product_id,
    LTRIM(RTRIM(p.name)) AS product_name,
    LTRIM(RTRIM(p.brand)) AS product_brand,
    LTRIM(RTRIM(p.category)) AS product_category,
    LTRIM(RTRIM(p.department)) AS product_department,
    LTRIM(RTRIM(p.sku)) AS product_sku,
    p.cost AS product_cost,
    p.retail_price AS product_retail_price,
    sddc.distribution_center_key as distribution_center_key,
    p.distribution_center_id as source_distribution_center_id,
    p._run_id AS created_run_id
FROM bronze.products_raw p
JOIN silver.dim_distribution_centers sddc on p.distribution_center_id = sddc.distribution_center_id


