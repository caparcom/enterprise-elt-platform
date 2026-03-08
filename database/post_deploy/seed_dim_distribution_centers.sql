-- truncating for dev, will replace with merge/upsert
TRUNCATE TABLE silver.dim_distribution_centers

INSERT INTO silver.dim_distribution_centers (
	distribution_center_id,
	distribution_center_name,
	latitude,
	longitude,
	created_run_id
)
SELECT
	dc.id as distribution_center_id,
	LTRIM(RTRIM(dc.name)) as distribution_center_name,
	dc.latitude as latitude,
	dc.longitude as longitude,
	dc._run_id as created_run_id
FROM bronze.distribution_centers_raw dc