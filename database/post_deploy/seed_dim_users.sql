-- dev truncation vs merge/upsert
TRUNCATE TABLE silver.dim_users

INSERT INTO silver.dim_users (
	user_id,
    user_first_name,
    user_last_name,
    user_email,
    user_age,
    user_gender,
    user_state,
    user_street_address,
    user_postal_code,
    user_city,
    user_country,
    user_latitude,
    user_longitude,
    user_traffic_source,
    source_user_created_at,
    created_run_id
)
SELECT 
    u.id AS user_id,
    LTRIM(RTRIM(u.first_name)) AS user_first_name,
    LTRIM(RTRIM(u.last_name)) AS user_last_name,
    LTRIM(RTRIM(u.email)) AS user_email,
    u.age AS user_age,
    LTRIM(RTRIM(u.gender)) AS user_gender,
    LTRIM(RTRIM(u.state)) AS user_state,
    LTRIM(RTRIM(u.street_address)) AS user_street_address,
    LTRIM(RTRIM(u.postal_code)) AS user_postal_code,
    LTRIM(RTRIM(u.city)) AS user_city,
    LTRIM(RTRIM(u.country)) AS user_country,
    u.latitude AS user_latitude,
    u.longitude AS user_longitude,
    LTRIM(RTRIM(u.traffic_source)) AS user_traffic_source,
    TRY_CAST(u.created_at AS DATETIME2(3)) AS source_user_created_at,
    u._run_id AS created_run_id
FROM bronze.users_raw u