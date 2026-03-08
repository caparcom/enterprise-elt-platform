IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF OBJECT_ID('silver.dim_users', 'U') IS NULL
BEGIN
    CREATE TABLE silver.dim_users (
        user_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        user_first_name VARCHAR(50) NULL,
        user_last_name VARCHAR(50) NULL,
        user_email VARCHAR(50) NULL,
        user_age INT NULL,
        user_gender VARCHAR(5) NULL,
        user_state VARCHAR(100) NULL,
        user_street_address VARCHAR(75) NULL,
        user_postal_code VARCHAR(30) NULL,
        user_city VARCHAR(100) NULL,
        user_country VARCHAR(50) NULL,
        user_latitude DECIMAL(9,6) NULL,
        user_longitude DECIMAL(9,6) NULL,
        user_traffic_source VARCHAR(150) NULL,
        source_user_created_at DATETIME2(3) NULL,
        created_run_id VARCHAR(50) NOT NULL,
        created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_users_created_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_dim_users_user_id UNIQUE (user_id)

    );
END
GO