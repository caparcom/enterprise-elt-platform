IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF OBJECT_ID('silver.dim_distribution_centers', 'U') IS NULL
BEGIN
    CREATE TABLE silver.dim_distribution_centers (
        distribution_center_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        distribution_center_id BIGINT NOT NULL,
        distribution_center_name VARCHAR(255) NULL,
        latitude DECIMAL(9,6) NULL,
        longitude DECIMAL(9,6) NULL,
        created_run_id VARCHAR(50) NOT NULL,
        created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_distribution_center_created_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_dim_distribution_center_id UNIQUE (distribution_center_id)
    );

END
GO