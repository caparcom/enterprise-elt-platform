IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF OBJECT_ID('silver.dim_products', 'U') IS NULL
BEGIN
    CREATE TABLE silver.dim_products (
        product_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        product_id  BIGINT NOT NULL, -- natural from raw data
        product_name  VARCHAR(255) NULL,
        product_brand VARCHAR(255) NULL,
        product_category VARCHAR(255) NULL,
        product_department VARCHAR(255) NULL,
        product_sku VARCHAR(100) NULL,
        product_cost DECIMAL(10,2) NULL,
        product_retail_price DECIMAL(10,2) NULL,
        distribution_center_key INT NULL,
        source_distribution_center_id BIGINT NULL,
        created_run_id VARCHAR(50) NOT NULL,
        created_at DATETIME2(3) NOT NULL CONSTRAINT DF_dim_product_created_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_dim_product_product_id UNIQUE (product_id)
    );
END
GO