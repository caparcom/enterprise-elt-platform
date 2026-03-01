IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.inventory_items_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.inventory_items_raw (
        id BIGINT NULL,
        product_id int NULL,
        created_at VARCHAR(50) NULL,
        sold_at VARCHAR(50) NULL,
        cost DECIMAL(38,18) NULL,
        product_category VARCHAR(30) NULL,
        product_name NVARCHAR(MAX) NULL,
        product_brand VARCHAR(100) NULL,
        product_retail_price DECIMAL(38,18),
        product_department VARCHAR(25),
        product_sku VARCHAR(100),
        product_distribution_center_id INT NULL,
        -- metadata
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_bii_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_bii_run_id ON bronze.inventory_items_raw(_run_id);
END
GO