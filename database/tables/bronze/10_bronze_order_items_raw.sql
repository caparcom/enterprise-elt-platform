IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.order_items_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.order_items_raw (
        id BIGINT NULL,
        order_id int NULL,
        user_id int NULL,
        product_id int NULL,
        inventory_item_id int NULL,
        status VARCHAR(50) NULL,
        created_at VARCHAR(50) NULL,
        shipped_at VARCHAR(50) NULL,
        delivered_at VARCHAR(50) NULL,
        returned_at VARCHAR(50) NULL,
        sale_price DECIMAL(10,2) NULL,
        -- metadata
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_boi_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_boi_run_id ON bronze.order_items_raw(_run_id);
END
GO