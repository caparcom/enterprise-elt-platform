IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF OBJECT_ID('silver.fact_order_items', 'U') IS NULL
BEGIN
    CREATE TABLE silver.fact_order_items (
        order_item_key      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        order_item_id       BIGINT NOT NULL,
        order_id            BIGINT NOT NULL,
        inventory_item_id   BIGINT NULL,
        user_key            INT NOT NULL,
        product_key         INT NOT NULL,
        created_date_key    INT NULL,
        shipped_date_key    INT NULL,
        delivered_date_key  INT NULL,
        returned_date_key   INT NULL,
        status              VARCHAR(50) NULL,
        sale_price          DECIMAL(10,2) NULL,
        item_count          INT NOT NULL CONSTRAINT DF_fact_order_items_item_count DEFAULT 1,
        created_run_id      VARCHAR(50) NOT NULL,
        created_at          DATETIME2(3) NOT NULL CONSTRAINT DF_fact_order_items_created_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_fact_order_items_order_item_id UNIQUE (order_item_id)
    );
END
GO