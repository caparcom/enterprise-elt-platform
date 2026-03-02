CREATE OR ALTER PROCEDURE bronze.usp_load_thelook_run
    @run_id VARCHAR(50),
    @base_path VARCHAR(260)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @file VARCHAR(260);

    BEGIN TRY

        SET @file = @base_path + '\' + @run_id + '\' + 'distribution_centers.csv';
        EXEC bronze.usp_load_distribution_centers
            @run_id = @run_id,
            @source_file = @file;

        SET @file = @base_path + '\' + @run_id + '\' + 'products.csv';
        EXEC bronze.usp_load_products
            @run_id = @run_id,
            @source_file = @file;
        
        SET @file = @base_path + '\' + @run_id + '\' + 'users.csv';
        EXEC bronze.usp_load_users
            @run_id = @run_id,
            @source_file = @file;

        SET @file = @base_path + '\' + @run_id + '\' + 'orders.csv';
        EXEC bronze.usp_load_orders
            @run_id = @run_id,
            @source_file = @file;

        SET @file = @base_path + '\' + @run_id + '\' + 'inventory_items.csv';
        EXEC bronze.usp_load_inventory_items
            @run_id = @run_id,
            @source_file = @file;

        SET @file = @base_path + '\' + @run_id + '\' + 'order_items.csv';
        EXEC bronze.usp_load_order_items
            @run_id = @run_id,
            @source_file = @file;

        SET @file = @base_path + '\' + @run_id + '\' + 'events.csv';
        EXEC bronze.usp_load_events
            @run_id = @run_id,
            @source_file = @file;

    END TRY
    BEGIN CATCH
        THROW; -- we'll already log the actual error in each individual proc
    END CATCH
END;
GO