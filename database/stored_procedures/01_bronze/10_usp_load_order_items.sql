IF OBJECT_ID('bronze.usp_load_order_items', 'P') IS NOT NULL
	DROP PROCEDURE bronze.usp_load_order_items;
GO

CREATE PROCEDURE bronze.usp_load_order_items
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file)
	values (@run_id, 'thelook', 'bronze.order_items_raw', @source_file);
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#order_items_stage') IS NOT NULL DROP TABLE #order_items_stage;
	
		-- stage
		CREATE TABLE #order_items_stage(
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
			sale_price DECIMAL(10,2) NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #order_items_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.order_items_raw (id, order_id, user_id, 
		product_id, inventory_item_id, status, created_at, shipped_at, delivered_at, 
		returned_at, sale_price, _run_id, _source_file)
		SELECT id, order_id, user_id, 
		product_id, inventory_item_id, status, created_at, shipped_at, delivered_at, 
		returned_at, sale_price, @run_id, @source_file
		from #order_items_stage;
	
		DECLARE @rows BIGINT = @@ROWCOUNT;
	
		-- succeed
		UPDATE bronze.load_audit
		SET status = 'SUCCESS',
			loaded_rows = @rows,
			load_ended_at = SYSUTCDATETIME(),
			error_message = NULL
		WHERE audit_id = @audit_id
	
	END TRY
	-- or fail
	BEGIN CATCH
	
		UPDATE bronze.load_audit
		SET STATUS = 'FAILED',
			load_ended_at = SYSUTCDATETIME(),
			error_message = ERROR_MESSAGE()
		WHERE audit_id = @audit_id;
	
		THROW;
	
	END CATCH;

END;
GO