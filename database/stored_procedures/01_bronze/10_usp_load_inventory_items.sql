IF OBJECT_ID('bronze.usp_load_inventory_items', 'P') IS NOT NULL
	DROP PROCEDURE bronze.usp_load_inventory_items;
GO

CREATE PROCEDURE bronze.usp_load_inventory_items
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	-- ensure we don't duplicate data
	IF EXISTS (SELECT 1 
			FROM bronze.inventory_items_raw
			WHERE _run_id = @run_id
			)
	BEGIN
		THROW 51000, 'Run_id already loaded into bronze.inventory_items_raw.
						Use a new Run_id, or delete that Run_id for dev.', 1;
	END;

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file)
	values (@run_id, 'thelook', 'bronze.inventory_items_raw', @source_file);
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#inventory_items_stage') IS NOT NULL DROP TABLE #inventory_items_stage;
	
		-- stage
		CREATE TABLE #inventory_items_stage(
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
			product_distribution_center_id INT NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #inventory_items_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.inventory_items_raw (id, product_id, created_at, 
		sold_at, cost, product_category, product_name, product_brand, product_retail_price, 
		product_department, product_sku, product_distribution_center_id, _run_id, _source_file)
		SELECT id, product_id, created_at, 
		sold_at, cost, product_category, product_name, product_brand, product_retail_price, 
		product_department, product_sku, product_distribution_center_id, @run_id, @source_file
		from #inventory_items_stage;
	
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