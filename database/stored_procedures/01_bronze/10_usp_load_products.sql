CREATE OR ALTER PROCEDURE bronze.usp_load_products 
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	-- ensure we don't duplicate data
	IF EXISTS (SELECT 1 
			FROM bronze.products_raw
			WHERE _run_id = @run_id
			)
	BEGIN
		THROW 51000, 'Run_id already loaded into bronze.products_raw.
						Use a new Run_id, or delete that Run_id for dev.', 1;
	END;

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file) 
	values (@run_id, 'thelook', 'bronze.products_raw', @source_file); 
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#products_stage') IS NOT NULL DROP TABLE #products_stage; 
	
		-- stage
		CREATE TABLE #products_stage( 
			id BIGINT NULL,
			cost DECIMAL(38,18) NULL,
			category VARCHAR(30) NULL,
			name NVARCHAR(MAX) NULL,
			brand VARCHAR(50) NULL,
			retail_price DECIMAL(38,18) NULL,
			department VARCHAR(50) NULL,
			sku VARCHAR(50) NULL,
			distribution_center_id int NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #products_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.products_raw (id, cost, category,  
		name, brand, retail_price, department, sku, distribution_center_id, _run_id, _source_file)
		SELECT id, cost, category,  
		name, brand, retail_price, department, sku, distribution_center_id, @run_id, @source_file
		from #products_stage; 
	
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