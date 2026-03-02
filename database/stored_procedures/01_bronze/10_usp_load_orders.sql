CREATE OR ALTER PROCEDURE bronze.usp_load_orders 
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	-- ensure we don't duplicate data
	IF EXISTS (SELECT 1 
			FROM bronze.orders_raw
			WHERE _run_id = @run_id
			)
	BEGIN
		THROW 51000, 'Run_id already loaded into bronze.orders_raw.
						Use a new Run_id, or delete that Run_id for dev.', 1;
	END;

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file) 
	values (@run_id, 'thelook', 'bronze.orders_raw', @source_file); 
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#orders_stage') IS NOT NULL DROP TABLE #orders_stage; 
	
		-- stage
		CREATE TABLE #orders_stage( 
			order_id int NULL,
			user_id int NULL,
			status VARCHAR(30) NULL,
			gender VARCHAR(30) NULL,
			created_at VARCHAR(50) NULL,
			returned_at VARCHAR(50) NULL,
			shipped_at VARCHAR(50) NULL,
			delivered_at VARCHAR(50) NULL,
			num_of_item int NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #orders_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.orders_raw (order_id, user_id, status,  
		gender, created_at, returned_at, shipped_at, delivered_at, num_of_item, _run_id, _source_file)
		SELECT order_id, user_id, status,  
		gender, created_at, returned_at, shipped_at, delivered_at, num_of_item, @run_id, @source_file
		from #orders_stage; 
	
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