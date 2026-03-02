CREATE OR ALTER PROCEDURE bronze.usp_load_distribution_centers
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	-- ensure we don't duplicate data
	IF EXISTS (SELECT 1 
				FROM bronze.distribution_centers_raw
				WHERE _run_id = @run_id
				)
	BEGIN
		THROW 51000, 'Run_id already loaded into bronze.distribution_centers_raw.
						Use a new Run_id, or delete that run_id for dev.', 1;
	END;					

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file)
	values (@run_id, 'thelook', 'bronze.distribution_centers_raw', @source_file);
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#dist_centers_stage') IS NOT NULL DROP TABLE #dist_centers_stage;
	
		-- stage
		CREATE TABLE #dist_centers_stage(
			id BIGINT NULL,
			name varchar(255) NULL,
			latitude DECIMAL(9,6) NULL,
			longitude DECIMAL(9,6) NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #dist_centers_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.distribution_centers_raw (id, name, latitude, longitude, _run_id, _source_file)
		SELECT id, name, latitude, longitude, @run_id, @source_file
		from #dist_centers_stage;
	
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
