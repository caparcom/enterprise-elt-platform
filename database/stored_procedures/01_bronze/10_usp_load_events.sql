IF OBJECT_ID('bronze.usp_load_events', 'P') IS NOT NULL
	DROP PROCEDURE bronze.usp_load_events;
GO

CREATE PROCEDURE bronze.usp_load_events
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file)
	values (@run_id, 'thelook', 'bronze.events_raw', @source_file);
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#events_stage') IS NOT NULL DROP TABLE #events_stage;
	
		-- stage
		CREATE TABLE #events_stage(
			id BIGINT NULL,
			user_id int NULL,
			sequence_number int NULL,
			session_id VARCHAR(255) NULL,
			created_at VARCHAR(50) NULL,
			ip_address VARCHAR(45) NULL,
			city VARCHAR(100) NULL,
			state VARCHAR(100) NULL,
			postal_code VARCHAR(30) NULL,
			browser VARCHAR(50) NULL,
			traffic_source VARCHAR(30) NULL,
			uri VARCHAR(100) NULL,
			event_type VARCHAR(30) NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #events_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.events_raw (id, user_id, sequence_number, 
		session_id, created_at, ip_address, city, state, postal_code, browser, traffic_source, 
		uri, event_type, _run_id, _source_file)
		SELECT id, user_id, sequence_number, 
		session_id, created_at, ip_address, city, state, postal_code, browser, traffic_source, 
		uri, event_type, @run_id, @source_file
		from #events_stage;
	
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