IF OBJECT_ID('bronze.usp_load_users', 'P') IS NOT NULL 
	DROP PROCEDURE bronze.usp_load_users; 
GO

CREATE PROCEDURE bronze.usp_load_users 
	@run_id VARCHAR(50),
	@source_file VARCHAR(260)
AS
BEGIN
	SET NOCOUNT ON;

	-- ensure we don't duplicate data
	IF EXISTS (SELECT 1 
			FROM bronze.users_raw
			WHERE _run_id = @run_id
			)
	BEGIN
		THROW 51000, 'Run_id already loaded into bronze.users_raw.
						Use a new Run_id, or delete that Run_id for dev.', 1;
	END;

	DECLARE @audit_id BIGINT;
	
	-- start log
	INSERT INTO bronze.load_audit (run_id, source_name, target_table, source_file) 
	values (@run_id, 'thelook', 'bronze.users_raw', @source_file); 
	
	SET @audit_id = SCOPE_IDENTITY();
	
	BEGIN TRY
	
		IF OBJECT_ID('tempdb..#users_stage') IS NOT NULL DROP TABLE #users_stage; 
	
		-- stage
		CREATE TABLE #users_stage( 
			id BIGINT NULL,
			first_name VARCHAR(50) NULL,
			last_name VARCHAR(50) NULL,
			email VARCHAR(50) NULL,
			age int NULL,
			gender VARCHAR(10) NULL,
			state VARCHAR(100) NULL,
			street_address VARCHAR(75) NULL,
			postal_code VARCHAR(30) NULL,
			city VARCHAR(100) NULL,
			country VARCHAR(50) NULL,
			latitude DECIMAL(9,6) NULL,
			longitude DECIMAL(9,6) NULL,
			traffic_source VARCHAR(150) NULL,
			created_at VARCHAR(50) NULL
		)
	
		DECLARE @sql NVARCHAR(MAX) = N'
		
		BULK INSERT #users_stage
		FROM ''' + REPLACE(@source_file, ',', '""') + N'''
			WITH  (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				ROWTERMINATOR = ''0x0a'',
				TABLOCK
			);';
		
		EXEC sp_executesql @sql;
	
		-- load raw 
		INSERT INTO bronze.users_raw (id, first_name, last_name,  
		email, age, gender, state, street_address, postal_code, city, country, latitude, 
		longitude, traffic_source, created_at, _run_id, _source_file)
		SELECT id, first_name, last_name,  
		email, age, gender, state, street_address, postal_code, city, country, latitude, 
		longitude, traffic_source, created_at, @run_id, @source_file
		from #users_stage; 
	
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