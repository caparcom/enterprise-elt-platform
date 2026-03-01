IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name = 'bronze')
	EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.load_audit', 'u') IS NULL
BEGIN
	CREATE TABLE bronze.load_audit (
		audit_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
		run_id VARCHAR(50) NOT NULL,
		source_name VARCHAR(50) NOT NULL,
		target_table SYSNAME NOT NULL,
		source_file VARCHAR(300) NOT NULL,
		load_started_at DATETIME2(3) NOT NULL CONSTRAINT DF_load_audit_started DEFAULT SYSUTCDATETIME(),
		load_ended_at DATETIME2(3) NULL,
		loaded_rows BIGINT NULL,
		status VARCHAR(10) NOT NULL CONSTRAINT DF_load_audit_status DEFAULT 'STARTED',
		error_message NVARCHAR(4000) NULL
	);

	CREATE INDEX IX_load_audit_run ON bronze.load_audit(run_id, source_name);
	CREATE INDEX IX_load_audit_status ON bronze.load_audit(status);
END
GO