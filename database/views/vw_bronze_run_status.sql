IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
	EXEC('CREATE SCHEMA bronze');
GO

CREATE OR ALTER VIEW bronze.vw_bronze_run_status AS
WITH last_attempt AS (
	SELECT run_id,
		source_name,
		target_table,
		source_file,
		status,
		loaded_rows,
		load_started_at,
		load_ended_at,
		error_message,
		ROW_NUMBER() OVER (PARTITION BY run_id, source_name, target_table
			ORDER BY audit_id DESC) AS rn
	FROM bronze.load_audit
)
SELECT run_id,
	source_name, 
	target_table,
	source_file,
	status,
	loaded_rows,
	load_started_at,
	load_ended_at,
	error_message
FROM last_attempt
WHERE rn = 1;
GO