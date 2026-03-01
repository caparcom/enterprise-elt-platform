IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.file_registry', 'U') IS NULL
BEGIN
    CREATE TABLE bronze.file_registry (
        source_name      VARCHAR(50) NOT NULL,
        file_name        VARCHAR(128) NOT NULL,
        target_table     SYSNAME NOT NULL,
        has_header       BIT NOT NULL CONSTRAINT DF_file_registry_has_header DEFAULT 1,
        first_row        INT NOT NULL CONSTRAINT DF_file_registry_first_row DEFAULT 2,
        field_terminator VARCHAR(10) NOT NULL CONSTRAINT DF_file_registry_field_term DEFAULT ',',
        row_terminator   VARCHAR(10) NOT NULL CONSTRAINT DF_file_registry_row_term DEFAULT '0x0a',
        CONSTRAINT PK_file_registry PRIMARY KEY (source_name, file_name)
    );
END
GO