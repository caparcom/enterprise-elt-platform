IF NOT EXISTS (SELECT 1 FROM sys.schemas where name = 'silver')
	EXEC('CREATE SCHEMA silver');
GO

IF OBJECT_ID('silver.dim_date', 'U') IS NULL
BEGIN
	CREATE TABLE silver.dim_date (
		date_key INT NOT NULL PRIMARY KEY,
		[date] DATE NOT NULL,
		[year] SMALLINT NOT NULL,
		[quarter] TINYINT NOT NULL,
		[month] TINYINT NOT NULL,
		month_name VARCHAR(9) NOT NULL,
		day_of_month TINYINT NOT NULL,
		day_of_week TINYINT NOT NULL,
		day_name VARCHAR(9) NOT NULL,
		week_of_year TINYINT NOT NULL,
		is_weekend BIT NOT NULL
	);

	CREATE UNIQUE INDEX UX_dim_date_date ON silver.dim_date([date]);
END
GO