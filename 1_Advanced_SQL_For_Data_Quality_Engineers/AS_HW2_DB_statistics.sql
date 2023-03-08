/* Procedure db_statistics returns next output list
	[database_name] - contains database name from procedure variable
	[table_name] - contains table name from procedure variable
	[total_row_count] - contains number of rows from table (logic in lines 149-153)
	[column_name] - contains column name from INFORMATION_SCHEMA.COLUMNS table
	[data_type] - contains data type of column from INFORMATION_SCHEMA.COLUMNS table
	[count_of_distinct_values] - contains number of distinct rows for each column (logic in lines 122) 
	[count_of_null_values] - contains number of null values for each column (logic in lines 123)
	[count_of_uppercase_values] - contains number of string values with uppercase letters (uppercase is indicated by 'collate SQL_Latin1_General_CP1_CS_AS' clause) 
		(logic in lines 124-131)
	[count_of_lowercase_values] - contains number of string values with lowercase letters (lowercase is indicated by 'collate Latin1_General_CS_AS' clause)
		(logic in lines 131-139)
	[most_used_value] - contains most used value for each column. If two or more values have the largest number of appearances, procedure takes the first one
		(logic in lines 161-193 and 115)
	[most_used_value_percentage] - contains percentage of the most used value for each column. If two or more values have the largest number of appearances, 
		procedure takes the first one (logic in lines 116 and 161-193)
	[min_value] - contains min value for each column (for each column type)
	[max_value] - contains max value for each column (for each column type)
*/


/*
Columns that were not added: [count_of_empty_zero_values] and [rows_with_non_printable_chrarcters]
*/

USE TRN;


CREATE PROCEDURE db_statistics 
(
	@p_DatabaseName NVARCHAR(MAX),
	@p_SchemaName NVARCHAR(MAX),
	@p_TableName NVARCHAR(MAX)
)
AS 
BEGIN
	
	-- Check Existence of the Database
	DECLARE @ExistenceCheck as INT;
	DECLARE @ErrorMessage as NVARCHAR(MAX);
	SELECT @ExistenceCheck = DB_ID(@p_DatabaseName);
	IF @ExistenceCheck IS NULL
		BEGIN
			SET @ErrorMessage = CONCAT('Error: database name ', @p_DatabaseName, ' does not exist');
			RAISERROR(@ErrorMessage, 0, 0);
			RETURN 0;
		END

	-- Check Existence of the Schema
	SELECT @ExistenceCheck = SCHEMA_ID(@p_SchemaName);
	IF @ExistenceCheck IS NULL
		BEGIN
			SET @ErrorMessage = CONCAT('Error: schema name ', @p_SchemaName, ' does not exist');
			RAISERROR(@ErrorMessage, 0, 0);
			RETURN 0;
		END
	
	-- Check Existence of the Table (check is working only for table that is entered by user)
	IF @p_TableName != '%'
		BEGIN
			DECLARE @ObjectName as NVARCHAR(MAX);
			SET @ObjectName = CONCAT(@p_SchemaName, '.', @p_TableName);
			SELECT @ExistenceCheck = OBJECT_ID(@ObjectName);
			IF @ExistenceCheck IS NULL
				BEGIN
					SET @ErrorMessage = CONCAT('Error: table name ', @ObjectName, ' does not exist');
					RAISERROR(@ErrorMessage, 0, 0);
					RETURN 0;
				END
		END


	-- Select database, schema, table and column names into one table collection
	DECLARE @TableList AS TABLE (DatabaseName NVARCHAR(MAX), SchemaName NVARCHAR(MAX), TableName NVARCHAR(MAX), ColumnName NVARCHAR(MAX));

	-- if user want to select all tables, we add all tables to the table collection
	IF @p_TableName = '%'
		BEGIN
			INSERT INTO @TableList(DatabaseName, SchemaName, TableName, ColumnName)
			SELECT DISTINCT LOWER(@p_DatabaseName), LOWER(@p_SchemaName), LOWER(table_name), LOWER(column_name) 
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE LOWER(@p_DatabaseName) = LOWER(table_catalog) AND LOWER(@p_SchemaName) = LOWER(table_schema);
		END
	-- in other cases we add only table that was entered by user with columns
	ELSE
		BEGIN
			INSERT INTO @TableList(DatabaseName, SchemaName, TableName, ColumnName)
			SELECT LOWER(@p_DatabaseName), LOWER(@p_SchemaName), LOWER(@p_TableName), LOWER(column_name)
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE LOWER(@p_DatabaseName) = LOWER(table_catalog) AND LOWER(@p_SchemaName) = LOWER(table_schema) AND LOWER(@p_TableName) = LOWER(table_name);
		END


	-- Main part
	DECLARE @SQLCode AS NVARCHAR(MAX);

	WITH table_list
	AS
	(
		SELECT DISTINCT DatabaseName, SchemaName, TableName, ColumnName, LEAD(ColumnName) OVER (ORDER BY TableName, ColumnName) union_null
		FROM @TableList
	),
	SQLQuery
	AS
	(
		SELECT 
			CASE 
				WHEN union_null IS NOT NULL 
				THEN
					'
					SELECT mandatory_subquery.database_name, mandatory_subquery.schema_name, mandatory_subquery.table_name, 
							mandatory_subquery.all_count as total_row_count, mandatory_subquery.column_name, mandatory_subquery.data_type,
							mandatory_subquery.distinct_count as count_of_distinct_values, mandatory_subquery.null_count as count_of_null_values,
							mandatory_subquery.upper_count as count_of_uppercase_values, mandatory_subquery.lower_count as count_of_lowercase_values,
							CAST(optional_subquery.source_column_name AS NVARCHAR(MAX)) as most_used_value,
							CONCAT((CAST(optional_subquery.max_count AS FLOAT) / CAST(mandatory_subquery.all_count AS FLOAT) * 100), ''%'') as most_used_value_percentage,
							mandatory_subquery.min_value, mandatory_subquery.max_value
					FROM
					(
						SELECT ''' + table_list.DatabaseName + ''' as database_name, ''' + table_list.SchemaName + ''' as schema_name, ''' + table_list.TableName + '''
									as table_name, COUNT(table_for_count.count_all) as all_count, ''' + table_list.ColumnName + ''' as column_name,
									meta_data.data_type, COUNT(DISTINCT table_data.source_column_name) as distinct_count, 
									COUNT(CASE WHEN table_data.source_column_name IS NULL THEN 1 END) as null_count,
									COUNT(
											CASE 
												WHEN meta_data.data_type IN (''char'', ''varchar'') 
												THEN CASE 
														WHEN table_data.source_column_name = UPPER(table_data.source_column_name) collate SQL_Latin1_General_CP1_CS_AS
														THEN 1 
														END
											END) as upper_count,
									COUNT(
											CASE 
												WHEN meta_data.data_type IN (''char'', ''varchar'') 
												THEN CASE 
														WHEN table_data.source_column_name = lower(table_data.source_column_name) collate Latin1_General_CS_AS
														THEN 1 
														END
											END) as lower_count,
									CAST(MAX(table_data.source_column_name) AS NVARCHAR(MAX)) as max_value,
									CAST(MIN(table_data.source_column_name) AS NVARCHAR(MAX)) as min_value
						FROM 
							(
								SELECT column_name, data_type
								FROM INFORMATION_SCHEMA.COLUMNS
								WHERE LOWER(TABLE_SCHEMA) = LOWER('''+ table_list.SchemaName +''') AND LOWER(TABLE_NAME) = LOWER('''+ table_list.TableName +''') 
										AND LOWER(column_name) = LOWER('''+ table_list.ColumnName +''')
							) as meta_data
							JOIN
							(
								SELECT COUNT(*) count_all, ''' + table_list.TableName  +''' as table_name
								FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
							) table_for_count ON (LOWER('''+  table_list.TableName + ''') = LOWER(table_for_count.table_name))
							JOIN
							(
								SELECT '+  table_list.ColumnName + ' as source_column_name
								FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
							) as table_data ON (LOWER('''+  table_list.ColumnName + ''') = LOWER(meta_data.column_name))
							GROUP BY meta_data.data_type
						) as mandatory_subquery
						JOIN
						(
							SELECT TOP(1) ''' + table_list.DatabaseName + ''' as database_name, ''' + table_list.SchemaName + ''' as schema_name,
							''' + table_list.TableName + '''as table_name, ''' + table_list.ColumnName + ''' as column_name, 
							count_subquery.source_column_name,	max_subquery.max_count
							FROM
							(
								SELECT first_subquery.source_column_name, COUNT(first_subquery.source_column_name) as column_count
								FROM 
								(
									SELECT '+  table_list.ColumnName + ' as source_column_name
									FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
								) as first_subquery
								GROUP BY first_subquery.source_column_name
							) as count_subquery,
							(	
								SELECT MAX(second_subquery.column_count) as max_count
								FROM
								(
									SELECT first_subquery.source_column_name, COUNT(first_subquery.source_column_name) as column_count
									FROM 
									(
										SELECT '+  table_list.ColumnName + ' as source_column_name
										FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
									) as first_subquery
									GROUP BY first_subquery.source_column_name
								) as second_subquery
							) as max_subquery
							WHERE count_subquery.column_count = max_subquery.max_count
						) as optional_subquery ON (mandatory_subquery.database_name = optional_subquery.database_name AND 
							mandatory_subquery.schema_name = optional_subquery.schema_name AND mandatory_subquery.table_name = optional_subquery.table_name
							AND mandatory_subquery.column_name = optional_subquery.column_name)
						UNION ALL'
				ELSE
					'
					SELECT mandatory_subquery.database_name, mandatory_subquery.schema_name, mandatory_subquery.table_name, 
							mandatory_subquery.all_count as total_row_count, mandatory_subquery.column_name, mandatory_subquery.data_type,
							mandatory_subquery.distinct_count as count_of_distinct_values, mandatory_subquery.null_count as count_of_null_values,
							mandatory_subquery.upper_count as count_of_uppercase_values, mandatory_subquery.lower_count as count_of_lowercase_values,
							CAST(optional_subquery.source_column_name AS NVARCHAR(MAX)) as most_used_value,
							CONCAT((CAST(optional_subquery.max_count AS FLOAT) / CAST(mandatory_subquery.all_count AS FLOAT) * 100), ''%'') as most_used_value_percentage,
							mandatory_subquery.min_value, mandatory_subquery.max_value
					FROM
					(
						SELECT ''' + table_list.DatabaseName + ''' as database_name, ''' + table_list.SchemaName + ''' as schema_name, ''' + table_list.TableName + '''
									as table_name, COUNT(table_for_count.count_all) as all_count, ''' + table_list.ColumnName + ''' as column_name,
									meta_data.data_type, COUNT(DISTINCT table_data.source_column_name) as distinct_count, 
									COUNT(CASE WHEN table_data.source_column_name IS NULL THEN 1 END) as null_count,
									COUNT(
											CASE 
												WHEN meta_data.data_type IN (''char'', ''varchar'') 
												THEN CASE 
														WHEN table_data.source_column_name = UPPER(table_data.source_column_name) collate SQL_Latin1_General_CP1_CS_AS
														THEN 1 
														END
											END) as upper_count,
									COUNT(
											CASE 
												WHEN meta_data.data_type IN (''char'', ''varchar'') 
												THEN CASE 
														WHEN table_data.source_column_name = lower(table_data.source_column_name) collate Latin1_General_CS_AS
														THEN 1 
														END
											END) as lower_count,
									CAST(MAX(table_data.source_column_name) AS NVARCHAR(MAX)) as max_value,
									CAST(MIN(table_data.source_column_name) AS NVARCHAR(MAX)) as min_value
						FROM 
							(
								SELECT column_name, data_type
								FROM INFORMATION_SCHEMA.COLUMNS
								WHERE LOWER(TABLE_SCHEMA) = LOWER('''+ table_list.SchemaName +''') AND LOWER(TABLE_NAME) = LOWER('''+ table_list.TableName +''') 
										AND LOWER(column_name) = LOWER('''+ table_list.ColumnName +''')
							) as meta_data
							JOIN
							(
								SELECT COUNT(*) count_all, ''' + table_list.TableName  +''' as table_name
								FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
							) table_for_count ON (LOWER('''+  table_list.TableName + ''') = LOWER(table_for_count.table_name))
							JOIN
							(
								SELECT '+  table_list.ColumnName + ' as source_column_name
								FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
							) as table_data ON (LOWER('''+  table_list.ColumnName + ''') = LOWER(meta_data.column_name))
							GROUP BY meta_data.data_type
						) as mandatory_subquery
						JOIN
						(
							SELECT TOP(1) ''' + table_list.DatabaseName + ''' as database_name, ''' + table_list.SchemaName + ''' as schema_name,
							''' + table_list.TableName + '''as table_name, ''' + table_list.ColumnName + ''' as column_name, 
							count_subquery.source_column_name,	max_subquery.max_count
							FROM
							(
								SELECT first_subquery.source_column_name, COUNT(first_subquery.source_column_name) as column_count
								FROM 
								(
									SELECT '+  table_list.ColumnName + ' as source_column_name
									FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
								) as first_subquery
								GROUP BY first_subquery.source_column_name
							) as count_subquery,
							(	
								SELECT MAX(second_subquery.column_count) as max_count
								FROM
								(
									SELECT first_subquery.source_column_name, COUNT(first_subquery.source_column_name) as column_count
									FROM 
									(
										SELECT '+  table_list.ColumnName + ' as source_column_name
										FROM ' + table_list.SchemaName  + '.' + table_list.TableName  +'
									) as first_subquery
									GROUP BY first_subquery.source_column_name
								) as second_subquery
							) as max_subquery
							WHERE count_subquery.column_count = max_subquery.max_count
						) as optional_subquery ON (mandatory_subquery.database_name = optional_subquery.database_name AND 
							mandatory_subquery.schema_name = optional_subquery.schema_name AND mandatory_subquery.table_name = optional_subquery.table_name
							AND mandatory_subquery.column_name = optional_subquery.column_name)'
			END as sql_text
		FROM table_list
	)
	SELECT @SQLCode = STRING_AGG(sql_text, '') WITHIN GROUP (ORDER BY sql_text) FROM SQLQuery;

	EXEC SP_EXECUTESQL @SQLCode;

END


EXEC db_statistics 'TRN', 'hr', '%';


DROP PROCEDURE IF EXISTS db_statistics;
