WITH
	json_string AS
	(
		SELECT '[{"employee_id": "5181816516151", "department_id": "1", "class": "src\bin\comp\json"}, {"employee_id": "925155", "department_id": "1", "class": "src\bin\comp\json"}, {"employee_id": "815153", "department_id": "2", "class": "src\bin\comp\json"}, {"employee_id": "967", "department_id": "", "class": "src\bin\comp\json"}]' [str]
	),
	-- In this task I decided to create 7 columns. 
	--		[iteration_number] - uses to count number of iteration, will be used in the main select section 
    --		[string_cut] - this column stores one separate JSON object from all JSON objects (elements). With each subsequent iteration this column stores next object.
	--		[string_left] - this column stores whole JSON except objects that were processed before 
	--      [employee_id_string] - this column stores string that contains employee_id value, but value is not processed yet
	--		[department_id_string] - same purpose as for previous column, but for department_id
	--      [employee_id] - this column contains processed value employee_id from the employee_id_string column
	--		[department_id] - same purpose, but for department_id 
	parse_string ([iteration_number], [string_cut], [string_left], [employee_id_string], [department_id_string], [employee_id], [department_id])
	AS
	(
		SELECT 
			CAST(1 AS int) [iteration_number],
			-- Each JSON object ends by '}' symbol, so I deside to count this symbol as end of one JSON object
			CAST(LEFT([str], CHARINDEX('}', [str]) + 1) AS varchar(1024)) as [string_cut],
			-- And remove processed JSON object from main JSON
			REPLACE([str], LEFT([str], CHARINDEX('}', [str]) + 1), '') as [string_left],
			CAST(NULL AS varchar(1024)) as [employee_id_string],
			CAST(NULL AS varchar(1024)) as [department_id_string],
			-- employee_id is casted to bigint type
			CAST(NULL AS bigint) as [employee_id],
			-- department_id is casted to int type
			CAST(NULL AS int) as [department_id]
		FROM json_string
		UNION ALL 
		SELECT 
			CAST([iteration_number] + 1 as INT) as [iteration_number],
			CAST(LEFT([string_left], CHARINDEX('}', [string_left]) + 1) AS varchar(1024)) as [string_cut],
			REPLACE([string_left], LEFT([string_left], CHARINDEX('}', [string_left]) + 1), '') as [string_left],
			CASE
				-- If JSON object does not contain "employee_id", I mark this as NULL value for employee_id
				-- It is strange for me, but CHARINDEX function looks for specific literal (word) in both lower and upper case. So I don't need to LOWER() all JSON before processing
				WHEN CHARINDEX('"employee_id":', [string_cut]) = 0 
					THEN NULL
				-- If JSON object contains "employee_id", I take string starting from the value from key "employee_id" (example "employee_id": "123", ...)
				ELSE CAST(TRIM(SUBSTRING([string_cut], CHARINDEX('"employee_id":', [string_cut]) + LEN('"employee_id":'), LEN([string_cut]))) AS varchar(1024)) 
			END AS [employee_id_string],
			-- same case for department_id
			CASE
				WHEN CHARINDEX('"department_id":', [string_cut]) = 0 THEN NULL
				ELSE CAST(TRIM(SUBSTRING([string_cut], CHARINDEX('"department_id":', [string_cut]) + LEN('"department_id":'), LEN([string_cut]))) AS varchar(1024)) 
			END AS [department_id_string],
			CASE 
				-- Firstly I processed my mark from previous step: if JSON object does not contain "employee_id" I just put NULL
				WHEN [employee_id_string] IS NULL 
					THEN NULL
				-- If JSON object contains key "employee_id" but not value (example "employee_id": ""), I count it as NULL value
				WHEN CAST(LEFT(RIGHT([employee_id_string], LEN([employee_id_string]) - 1), CHARINDEX('"', RIGHT([employee_id_string], LEN([employee_id_string]) - 1)) - 1) as bigint) = '' 
					THEN NULL
				-- In all other cases I assume that will be used number and process it
				ELSE CAST(LEFT(RIGHT([employee_id_string], LEN([employee_id_string]) - 1), CHARINDEX('"', RIGHT([employee_id_string], LEN([employee_id_string]) - 1)) - 1) as bigint)
			END as [employee_id],
			-- same case for department_id
			CASE 
				WHEN [department_id_string] IS NULL THEN NULL
				WHEN CAST(LEFT(RIGHT([department_id_string], LEN([department_id_string]) - 1), CHARINDEX('"', RIGHT([department_id_string], LEN([department_id_string]) - 1)) - 1) as int) = '' THEN NULL
				ELSE CAST(LEFT(RIGHT([department_id_string], LEN([department_id_string]) - 1), CHARINDEX('"', RIGHT([department_id_string], LEN([department_id_string]) - 1)) - 1) as int)
			END as [department_id]
		FROM parse_string
		-- I continue parsing while main JSON is not empty, or preprocessed string with "employee_id" value is not empty, or preprocessed string with "department_id" value is not empty  
		WHERE LEN([string_cut]) > 1 OR LEN([employee_id_string]) > 1 OR LEN([department_id_string]) > 1
	)
SELECT [employee_id], [department_id]
FROM parse_string
-- I limit select because first two iterations do not process values from "employee_id" and "department_id"
WHERE iteration_number > 2;
