

-- CAUTION:VALID FOR MYSQL 8.0.41 OR ABOVE

USE query_logger;

/*This SQL statement creates a `query_log` table to store metadata about executed SQL queries, including timing, user, query text, and performance metrics.
 It defines appropriate data types and indexes to optimize search and analysis. The table supports query auditing, performance monitoring, and debugging.*/


-- 1. TABLE DEFINITION (Your definition is good, no changes needed)
CREATE TABLE IF NOT EXISTS `query_log` (
  `log_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `executed_at` TIMESTAMP NOT NULL,
  `executed_by_user` VARCHAR(128) NOT NULL,
  `client_host` VARCHAR(255) NOT NULL,
  `query_type` ENUM('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'OTHER') NOT NULL,
  `table_name` VARCHAR(255) DEFAULT NULL,
  `query_text` TEXT NOT NULL,
  `query_hash` CHAR(64) NOT NULL,
  `duration_ms` DECIMAL(10, 3) NOT NULL,
  `rows_examined` INT UNSIGNED NOT NULL,
  `rows_sent` INT UNSIGNED NOT NULL,
  `used_an_index` BOOLEAN NOT NULL,
  `execution_plan` JSON DEFAULT NULL,
  `error_message` TEXT DEFAULT NULL,
  `is_slow_query` BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (`log_id`),
  INDEX `idx_executed_at` (`executed_at`),
  INDEX `idx_query_type` (`query_type`),
  INDEX `idx_table_name` (`table_name`),
  INDEX `idx_query_hash` (`query_hash`),
  INDEX `idx_hash_duration` (`query_hash`, `duration_ms`),
  INDEX `idx_filter_sort` (`filter_clause`, `sort_clause`),
  INDEX `idx_executed_by_user` (`executed_by_user`),
  INDEX `idx_duration_ms` (`duration_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Logs executed SQL queries and their performance metadata.';

-- Some debugging only 

-- ALTER TABLE query_log
-- DROP INDEX idx_cover_querylog,
-- aDD INDEX idx_hash_duration (query_hash, duration_ms);

-- ALTER TABLE query_log
--   ADD INDEX idx_filter_sort (
--     used_an_index,
--     query_type,
--     duration_ms
--   );

/*This script configures MySQL to log slow queries to a table, then defines a stored procedure, 
`sp_process_query_logs`, to process these entries. The procedure parses details like user, query type, and duration, 
then stores them in a custom `query_log` table. An event scheduler, `evt_process_query_logs`, automates this process every 5 minutes. 
This entire setup systematically monitors and logs slow queries for performance analysis.*/

-- 2. SETUP GLOBAL VARIABLES (Corrected)
-- Ensure logging is ON and directed to the TABLE so the procedure can read it.
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL log_output = 'TABLE'; -- This is the key fix. Log to TABLE, not FILE.
SET GLOBAL long_query_time = 0.1; -- Log queries longer than 100ms.


-- 3. STORED PROCEDURE (Corrected and Improved)

DELIMITER $$

-- CREATE PROCEDURE IF NOT EXISTS is now used for idempotent creation.
-- This requires MySQL 5.7.7+ or MariaDB 10.1.2+.
-- If your MySQL version is older, you might need to revert to 'DROP PROCEDURE IF EXISTS'
CREATE PROCEDURE IF NOT EXISTS `sp_process_query_logs`(IN slow_query_threshold_ms INT)
BEGIN
    -- Declare variables to hold data from the slow_log table
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_user_host VARCHAR(255);
    DECLARE v_query_time TIME;
    DECLARE v_sql_text TEXT;
    DECLARE v_rows_sent INT;
    DECLARE v_rows_examined INT;

    -- Declare variables for our custom log table
    DECLARE v_query_type ENUM('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'OTHER');
    DECLARE v_table_name VARCHAR(255); -- Added variable to store the parsed table name
    DECLARE v_executed_by_user VARCHAR(128);
    DECLARE v_client_host VARCHAR(255);
    DECLARE v_duration_ms DECIMAL(10, 3);
    DECLARE v_execution_plan JSON;
    DECLARE v_used_an_index BOOLEAN;
    
    -- User-defined variable for cursor completion. No explicit DECLARE needed for @done.
    -- It will be implicitly created when first SET.

    -- Cursor to iterate through new entries in the slow log
    -- It selects entries that have not yet been processed (based on the max executed_at in query_log)
    DECLARE log_cursor CURSOR FOR
        SELECT start_time, user_host, query_time, sql_text, rows_sent, rows_examined
        FROM mysql.slow_log
        WHERE start_time > (SELECT IFNULL(MAX(executed_at), '1970-01-01 00:00:00') FROM query_log); -- Ensure a valid default timestamp

    -- Declare continue handler to exit loop when no more rows are found by the cursor
    -- @done will be implicitly created and set to TRUE when NOT FOUND occurs
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET @done = TRUE;
    
    -- Open the cursor to start fetching data
    OPEN log_cursor;
    
    -- Loop through each record fetched by the cursor
    log_loop: LOOP
        -- Initialize @done for each iteration to ensure correct loop control
        -- This reset is crucial for the loop's first execution or if cursor could be empty initially.
        SET @done = FALSE; 

        FETCH log_cursor INTO v_start_time, v_user_host, v_query_time, v_sql_text, v_rows_sent, v_rows_examined;
        
        -- Exit the loop if no more rows are found (i.e., @done is set to TRUE by the handler)
        IF @done THEN
            LEAVE log_loop;
        END IF;
        
        -- 1. Parse User and Host from the 'user_host' string (e.g., 'root[root] @ localhost [127.0.0.1]')
        SET v_executed_by_user = SUBSTRING_INDEX(v_user_host, ' @ ', 1);
        SET v_client_host = SUBSTRING_INDEX(v_user_host, ' @ ', -1);
        
        -- 2. Convert query time (TIME datatype) to total milliseconds
        SET v_duration_ms = (TIME_TO_SEC(v_query_time) * 1000) + (MICROSECOND(v_query_time) / 1000);
        
        -- 3. Identify Query Type based on the SQL text's starting keyword
        SET v_query_type = CASE
            WHEN v_sql_text LIKE 'SELECT%' THEN 'SELECT'
            WHEN v_sql_text LIKE 'INSERT%' THEN 'INSERT'
            WHEN v_sql_text LIKE 'UPDATE%' THEN 'UPDATE'
            WHEN v_sql_text LIKE 'DELETE%' THEN 'DELETE'
            WHEN v_sql_text LIKE 'CREATE%' THEN 'CREATE'
            WHEN v_sql_text LIKE 'ALTER%' THEN 'ALTER'
            WHEN v_sql_text LIKE 'DROP%' THEN 'DROP'
            ELSE 'OTHER'
        END;

        -- 4. Extract Table Name (Best Effort Parsing)
        -- Note: This is a simplified parser. It may not correctly identify the table name in complex queries
        -- (e.g., queries with joins or subqueries). It primarily looks for the first table name 
        -- found after common DML/DDL keywords. Case-insensitivity is handled by checking against v_query_type.
        SET v_table_name = NULL; -- Reset for each query

        CASE v_query_type
            WHEN 'SELECT' THEN -- Looks for the word after 'FROM'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'FROM ', -1), ' ', 1);
            WHEN 'INSERT' THEN -- Looks for the word after 'INTO'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'INTO ', -1), ' ', 1);
            WHEN 'UPDATE' THEN -- Looks for the word after 'UPDATE'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'UPDATE ', -1), ' ', 1);
            WHEN 'DELETE' THEN -- Looks for the word after 'FROM'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'FROM ', -1), ' ', 1);
            WHEN 'CREATE' THEN -- Looks for the word after 'TABLE'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'TABLE ', -1), '(', 1);
            WHEN 'ALTER' THEN -- Looks for the word after 'TABLE'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'TABLE ', -1), ' ', 1);
            WHEN 'DROP' THEN -- Looks for the word after 'TABLE'
                SET v_table_name = SUBSTRING_INDEX(SUBSTRING_INDEX(UPPER(v_sql_text), 'TABLE ', -1), ' ', 1);
            ELSE
                SET v_table_name = NULL;
        END CASE;

        -- Clean up potential backticks or other characters from the extracted name
        IF v_table_name IS NOT NULL THEN
            SET v_table_name = REPLACE(v_table_name, '`', '');
            SET v_table_name = TRIM(v_table_name);
        END IF;
        
        -- 5. Execution Plan and Index Usage (Placeholder for MySQL Compatibility)
        -- Direct capture of EXPLAIN FORMAT=JSON output into a variable

-- 4. EVENT SCHEDULER (Corrected)
-- Ensure the event scheduler is running persistently (add event_scheduler=ON to my.cnf)
SET GLOBAL event_scheduler = ON;
-- 1. Change the delimiter from ; to $$
DELIMITER $$

-- 2. Now, create the entire event. The client will ignore the internal semicolon.
CREATE EVENT IF NOT EXISTS `evt_process_query_logs` ON SCHEDULE EVERY 5 MINUTE DO 
BEGIN    
 -- This threshold (500ms) is passed to the procedure to flag slow queries.    
 CALL sp_process_query_logs(500); 
 END


DELIMITER $$



/*This SQL query groups and counts identical queries based on their `query_hash` from the `query_log`. 
It calculates the average execution duration for each unique query. 
The purpose is to identify the 20 most frequently executed queries and their typical performance, 
allowing for targeted optimization efforts on frequently run operations.*/

SELECT
    query_hash,
    COUNT(*) AS execution_count,
    AVG(duration_ms) AS avg_duration_ms,
    ANY_VALUE(query_text) AS sample_query
FROM
    query_log
GROUP BY
    query_hash
ORDER BY
    execution_count DESC
LIMIT 20;

/*This SQL query retrieves details of the 50 slowest `SELECT` queries from the `query_log` that did not utilize an index and took longer than 100 milliseconds to execute. 
It orders the results by duration and then by rows examined in descending order to highlight the most problematic queries. 
The purpose is to identify specific inefficient SELECT statements for optimization.*/

SELECT
    log_id,
    executed_at,
    duration_ms,
    rows_examined,
    query_text
FROM
    query_log
WHERE
    query_type = 'SELECT'
AND used_an_index = FALSE
AND duration_ms > 100 -- Filter for queries that took more than 100ms
ORDER BY
    duration_ms DESC, rows_examined DESC
LIMIT 50;

/*This SQL code analyzes `query_log` to count occurrences of specific SQL keywords (like `FROM`, `WHERE`, `JOIN`) across all queries. 
It first cleans the raw query text by removing comments and quoted strings, then calculates counts efficiently in a single pass. 
Finally, it presents these counts in a clear, unpivoted format, providing insights into common query patterns.*/

-- Step 1 & 2: Clean the data and calculate all counts in a single pass.
-- This avoids scanning the table and running REGEXP_REPLACE multiple times.
WITH cleaned_and_counted AS (
  SELECT
    -- Count for 'FROM'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'FROM', ''))) / 4) AS UNSIGNED) AS from_count,

    -- Count for 'WHERE'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'WHERE', ''))) / 5) AS UNSIGNED) AS where_count,

    -- Count for 'JOIN'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'JOIN', ''))) / 4) AS UNSIGNED) AS join_count,

    -- Count for 'GROUP BY'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'GROUP BY', ''))) / 8) AS UNSIGNED) AS group_by_count,

    -- Count for 'HAVING'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'HAVING', ''))) / 6) AS UNSIGNED) AS having_count,

    -- Count for 'ORDER BY'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'ORDER BY', ''))) / 8) AS UNSIGNED) AS order_by_count,

    -- Count for 'LIMIT'
    CAST(SUM((LENGTH(uc_txt) - LENGTH(REPLACE(uc_txt, 'LIMIT', ''))) / 5) AS UNSIGNED) AS limit_count

  FROM (
    -- This subquery performs the expensive cleaning only ONCE.
    SELECT
      UPPER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  query_text,
                  '''[^'']*''',                      -- remove single-quoted strings
                  ''
                ),
                '"[^"]*"',                          -- remove double-quoted strings
                ''
              ),
              '/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/',  -- remove block comments
              ''
            ),
            '--[^\n\r]*',                          -- remove -- comments
            ''
          ),
          '#[^\n\r]*',                            -- remove # comments
          ''
        )
      ) AS uc_txt
    FROM query_log
  ) AS cleaned
)

-- Step 3: Unpivot the single row of results into the desired final format.
-- This is a very fast operation on the already-aggregated data.
SELECT 'FROM',     from_count     FROM cleaned_and_counted
UNION ALL
SELECT 'WHERE',    where_count    FROM cleaned_and_counted
UNION ALL
SELECT 'JOIN',     join_count     FROM cleaned_and_counted
UNION ALL
SELECT 'GROUP BY', group_by_count FROM cleaned_and_counted
UNION ALL
SELECT 'HAVING',   having_count   FROM cleaned_and_counted
UNION ALL
SELECT 'ORDER BY', order_by_count FROM cleaned_and_counted
UNION ALL
SELECT 'LIMIT',    limit_count    FROM cleaned_and_counted;

/*This SQL view, `vw_slow_queries`, filters the `query_log` table to show only queries marked as slow. 
It provides key details like execution time, user, duration, rows examined, and the query text. 
The purpose is to easily identify and analyze performance bottlenecks within the database.*/

CREATE OR REPLACE VIEW `vw_slow_queries` AS
SELECT
    log_id,
    executed_at,
    executed_by_user,
    duration_ms,
    rows_examined,
    query_text
FROM
    query_log
WHERE
    is_slow_query = TRUE;

/*This view aggregates query performance metrics to identify the most frequent and resource-intensive database operations. 
Its purpose is to provide a high-level overview for targeted performance tuning.
 This helps pinpoint whether operations like `SELECT`s or `UPDATE`s are causing system-wide bottlenecks.*/
 
CREATE OR REPLACE VIEW `vw_query_type_performance` AS
SELECT
    query_type,
    COUNT(*) AS total_executions,
    AVG(duration_ms) AS avg_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    SUM(rows_examined) AS total_rows_examined
FROM
    query_log
GROUP BY
    query_type;