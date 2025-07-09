-- Stream monitoring and DML
SHOW STREAMS;
SELECT * FROM customer_table_changes;

SELECT CURRENT_ROLE();

INSERT INTO customer VALUES(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut','Cape Verde',CURRENT_TIMESTAMP());

UPDATE customer SET FIRST_NAME='SK', update_timestamp = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ WHERE customer_id=223136;

DELETE FROM customer WHERE customer_id =77;

SELECT * FROM customer_history WHERE customer_id IN (72,73,223136);
SELECT * FROM customer_table_changes WHERE customer_id = 223136;
SELECT * FROM customer WHERE customer_id IN (72,73,223136);

-- View Creation
CREATE OR REPLACE VIEW v_customer_change_data AS
SELECT 
CUSTOMER_ID, 
FIRST_NAME, 
LAST_NAME,
EMAIL, 
STREET, 
CITY,
STATE,
COUNTRY,
start_time, 
end_time, 
is_current, 
'I' AS dml_type
FROM (
    SELECT 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    STREET, 
    CITY,
    STATE,
    COUNTRY,
    update_timestamp AS start_time,
    LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp ASC) AS end_time_raw,
    CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE end_time_raw END AS end_time,
    CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM (
        SELECT 
        CUSTOMER_ID, 
        FIRST_NAME, 
        LAST_NAME, 
        EMAIL, 
        STREET, 
        CITY,
        STATE,
        COUNTRY,
        UPDATE_TIMESTAMP
        FROM customer_table_changes
        WHERE metadata$action = 'INSERT'
        AND metadata$isupdate = 'FALSE'
    )
)
UNION
SELECT 
CUSTOMER_ID, 
FIRST_NAME, 
LAST_NAME, 
EMAIL, 
STREET, 
CITY,
STATE,
COUNTRY, 
start_time, 
end_time, 
is_current, 
dml_type
FROM (
    SELECT 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    STREET, 
    CITY,
    STATE,
    COUNTRY,
    update_timestamp AS start_time,
    LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp ASC) AS end_time_raw,
    CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE end_time_raw END AS end_time,
    CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current, 
    dml_type
    FROM (
        SELECT 
        CUSTOMER_ID, 
        FIRST_NAME, 
        LAST_NAME, 
        EMAIL, 
        STREET, 
        CITY,
        STATE,
        COUNTRY, 
        update_timestamp, 
        'I' AS dml_type
        FROM customer_table_changes
        WHERE metadata$action = 'INSERT'
        AND metadata$isupdate = 'TRUE'
        UNION
        SELECT 
        CUSTOMER_ID, 
        NULL,
        NULL, 
        NULL, 
        NULL,
        NULL,
        NULL,
        NULL, 
        start_time, 
        'U' AS dml_type
        FROM customer_history
        WHERE customer_id IN (
            SELECT 
            DISTINCT customer_id 
            FROM customer_table_changes
            WHERE metadata$action = 'DELETE'
            AND metadata$isupdate = 'TRUE'
        )
        AND is_current = TRUE
    )
)
UNION
SELECT 
ctc.CUSTOMER_ID, NULL, NULL, NULL, NULL, NULL,NULL,NULL, 
ch.start_time, 
CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, NULL, 
'D'
FROM customer_history ch
INNER JOIN customer_table_changes ctc
   ON ch.customer_id = ctc.customer_id
WHERE ctc.metadata$action = 'DELETE'
AND ctc.metadata$isupdate = 'FALSE'
AND ch.is_current = TRUE;

SELECT * FROM v_customer_change_data;

-- Task creation for merge
CREATE OR REPLACE TASK tsk_scd_hist WAREHOUSE= COMPUTE_WH SCHEDULE='1 MINUTE'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
AS
MERGE INTO customer_history ch
USING v_customer_change_data ccd
   ON ch.CUSTOMER_ID = ccd.CUSTOMER_ID
   AND ch.start_time = ccd.start_time
WHEN MATCHED AND ccd.dml_type = 'U' THEN UPDATE
    SET ch.end_time = ccd.end_time,
        ch.is_current = FALSE
WHEN MATCHED AND ccd.dml_type = 'D' THEN UPDATE
    SET ch.end_time = ccd.end_time,
        ch.is_current = FALSE
WHEN NOT MATCHED AND ccd.dml_type = 'I' THEN INSERT
          (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, start_time, end_time, is_current)
    VALUES (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY,ccd.STATE,ccd.COUNTRY, ccd.start_time, ccd.end_time, ccd.is_current);

SHOW TASKS;
ALTER TASK tsk_scd_hist RESUME; -- SUSPEND

-- Test DML
INSERT INTO customer VALUES(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut','Cape Verde',CURRENT_TIMESTAMP());
UPDATE customer SET FIRST_NAME='Jessica' WHERE customer_id=7523;
DELETE FROM customer WHERE customer_id =136 AND FIRST_NAME = 'Kim';

SELECT COUNT(*),customer_id FROM customer GROUP BY customer_id HAVING COUNT(*)=1;
SELECT * FROM customer_history WHERE customer_id =223136;
SELECT * FROM customer_history WHERE IS_CURRENT=TRUE;

SELECT TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, scheduled_time) AS next_run, scheduled_time, CURRENT_TIMESTAMP, name, state 
FROM TABLE(information_schema.task_history()) WHERE state = 'SCHEDULED' ORDER BY completed_time DESC;

SELECT * FROM customer_history WHERE IS_CURRENT=FALSE;

ALTER TASK tsk_scd_hist SUSPEND;