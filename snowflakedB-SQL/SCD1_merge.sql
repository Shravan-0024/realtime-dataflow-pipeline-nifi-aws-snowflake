MERGE INTO customer c 
USING customer_raw cr
   ON  c.customer_id = cr.customer_id
WHEN MATCHED AND c.customer_id <> cr.customer_id OR
                 c.first_name  <> cr.first_name  OR
                 c.last_name   <> cr.last_name   OR
                 c.email       <> cr.email       OR
                 c.street      <> cr.street      OR
                 c.city        <> cr.city        OR
                 c.state       <> cr.state       OR
                 c.country     <> cr.country THEN UPDATE
    SET c.customer_id = cr.customer_id
       ,c.first_name  = cr.first_name 
       ,c.last_name   = cr.last_name  
       ,c.email       = cr.email      
       ,c.street      = cr.street     
       ,c.city        = cr.city       
       ,c.state       = cr.state      
       ,c.country     = cr.country  
       ,update_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    VALUES (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);

SELECT count(*) FROM customer;

CREATE OR REPLACE PROCEDURE pdr_scd_demo()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
  var cmd = `
             MERGE INTO customer c 
             USING customer_raw cr
                ON  c.customer_id = cr.customer_id
             WHEN MATCHED AND c.customer_id <> cr.customer_id OR
                              c.first_name  <> cr.first_name  OR
                              c.last_name   <> cr.last_name   OR
                              c.email       <> cr.email       OR
                              c.street      <> cr.street      OR
                              c.city        <> cr.city        OR
                              c.state       <> cr.state       OR
                              c.country     <> cr.country THEN UPDATE
                 SET c.customer_id = cr.customer_id
                     ,c.first_name  = cr.first_name 
                     ,c.last_name   = cr.last_name  
                     ,c.email       = cr.email      
                     ,c.street      = cr.street     
                     ,c.city        = cr.city       
                     ,c.state       = cr.state      
                     ,c.country     = cr.country  
                     ,update_timestamp = CURRENT_TIMESTAMP()
             WHEN NOT MATCHED THEN INSERT
                        (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                 VALUES (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
  `
  var cmd1 = "TRUNCATE TABLE SCD_DEMO.SCD2.customer_raw;"
  var sql = snowflake.createStatement({sqlText: cmd});
  var sql1 = snowflake.createStatement({sqlText: cmd1});
  var result = sql.execute();
  var result1 = sql1.execute();
  return cmd+'\n'+cmd1;
$$;

CALL pdr_scd_demo();

-- Set up TASKADMIN role
USE ROLE securityadmin;
CREATE OR REPLACE ROLE taskadmin;

-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to TASKADMIN
USE ROLE accountadmin;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE taskadmin;

-- Set the active role to SECURITYADMIN to show that this role can grant a role to another role 
USE ROLE securityadmin;
GRANT ROLE taskadmin TO ROLE sysadmin;

USE ROLE accountadmin;

CREATE OR REPLACE TASK tsk_scd_raw WAREHOUSE = COMPUTE_WH SCHEDULE = '1 MINUTE'
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL pdr_scd_demo();

SHOW TASKS;
ALTER TASK tsk_scd_raw RESUME; -- SUSPEND
SHOW TASKS;

SELECT TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, scheduled_time) AS next_run, scheduled_time, CURRENT_TIMESTAMP, name, state 
FROM TABLE(information_schema.task_history()) 
WHERE state = 'SCHEDULED' 
ORDER BY completed_time DESC;

SELECT count(*) FROM customer;

ALTER TASK tsk_scd_raw SUSPEND;