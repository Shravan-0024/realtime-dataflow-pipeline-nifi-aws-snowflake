CREATE OR REPLACE STAGE customer_ext_stage
    url='s3://apache-nifi-snowflake-sannu/fake_streaming_data/'
    credentials=(aws_key_id='aws-access-key' aws_secret_key='aws-secret-key');
   

CREATE OR REPLACE FILE FORMAT csv
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

SHOW STAGES;
LIST @customer_ext_stage;


CREATE OR REPLACE PIPE customer_s3_pipe
  AUTO_INGEST = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = csv;

SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('customer_s3_pipe');

SELECT count(*) FROM customer_raw;

TRUNCATE  customer_raw;