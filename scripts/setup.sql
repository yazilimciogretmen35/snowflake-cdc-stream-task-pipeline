CREATE WAREHOUSE NEW_WAREHOUSE
WITH WAREHOUSE_SIZE='SMALL'
AUTO_SUSPEND=60
AUTO_RESUME=TRUE;

CREATE DATABASE IF NOT EXISTS DB;

CREATE SCHEMA IF NOT EXISTS NEW_SCHEMA;



USE WAREHOUSE NEW_WAREHOUSE;

USE DATABASE DB;

USE SCHEMA NEW_SCHEMA;



CREATE OR REPLACE TABLE employees_raw2 (
    id INT,
    name STRING,
    age INT,
    job STRING,
    salary INT,
    city STRING
);


CREATE OR REPLACE STREAM employees_stream2
ON TABLE employees_raw2;



CREATE OR REPLACE TABLE employees_clean2 (
    id INT,
    name STRING,
    age INT,
    job STRING,
    salary INT,
    city STRING
);



CREATE OR REPLACE STAGE employees_stage2;

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1
FIELD_DELIMITER = ';'
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

COPY INTO employees_raw2
FROM @employees_stage2
FILE_FORMAT = csv_format;





MERGE INTO employees_clean2 t
USING employees_stream2 s
ON t.id = s.id

WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN
DELETE

WHEN MATCHED THEN
UPDATE SET
    name = s.name,
    age = s.age,
    job = s.job,
    salary = s.salary,
    city = s.city

WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
INSERT (id, name, age, job, salary, city)
VALUES (s.id, s.name, s.age, s.job, s.salary, s.city);




CREATE OR REPLACE TASK employees_task2
WAREHOUSE = NEW_WAREHOUSE
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('employees_stream2')
AS
MERGE INTO employees_clean2 t
USING employees_stream2 s
ON t.id = s.id

WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE

WHEN MATCHED THEN UPDATE SET
    name = s.name,
    age = s.age,
    job = s.job,
    salary = s.salary,
    city = s.city

WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
INSERT (id, name, age, job, salary, city)
VALUES (s.id, s.name, s.age, s.job, s.salary, s.city);



ALTER TASK employees_task2 RESUME;

list @employees_stage2;

select * from employees_raw2;

select * from employees_clean2;