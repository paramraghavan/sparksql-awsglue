I have2 snowflake jobs , job1 is  executing  query 
create or replace temporary table temp
 taking more time than 
JOB2 USING query 
 create or replace table temp
THESE TEMP TABLES  need not persist across session
but this is invoked about 19 times in this job AND EACH QUERY  USED BY TEMPORARY TABLE SELECTS FROM DIFFERENT  TABLE
how to make this job faster