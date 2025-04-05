select A.TASK_ID, A.JOB_ID, B.BATCH_ID
FROM
    {{this.database}}.HELIX_AUDIT.TASK_METADATA A
    INNER JOIN {{var('database')}}.{{var('audit_schema')}}.JOB_METADATA B ON B.JOB_ID = A.JOB_ID
where 
    A.TASK_NAME = CONCAT('models/bronze/', 'helix_bronze_daily_dataload_stg_customers')  -- ,{{this.name}})

-- Snowflake
select CONCAT('models/bronze/','helix_bronze_daily_dataload_stg_customers');

select A.TASK_ID, A.JOB_ID, B.BATCH_ID 
FROM
    DA_HELIX_DB_TEST.HELIX_AUDIT.TASK_METADATA A
    INNER JOIN DA_HELIX_DB_TEST.HELIX_AUDIT.JOB_METADATA B ON B.JOB_ID = A.JOB_ID
where 
    A.TASK_NAME = 'models/bronze/helix_bronze_daily_dataload_stg_customer.sql' ;   -- under single quote it works not in double quote
    CONCAT('"','models/bronze/', 'helix_bronze_daily_dataload_stg_customer.sql','"')  ; -- ,inline_query)
