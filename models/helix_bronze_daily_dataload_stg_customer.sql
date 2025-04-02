-- helix_bronze_daily_dataload_stg_customer.sql

{{
    config(
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_STG',  
        unique_key="C_CODE",
        materialized='incremental'  
    )
}}

SELECT  *, {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_CUSTOMER') }}

{% if is_incremental() %}

  WHERE C_MODIFIED_DATE > (select max(C_MODIFIED_DATE)  from {{ this }})

{% endif %}