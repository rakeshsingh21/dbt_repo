-- helix_bronze_daily_dataload_stg_supplier.sql

{{
    config(
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_STG',  
        materialized='incremental', 
        unique_key='s_supp_code'
        )
}}

SELECT  * , {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_SUPPLIER') }}

{% if is_incremental() %}

  WHERE S_MODIFIED_DATE > (select max(S_MODIFIED_DATE)  from {{ this }})

{% endif %}