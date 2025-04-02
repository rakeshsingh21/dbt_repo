-- helix_bronze_daily_dataload_stg_ordr_lineitem_details.sql

{{
    config(
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_STG',  
        materialized='incremental', 
        unique_key='l_order_line_number'
        )
}}

SELECT  * , {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_LINEITEM') }}

{% if is_incremental() %}

  WHERE l_modified_date > (select max(l_modified_date)  from {{ this }})

{% endif %}