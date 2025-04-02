-- helix_bronze_daily_dataload_stg_partssupplier.sql

{{
    config(
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_STG',  
        materialized='incremental', 
        unique_key='ps_supp_code'
        )
}}

SELECT seq_stg_part.nextval as ps_suppkey, * , {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_PARTSSUPPLIER') }}

{% if is_incremental() %}

  WHERE ps_modified_date > (select max(ps_modified_date)  from {{ this }})

{% endif %}