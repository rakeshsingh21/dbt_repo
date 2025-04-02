{% snapshot stg_customer_snapshot %}

{{
    config(
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_STG',  
        unique_key="C_CODE",
        strategy='check',
        check_cols=["C_PHONE"]
    )
}}

SELECT * , {{ get_ts_usr_acname() }}
from 
    {{ source('helix_poc_proj', 'RAW_CUSTOMER_NEW') }} C
WHERE 
    EXISTS (select n_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION A where A.n_name = C.c_nation_name )
AND EXISTS (select r_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION B where B.r_name = C.c_region_name)    

{% endsnapshot %} 
