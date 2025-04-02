-- helix_stg_daily_dataload_silver_supplier.sql

{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_CDW',
        unique_key='s_supp_code' 
    )
}}

SELECT seq_stg_part.nextval as suppkey, *, 
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('stg_supplier')}} 
-- INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.nation B where B.n_name = A.c_nation_name
-- qualify row_number() over(partition by n_nationkey order by n_nationkey) =1 

{% if is_incremental() %}

   WHERE S_MODIFIED_DATE > (select max(S_MODIFIED_DATE)  from {{ this }})

{% endif %}