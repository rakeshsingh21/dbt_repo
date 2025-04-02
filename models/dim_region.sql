{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_CDW',
        unique_key='c_region_name' 
    )
}}

SELECT -- B.r_regionkey as r_regionkey, 
    seq_stg_part.nextval as r_regionkey,
     A.c_region_name as r_region_name, A.c_region_comment as r_region_comment, 
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('stg_customer')}} A
--INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION B where B.r_name = A.c_region_name
QUALIFY row_number() over(partition by c_region_name order by c_region_name) =1
