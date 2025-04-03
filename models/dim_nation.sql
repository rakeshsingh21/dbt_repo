{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_CDW',
        unique_key='c_region_name' 
    )
}}

SELECT --B.n_nationkey as n_nationkey, 
        seq_stg_part.nextval as nationkey,
        B.r_regionkey as n_regionkey , A.c_nation_name as c_nation_name, A.c_nation_comment as c_nation_comment, 
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('stg_customer')}} A
-- INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.nation B where B.n_name = A.c_nation_name  -- c_region_name
    INNER JOIN {{ ref('stg_region')}} B where B.r_name A.c_region_name
qualify row_number() over(partition by c_nation_name order by c_nation_name) =1 
