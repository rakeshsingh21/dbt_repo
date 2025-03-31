SELECT * , {{ get_ts_usr_acname() }}
from 
    DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" C 
WHERE 
    EXISTS (select n_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION A where A.n_name = C."c_nation_name" )
AND EXISTS (select r_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION B where B.r_name = C."c_region_name")    
limit 200 
