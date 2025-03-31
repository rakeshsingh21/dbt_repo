select count(*) from DA_HELIX_DB_TEST.HELIX_STG."stg_customer" 

use database DA_HELIX_DB_TEST;

select * from DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" limit 200;


select * from HELIX_RAW."raw_customer" limit 200;

select * from DA_HELIX_DB_DEV.HELIX_RAW.ERROR_METADATA limit 10;

select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER where c_modified_date is NOT null;


select count(*) from DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" C 
where EXISTS (select n_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION A where A.n_name = C."c_nation_name" )
and EXISTS (select r_name from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION B where B.r_name = C."c_region_name") ;

select current_warehouse();
select current_warehouse(), current_database(), current_schema();

drop view DA_HELIX_DB_DEV.HELIX_RAW.RAW_CUST;

ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF';
SELECT CURRENT_TIMESTAMP(2) as CREATED_DATE, CURRENT_USER() as CREATED_BY;
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();

select *,CURRENT_TIMESTAMP(2) as CREATED_DATE, CURRENT_USER() as CREATED_BY, row_number() over (order by "c_code") as row_number from DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" limit 200;

select get_ddl('table', 'DA_HELIX_DB_TEST.HELIX_STG.RAW_CUST');

select "c_code" from DA_HELIX_DB_TEST.HELIX_STG.RAW_CUST;


alter table DA_HELIX_DB_TEST.HELIX_STG.RAW_CUST add constraint "Unique_Customer_code" UNIQUE("c_code") enforced;

alter table DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" rename column "c_code" to C_CODE;
select * from DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" limit 10;

show constraints;
select * from DA_HELIX_DB_TEST.INFORMATION_SCHEMA.COLUMNS where table_name = 'RAW_CUST';

CUSTOMER TABLE:
------------------
create or replace TABLE DA_HELIX_DB_TEST.HELIX_STG.RAW_CUSTOMER (
	C_CODE VARCHAR(40) UNIQUE,
	C_NAME VARCHAR(25),
	C_ADDRESS VARCHAR(40),
	C_PHONE VARCHAR(15),
	C_ACCTBAL NUMBER(15,2),
	C_MKTSEGMENT  VARCHAR(10),
	C_COMMENT VARCHAR(117),
	C_NATION_NAME VARCHAR(25),
	C_NATION_COMMENT VARCHAR(152),
	C_REGION_NAME VARCHAR(25),
	C_REGION_COMMENT VARCHAR(152),
	C_MODIFIED_DATE TIMESTAMP_NTZ(9)
);

insert into DA_HELIX_DB_TEST.HELIX_STG.RAW_CUSTOMER 
select 	"c_code", "c_name", "c_address",'c_phone',"c_acctbal","c_mktsegment", "c_comment","c_nation_name", "c_nation_comment", "c_region_name"
,"c_region_comment", current_timestamp() 
from  DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" ;

insert into DA_HELIX_DB_TEST.HELIX_STG.RAW_CUSTOMER 
select 	"c_code", "c_name", "c_address",'999-999-9999',"c_acctbal","c_mktsegment", "c_comment","c_nation_name", "c_nation_comment", "c_region_name"
,"c_region_comment", current_timestamp() 
from  DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" 
where "c_code" in (60001, 60002, 60003); 

select * from DA_HELIX_DB_TEST.HELIX_RAW."raw_customer" LIMIT 10;

select * from  DA_HELIX_DB_TEST.HELIX_STG.RAW_CUST WHERE c_code in (60001, 60002, 60003); 

----------------------------------------------



