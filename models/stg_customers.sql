select
    id as customer_id,
    first_name,
    last_name
from {{ source('customer_orders', 'customers') }}
/*from raw.jaffle_shop.customers */

-----------------

04/02

{{
    config(
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_STG',  
        unique_key="C_CODE",
        materialized='incremental'  
    )
}}

SELECT seq_stg_part.nextval as c_custkey, *, {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_CUSTOMER_NEW') }}

{% if is_incremental() %}

  WHERE C_MODIFIED_DATE > (select max(C_MODIFIED_DATE)  from {{ this }})

{% endif %}
