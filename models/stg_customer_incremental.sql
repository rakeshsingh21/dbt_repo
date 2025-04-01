{{
  config(materialized='incremental', unique_key='C_CODE')
}}

SELECT * , {{ get_ts_usr_acname() }}
FROM 
     {{ source('helix_poc_proj', 'RAW_CUSTOMER_NEW') }}


{% if is_incremental() %}

  WHERE C_MODIFIED_DATE > (select DATEADD(day, -3, max(C_MODIFIED_DATE) ) from {{ this }})      -- This dateadd -3 3 is if any record arrived late... Means rec updated 2 days back but arrived today 

{% endif %}
