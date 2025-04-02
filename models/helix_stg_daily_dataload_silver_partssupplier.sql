-- helix_stg_daily_dataload_silver_partssupplier.sql
{{
    config( 
        target_database='DA_HELIX_DB_TEST',
        target_schema='HELIX_CDW',
        unique_key='ps_supp_code' 
    )
}}

SELECT seq_stg_part.nextval as suppkey, A.*, 
       {{ get_ts_usr_acname() }} 
FROM 
    {{ ref('stg_partssupplier')}} A
    INNER JOIN {{ ref('dim_part') B ON B.ps_part_code = A.ps_part_code
	INNER JOIN {{ ref('dim_supplier') C ON C.ps_supp_code = A.ps_supp_code
	qualify row_number() over(partition by ps_supp_code order by ps_supp_code) =1 

{% if is_incremental() %}

   WHERE PS_MODIFIED_DATE > (select max(PS_MODIFIED_DATE)  from {{ this }})

{% endif %}