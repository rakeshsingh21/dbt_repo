{% macro get_rec_count() %}
   {{log("Starting ", info=true)}}     # THIS INFO GETTING PRINTED ON LOG FILES/CONSOLE

    {% set query %}

        SELECT count(*)  FROM 
        {{ source('helix_poc_proj', 'RAW_PART') }}
        WHERE P_MODIFIED_DATE > (select COALESCE(max(P_MODIFIED_DATE), TO_DATE('1900-01-01')) FROM {{ this }})

    {% endset %}

    {% if execute %}
        {{log("Executing Count_metadata macro", info=true)}}
        {% set count_result = run_query(query) %}
        {{log("Just before printing the count_result", info=true)}}
        {{log(count_result, info=true)}}

        {% set count_result_value = count_result.columns[0].values()[0] %}

        {{log("Just before printing the count_result_VALUE", info=true)}}
        {{log(count_result_value, info=true)}}        #NOW THIS IS ABLE TO GET THE EXACT COUNT  
        

    {% endif %}
