{% macro get_ts_usr_acname() %}
    CURRENT_TIMESTAMP(2) as CREATED_DATE, CURRENT_USER() as CREATED_BY, CURRENT_ACCOUNT_NAME() as INSTANCE_ID
{% endmacro %}
