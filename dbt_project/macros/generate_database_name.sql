{#
    Decides which database each model is written to.

    dev  : always the single DuckDB catalog from the profile (nothing changes).
    prod : Silver (the "staging" schema group, including the holiday seed) goes to the STAGING database,
           Gold goes to the profile database PROD. This matches the blueprint RAW -> STAGING -> PROD,
           so BI_READER_ROLE (PROD only) never needs access to Silver.
#}
{% macro generate_database_name(custom_database_name, node) -%}
    {%- if target.name == 'prod' and node.config.get('schema') == 'staging' -%}
        STAGING
    {%- elif custom_database_name is none -%}
        {{ target.database }}
    {%- else -%}
        {{ custom_database_name | trim }}
    {%- endif -%}
{%- endmacro %}
