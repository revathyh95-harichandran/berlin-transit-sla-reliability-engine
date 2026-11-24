{#
    Decides which schema (folder) each model is written to.

    dev  : <target schema>_<custom schema>, e.g. dev_staging and dev_marts.
           Every developer gets their own folders, like personal drawers in a shared cabinet.
    prod : Silver lands in STAGING.SILVER (database chosen in generate_database_name), Gold in PROD.MARTS.
           Production has one official set of folders, so nothing gets a prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' and custom_schema_name | trim == 'staging' -%}
        SILVER
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
