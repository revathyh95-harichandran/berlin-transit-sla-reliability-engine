{#
    Stand-ins used ONLY by the sqlfluff linter (see .sqlfluff). The linter reads plain Jinja and does not
    know dbt, so these stubs mimic dbt functions and this project's macros with simple DuckDB SQL.
    dbt itself never loads this folder.
#}
{% macro ref(name) -%}{{ name }}{%- endmacro %}
{% macro source(source_name, table_name) -%}{{ source_name }}.{{ table_name }}{%- endmacro %}
{% macro config() -%}{% if kwargs %}{% endif %}{%- endmacro %}
{% macro is_incremental() -%}{%- endmacro %}
{% macro var(name, default=none) -%}1{%- endmacro %}
{% macro to_berlin_time(column) -%}timezone('Europe/Berlin', timezone('UTC', {{ column }})){%- endmacro %}
{% macro as_utc_timestamp(column) -%}timezone('UTC', {{ column }}){%- endmacro %}
{% macro sha256_hex(expression) -%}sha256({{ expression }}){%- endmacro %}
{% macro hash_pii(column) -%}sha256(cast({{ column }} as varchar)){%- endmacro %}
{% macro point_in_polygon(geojson_column, longitude, latitude) -%}
    st_contains(st_geomfromgeojson({{ geojson_column }}), st_point({{ longitude }}, {{ latitude }}))
{%- endmacro %}
{% macro add_days(date_expression, days_expression) -%}({{ date_expression }} + {{ days_expression }}){%- endmacro %}
{% macro add_hours(timestamp_expression, hours_expression) -%}
    ({{ timestamp_expression }} + to_hours({{ hours_expression }}))
{%- endmacro %}
{% macro iso_day_of_week(date_expression) -%}isodow({{ date_expression }}){%- endmacro %}
{% macro surrogate_key(columns) -%}md5(concat_ws('|', {{ columns | join(', ') }})){%- endmacro %}
