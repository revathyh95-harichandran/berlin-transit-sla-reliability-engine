{#
    Small translators for SQL that is spelled differently in DuckDB (dev) and Snowflake (prod).
    Like a bilingual signpost at a station: same meaning, different language per country.
    Each macro uses adapter.dispatch, so dbt picks the right version for the current warehouse.
#}


{# ---- Normalise ANY timestamp type to one plain UTC timestamp ------------------------------ #}
{# dlt can create TIMESTAMP_TZ in one table and TIMESTAMP_LTZ in another on Snowflake, and those cannot be #}
{# stacked with UNION. Every raw timestamp passes through this macro first, so all branches have ONE type: #}
{# a plain UTC clock time (TIMESTAMP_NTZ on Snowflake, TIMESTAMP on DuckDB). Input must carry a time zone. #}
{% macro as_utc_timestamp(column) -%}
    {{ return(adapter.dispatch('as_utc_timestamp')(column)) }}
{%- endmacro %}

{% macro duckdb__as_utc_timestamp(column) -%}
    timezone('UTC', {{ column }})
{%- endmacro %}

{% macro snowflake__as_utc_timestamp(column) -%}
    convert_timezone('UTC', {{ column }})::timestamp_ntz
{%- endmacro %}


{# ---- Plain UTC timestamp (from as_utc_timestamp) to Europe/Berlin wall-clock time, CET/CEST aware ---- #}
{% macro to_berlin_time(utc_column) -%}
    {{ return(adapter.dispatch('to_berlin_time')(utc_column)) }}
{%- endmacro %}

{% macro duckdb__to_berlin_time(utc_column) -%}
    timezone('Europe/Berlin', timezone('UTC', {{ utc_column }}))
{%- endmacro %}

{% macro snowflake__to_berlin_time(utc_column) -%}
    convert_timezone('UTC', 'Europe/Berlin', {{ utc_column }})::timestamp_ntz
{%- endmacro %}


{# ---- SHA-256 fingerprint (hex text) ------------------------------------------------------ #}
{% macro sha256_hex(expression) -%}
    {{ return(adapter.dispatch('sha256_hex')(expression)) }}
{%- endmacro %}

{% macro duckdb__sha256_hex(expression) -%}
    sha256({{ expression }})
{%- endmacro %}

{% macro snowflake__sha256_hex(expression) -%}
    sha2({{ expression }}, 256)
{%- endmacro %}


{# ---- Salted hash for IDs that must never reach PROD in readable form ---------------------- #}
{# Like a library card number: you can match two records by it, but you cannot read a name from it. #}
{% macro hash_pii(column) -%}
    case
        when {{ column }} is null then null
        else {{ sha256_hex("'" ~ var('hash_salt') ~ "' || '|' || cast(" ~ column ~ " as varchar)") }}
    end
{%- endmacro %}


{# ---- Point-in-polygon: is this GPS point inside this GeoJSON district shape? ------------- #}
{% macro point_in_polygon(geojson_column, longitude, latitude) -%}
    {{ return(adapter.dispatch('point_in_polygon')(geojson_column, longitude, latitude)) }}
{%- endmacro %}

{% macro duckdb__point_in_polygon(geojson_column, longitude, latitude) -%}
    st_contains(st_geomfromgeojson({{ geojson_column }}), st_point({{ longitude }}, {{ latitude }}))
{%- endmacro %}

{% macro snowflake__point_in_polygon(geojson_column, longitude, latitude) -%}
    st_contains(to_geography({{ geojson_column }}), st_makepoint({{ longitude }}, {{ latitude }}))
{%- endmacro %}


{# ---- Date and time arithmetic ------------------------------------------------------------ #}
{% macro add_days(date_expression, days_expression) -%}
    {{ return(adapter.dispatch('add_days')(date_expression, days_expression)) }}
{%- endmacro %}

{% macro duckdb__add_days(date_expression, days_expression) -%}
    ({{ date_expression }} + cast({{ days_expression }} as integer))
{%- endmacro %}

{% macro snowflake__add_days(date_expression, days_expression) -%}
    dateadd(day, {{ days_expression }}, {{ date_expression }})
{%- endmacro %}


{% macro add_hours(timestamp_expression, hours_expression) -%}
    {{ return(adapter.dispatch('add_hours')(timestamp_expression, hours_expression)) }}
{%- endmacro %}

{% macro duckdb__add_hours(timestamp_expression, hours_expression) -%}
    ({{ timestamp_expression }} + to_hours(cast({{ hours_expression }} as integer)))
{%- endmacro %}

{% macro snowflake__add_hours(timestamp_expression, hours_expression) -%}
    dateadd(hour, {{ hours_expression }}, {{ timestamp_expression }})
{%- endmacro %}


{# ISO weekday number: Monday = 1 ... Sunday = 7 #}
{% macro iso_day_of_week(date_expression) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(date_expression)) }}
{%- endmacro %}

{% macro duckdb__iso_day_of_week(date_expression) -%}
    isodow({{ date_expression }})
{%- endmacro %}

{% macro snowflake__iso_day_of_week(date_expression) -%}
    dayofweekiso({{ date_expression }})
{%- endmacro %}


{# ---- Surrogate key: one stable text ID built from several columns ------------------------ #}
{# Like a locker number made from "building + floor + door": the same inputs always give the same ID. #}
{% macro surrogate_key(columns) -%}
    md5(concat_ws('|'
        {%- for column in columns -%}
        , coalesce(cast({{ column }} as varchar), '')
        {%- endfor -%}
    ))
{%- endmacro %}
