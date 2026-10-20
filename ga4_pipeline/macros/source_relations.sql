{% macro event_versions_relation() -%}
  {% if target.type == 'bigquery' -%}
    {{ source('ga4_landing', 'event_versions') }}
  {%- else -%}
    {{ ref('raw_ga4_events') }}
  {%- endif %}
{%- endmacro %}

{% macro item_versions_relation() -%}
  {% if target.type == 'bigquery' -%}
    {{ source('ga4_landing', 'item_versions') }}
  {%- else -%}
    {{ ref('raw_ga4_items') }}
  {%- endif %}
{%- endmacro %}

{% macro ga4_integer() -%}
  {%- if target.type == 'bigquery' -%}INT64{%- else -%}BIGINT{%- endif -%}
{%- endmacro %}
