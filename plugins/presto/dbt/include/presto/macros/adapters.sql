
-- - get_catalog
-- - list_relations_without_caching
-- - get_columns_in_relation

{% macro presto_ilike(column, value) -%}
	regexp_like({{ column }}, '(?i)\A{{ value }}\Z')
{%- endmacro %}


{% macro presto__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from
      {{ information_schema_name(relation.database) }}.columns

      where {{ presto_ilike('table_name', relation.identifier) }}
        {% if relation.schema %}
        and {{ presto_ilike('table_schema', relation.schema) }}
        {% endif %}
        {% if relation.database %}
        and {{ presto_ilike('table_catalog', relation.database) }}
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}



{% macro presto__list_relations_without_caching(database, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ information_schema_name(database) }}.tables
    where {{ presto_ilike('table_schema', schema) }}
      and {{ presto_ilike('table_catalog', database) }}
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

