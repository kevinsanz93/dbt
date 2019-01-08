{% macro postgres__get_relations () -%}
  {%- call statement('relations', fetch_result=True) -%}
    with relation as (
        select
            pg_rewrite.ev_class as class,
            pg_rewrite.oid as id
        from pg_rewrite
    ),
    class as (
        select
            oid as id,
            relname as name,
            relnamespace as schema,
            relkind as kind
        from pg_class
    ),
    dependency as (
        select
            pg_depend.objid as id,
            pg_depend.refobjid as ref
        from pg_depend
    ),
    schema as (
        select
            pg_namespace.oid as id,
            pg_namespace.nspname as name
        from pg_namespace
        where nspname != 'information_schema' and nspname not like 'pg_%'
    ),
    relationships as (
        select
            referenced_class.name as referenced_name,
            referenced_class.schema as referenced_schema_id,
            dependent_class.name as dependent_name,
            dependent_class.schema as dependent_schema_id,
            referenced_class.kind as kind
        from relation
        join class as referenced_class on relation.class=referenced_class.id
        join dependency on relation.id=dependency.id
        join class as dependent_class on dependency.ref=dependent_class.id
        where
            referenced_class.kind in ('r', 'v') and
            (referenced_class.name != dependent_class.name or
             referenced_class.schema != dependent_class.schema)
    )

    select
        referenced_schema.name as referenced_schema,
        relationships.referenced_name as referenced_name,
        dependent_schema.name as dependent_schema,
        relationships.dependent_name as dependent_name
    from relationships
    join schema as dependent_schema on relationships.dependent_schema_id=dependent_schema.id
    join schema as referenced_schema on relationships.referenced_schema_id=referenced_schema.id
    group by referenced_schema, referenced_name, dependent_schema, dependent_name
    order by referenced_schema, referenced_name, dependent_schema, dependent_name;
  {%- endcall -%}

  {{ return(load_result('relations').table) }}
{% endmacro %}
