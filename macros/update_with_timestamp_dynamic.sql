{% macro update_with_timestamp_dynamic(column_name, database=target.database, schema=target.schema, dry_run=True) %}
  
  {% set get_tables_query %}
    SELECT
        table_name
    FROM {{ database }}.information_schema.columns
    WHERE table_schema = upper('{{ schema }}')
      AND column_name = upper('{{ column_name }}')
      AND data_type = 'TIMESTAMP_NTZ'
  {% endset %}

  {{ log('\nGetting a list of tables to update...', info=true) }}
  {% set tables_to_update = run_query(get_tables_query).columns[0].values() %}

  {% for table in tables_to_update %}
    {% set sql_update %}
      -- Zmieniono ref(table) na pełną ścieżkę do tabeli
      UPDATE {{ database }}.{{ schema }}.{{ table }}
      SET {{ column_name }} = CURRENT_TIMESTAMP();
    {% endset %}
    
    {% if dry_run %}
        {{ log('Dry run - The following query would be executed: ' ~ sql_update, info=true) }}
    {% else %}
        {{ log('Updating table: ' ~ table, info=true) }}
        {% do run_query(sql_update) %}
    {% endif %}
  {% endfor %}
  
  {{ log('\nCheck the logs above for the results of the dry run.', info=true) }}

{% endmacro %}


--dbt run-operation update_with_timestamp_dynamic --args '{column_name: "_batched_at", database: "raw", schema: "stripe", dry_run: false}'