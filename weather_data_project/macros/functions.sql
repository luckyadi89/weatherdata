{% macro create_table_stmt_fromfile_with_sql(target_relation, sql) %}
    {% if sql is not none %}
        {% set log_msg = "Creating table: " ~ target_relation.identifier %}
        {{ log(log_msg, info=True) }}
        {{ run_query(sql) }}
        {% if execute %}
            {% set confirmation_msg = "Table created: " ~ target_relation.identifier %}
            {{ log(confirmation_msg, info=True) }}
        {% endif %}
    {% else %}
        {% set error_msg = "SQL statement is None for creating table: " ~ target_relation.identifier %}
        {{ log(error_msg, error=True) }}
        {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}
{% endmacro %}


{% macro create_table_stmt_fromfile(target_relation, sql) %}
    {% if sql is not none %}
        {% set log_msg = "Preparing to create table: " ~ target_relation.identifier %} 
        {{ log(log_msg, info=True) }}
        
        -- Here you might add logic to modify or validate the SQL statement
        {% set sql_to_run = sql %}
        
        -- You might also log or modify the SQL based on any conditions or configurations
        {% if config.get('some_condition') %}
            {% set sql_to_run = sql_to_run ~ ' /* Additional SQL modification */' %}
        {% endif %}
        
        {% if execute %}
            {{ run_query(sql_to_run) }}
            {% set confirmation_msg = "Table created: " ~ target_relation.identifier %}
            {{ log(confirmation_msg, info=True) }}
        {% endif %}
    {% else %}
        {% set error_msg = "SQL statement is None for creating table: " ~ target_relation.identifier %}
        {{ log(error_msg, error=True) }}
        {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}
{% endmacro %}