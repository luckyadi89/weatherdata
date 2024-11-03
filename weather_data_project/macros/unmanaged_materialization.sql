{% materialization unmanaged, adapter='snowflake' %}

{%- set display_log = var('debug', default=false) -%}
{%- set re = modules.re -%}
{%- set on_schema_change = 'sync_all_columns' -%}
{%- set keep_backup_flag = config.get('keep_backup_flag', default=false) -%} -- indicate if the backup copy of previous version is to be retained
{%- set column_sync_flag = config.get('column_sync_flag', default=false) -%} -- indicate if the column has changed then add or delete columns
{%- set backup_table_flag = config.get('backup_table_flag', default=false) -%}
{%- set is_transient = config.get('is_transient', default=false) -%}
{%- set unique_key = config.get('unique_key') -%}
{%- set update_unsupported_dtype_changes = config.get('update_unsupported_dtype_changes', default=false) -%}
{%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
{%- set enable_automatic_clustering = config.get('automatic_clustering', default=true) -%}
{%- set identifier = model['alias'] -%}
{%- set full_refresh_mode = (should_full_refresh()) -%}


{%- set current_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
{%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}
{%- set tmp_relation = make_temp_relation(target_relation) -%}

{{ adapter.drop_relation(tmp_relation) }}

-- transient table setting
{% if is_transient is true %}
    {% set new_sql = re.sub('(?i)CREATE\\s+TABLE', 'CREATE TRANSIENT TABLE', sql) %}
{% else %}
    {% set new_sql = sql %}
{% endif %}
-- setup
{{ run_hooks(pre_hooks, inside_transaction=False) }}

-- 'BEGIN' happens here:
{{ run_hooks(pre_hooks, inside_transaction=True) }}

{% if backup_table_flag is true %}
    -- backup the existing table just in case
    {%- set backup_suffix_dt = PY_current_timestamp() -%}
    {%- set backup_table_suffix = config.get('backup_table_suffix', default='_DBT_BACKUP_') -%}
    {%- set backup_identifier = model['name'] ~ backup_table_suffix ~ backup_suffix_dt -%}

    {%- set backup_relation = api.Relation.create(database=database,
                                                  schema=schema,
                                                  identifier=backup_identifier,
                                                  type='table') -%}
    {{ clone_table_relation_if_exists(current_relation, backup_relation) }}
{% endif %}

{% if current_relation is none %}
    {%- call statement('main') -%}
        {{ create_table_stmt_fromfile(target_relation, new_sql) }}
    {%- endcall -%}
{% else %}
    -- Execute with full-refresh mode
    {% if (full_refresh_mode) %}
        -- Drop current table
        {% do adapter.drop_relation(current_relation) %}
        -- Execute new statement
        {%- call statement('main') -%}
            {{ create_table_stmt_fromfile_with_sql(target_relation, new_sql) }}
        {%- endcall -%}
    {% else %}
        {% call statement('main') -%}
            {% set tmpsql = new_sql.replace(identifier, tmp_relation.identifier) %}
            {{ create_table_stmt_fromfile_with_sql(target_relation, tmpsql) }}
        {% endcall -%}
        -- Processing to sync on table between source and target --
        {% set dest_columns = sync_on_table(tmp_relation, current_relation, on_schema_change, update_unsupported_dtype_changes) %}
    {% endif %}
{% endif %}

{{ adapter.drop_relation(tmp_relation) }}

{% if keep_backup_flag is false and backup_table_flag is true %}
    {{ adapter.drop_relation(backup_relation) }}
{% endif %}

{{ run_hooks(post_hooks, inside_transaction=True) }}

-- 'COMMIT' happens here
{{ adapter.commit() }}

-- processing clustering end of process
{{ process_clustering(identifier, cluster_by_keys, enable_automatic_clustering) }}

{{ run_hooks(post_hooks, inside_transaction=False) }}

{{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
