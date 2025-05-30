{% macro m_load_dq_group_config(p_src_name) %}
    {% if execute %}
        {% for src in graph.sources.values() %}
            {% if src.source_name == p_src_name %}
                {% do log("MATCH FOUND: " ~ src.source_name, info=True) %} 

                {% if src.meta is not none and 'group' in src.meta %}
                    {% set group_meta = src.meta['group'] %}
                    {% do log("Extracted group meta: " ~ group_meta | tojson, info=True) %}

                    {% if group_meta | length > 0 %}
                        {% set json_str = group_meta | tojson | replace("'", '"') %}
                        {% set merge_sql %}
                            merge into {{ source('dq_config', 'dq_group_t') }} as target
                            using (
                                select
                                    value:group_id::string as group_id,
                                    value:group_name::string as group_name,
                                    value:project_id::string as project_id,
                                    current_timestamp() as created_ts,
                                    current_timestamp() as modified_ts
                                from lateral flatten(input => parse_json('{{ json_str }}'))
                            ) as source
                            on target.group_id = source.group_id
                            when matched then update set
                                target.group_name = source.group_name,
                                target.project_id = source.project_id,
                                target.modified_ts = source.modified_ts
                            when not matched then insert (
                                group_id, group_name, project_id, created_ts, modified_ts
                            ) values (
                                source.group_id, source.group_name, source.project_id, source.created_ts, source.modified_ts
                            );
                        {% endset %}
                        {% do log("Running MERGE SQL for group meta from source: " ~ src.source_name, info=True) %}
                        {% call statement('merge_group_meta', fetch_result=False) %}
                            {{ merge_sql }}
                        {% endcall %}
                    {% else %}
                        {% do log("Group meta found but empty in source: " ~ src.source_name, info=True) %}
                    {% endif %}
                {% else %}
                    {% do log("No Group key found in meta for source: " ~ src.source_name, info=True) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
