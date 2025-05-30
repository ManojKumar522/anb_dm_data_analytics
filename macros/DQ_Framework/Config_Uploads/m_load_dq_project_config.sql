{% macro m_load_dq_project_config(p_src_name) %}
    {% if execute %}
        {% for src in graph.sources.values() %}
            {% if src.source_name == p_src_name %}
                {% do log("MATCH FOUND: " ~ src.source_name, info=True) %} 

                {% if src.meta is not none and 'project' in src.meta %}
                    {% set project_meta = src.meta['project'] %}
                    {% do log("Extracted project meta: " ~ project_meta | tojson, info=True) %}

                    {% if project_meta | length > 0 %}
                        {% set json_str = project_meta | tojson | replace("'", '"') %}
                        {% set merge_sql %}
                            merge into {{ source('dq_config', 'dq_project_t') }} as target
                            using (
                                select
                                    value:project_id::string as project_id,
                                    value:project_name::string as project_name,
                                    value:owner::string as owner,
                                    value:active_flg::string as active_flg,
                                    current_timestamp() as created_ts,
                                    current_timestamp() as modified_ts
                                from lateral flatten(input => parse_json('{{ json_str }}'))
                            ) as source
                            on target.project_id = source.project_id
                            when matched then update set
                                target.project_name = source.project_name,
                                target.owner = source.owner,
                                target.active_flg = source.active_flg,
                                target.modified_ts = source.modified_ts
                            when not matched then insert (
                                project_id, project_name, owner, active_flg, created_ts, modified_ts
                            ) values (
                                source.project_id, source.project_name, source.owner, source.active_flg, source.created_ts, source.modified_ts
                            );
                        {% endset %}
                        {% do log("Running MERGE SQL for source: " ~ src.source_name, info=True) %}
                        {% call statement('merge_project_meta', fetch_result=False) %}
                            {{ merge_sql }}
                        {% endcall %}
                    {% else %}
                        {% do log("Project meta found but empty in source: " ~ src.source_name, info=True) %}
                    {% endif %}
                {% else %}
                    {% do log("No Project found in meta for source: " ~ src.source_name, info=True) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
