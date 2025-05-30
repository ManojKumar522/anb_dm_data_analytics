{% macro m_load_dq_rule_exp_config(p_src_name) %}
    {% if execute %}
        {% for src in graph.sources.values() %}
            {% if src.source_name == p_src_name %}
                {% do log("MATCH FOUND: " ~ src.source_name, info=True) %} 

                {% if src.meta is not none and 'expectations' in src.meta %}
                    {% set dq_rule_exp_dict = src.meta['expectations'] %}
                    {% do log("Extracted expectations: " ~ dq_rule_exp_dict | tojson, info=True) %}

                    {% if dq_rule_exp_dict | length > 0 %}
                        {% set dq_rule_exp_json = dq_rule_exp_dict | tojson | replace("'", '"') %}
                        {% set merge_sql %}
                            merge into {{ source('dq_config', 'dq_rule_exp_t') }} as target
                            using (
                                select
                                    value:exp_id::string as exp_id,
                                    value:group_id::string as group_id,
                                    value:exp_type::string as exp_type,
                                    value:exp_message::string as exp_message,
                                    value:exp_dt::string as exp_dt,
                                    value:severity_level::string as severity_level,
                                    value:source_system::string as source_system,
                                    value:escalation_poc::string as escalation_poc,
                                    value:active_flg::string as active_flg,
                                    value:schema_name::string as schema_name,
                                    value:tbl_name::string as tbl_name,
                                    value:full_tbl_name::string as full_tbl_name,
                                    value:col_name::string as col_name,
                                    value:param_value::string as param_value,
                                    current_timestamp() as created_ts,
                                    current_timestamp() as modified_ts
                                from lateral flatten(input => parse_json('{{ dq_rule_exp_json }}'))
                            ) as source
                            on target.exp_id = source.exp_id
                            when matched then update set
                                target.exp_type = source.exp_type,
                                target.exp_message = source.exp_message,
                                target.exp_dt = source.exp_dt,
                                target.severity_level = source.severity_level,
                                target.source_system = source.source_system,
                                target.escalation_poc = source.escalation_poc,
                                target.active_flg = source.active_flg,
                                target.schema_name = source.schema_name,
                                target.tbl_name = source.tbl_name,
                                target.full_tbl_name = source.full_tbl_name,
                                target.col_name = source.col_name,
                                target.param_value = source.param_value,
                                target.modified_ts = source.modified_ts
                            when not matched then insert (
                                exp_id, group_id, exp_type, exp_message, exp_dt, severity_level, source_system, escalation_poc, active_flg, schema_name, tbl_name, full_tbl_name, col_name, param_value, created_ts, modified_ts
                            ) values (
                                source.exp_id, source.group_id, source.exp_type, source.exp_message, source.exp_dt, source.severity_level, source.source_system, source.escalation_poc, source.active_flg, source.schema_name, source.tbl_name, source.full_tbl_name, source.col_name, source.param_value, source.created_ts, source.modified_ts
                            );
                        {% endset %}
                        {% do log("Running MERGE SQL for dq_rule_exp_t from source: " ~ src.source_name, info=True) %}
                        {% call statement('merge_dq_rule_exp', fetch_result=False) %}
                            {{ merge_sql }}
                        {% endcall %}
                    {% else %}
                        {% do log("Expectations found but empty in source: " ~ src.source_name, info=True) %}
                    {% endif %}
                {% else %}
                    {% do log("No expectations key found in meta for source: " ~ src.source_name, info=True) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
