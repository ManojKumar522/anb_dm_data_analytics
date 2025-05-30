{% macro m_load_dq_rule_config(p_src_name) %}
    {% if execute %}
        {% for src in graph.sources.values() %}
            {% if src.source_name == p_src_name %}
                {% do log("MATCH FOUND: " ~ src.source_name, info=True) %} 

                {% if src.meta is not none and 'rules' in src.meta %}
                    {% set rule_meta = src.meta['rules'] %}
                    {% do log("Extracted rules meta: " ~ rule_meta | tojson, info=True) %}

                    {% if rule_meta | length > 0 %}
                        {% set json_str = rule_meta | tojson | replace("'", '"') %}
                        {% set merge_sql %}
                            merge into {{ source('dq_config', 'dq_rule_t') }} as target
                            using (
                                select
                                    value:rule_id::string as rule_id,
                                    value:rule_name::string as rule_name,
                                    value:rule_description::string as rule_description,
                                    value:rule_type::string as rule_type,
                                    value:dbt_test::string as dbt_test,
                                    value:param_attrib::string as param_attrib,
                                    value:expected_value::string as expected_value,
                                    current_timestamp() as created_ts,
                                    current_timestamp() as modified_ts
                                from lateral flatten(input => parse_json('{{ json_str }}'))
                            ) as source
                            on target.rule_id = source.rule_id
                            when matched then update set
                                target.rule_name = source.rule_name,
                                target.rule_description = source.rule_description,
                                target.rule_type = source.rule_type,
                                target.dbt_test = source.dbt_test,
                                target.param_attrib = source.param_attrib,
                                target.expected_value = source.expected_value,
                                target.modified_ts = source.modified_ts
                            when not matched then insert (
                                rule_id, rule_name, rule_description, rule_type, dbt_test, param_attrib, expected_value, created_ts, modified_ts
                            ) values (
                                source.rule_id, source.rule_name, source.rule_description, source.rule_type, source.dbt_test, source.param_attrib, source.expected_value, source.created_ts, source.modified_ts
                            );
                        {% endset %}
                        {% do log("Running MERGE SQL for rules from source: " ~ src.source_name, info=True) %}
                        {% call statement('merge_rule_meta', fetch_result=False) %}
                            {{ merge_sql }}
                        {% endcall %}
                    {% else %}
                        {% do log("Rules meta found but empty in source: " ~ src.source_name, info=True) %}
                    {% endif %}
                {% else %}
                    {% do log("No Rules key found in meta for source: " ~ src.source_name, info=True) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
