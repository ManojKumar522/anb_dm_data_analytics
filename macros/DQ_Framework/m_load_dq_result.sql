{% macro m_load_dq_result(v_project_name, v_batch_id) %}
    {{ log("Starting m_fetch_dq_rule_apply macro", info=true) }}
    {{ log("Project Name : " ~ v_project_name, info=true) }}
    {{ log("Batch Id: " ~ v_batch_id, info=true) }}

    {% set custom_rule_mapping = dq_custom_rules() %}

    {% call statement('fetch_rule_exp', fetch_result=true) %}
    with
    rule_expressions as (
        select
            exp_id,
            group_id,
            param_value,
            exp_message,
            severity_level,
            schema_name,
            tbl_name,
            full_tbl_name,
            col_name,
            exp_dt,
            source_system,
            escalation_poc,
            active_flg
        from {{ source('dq_config', 'dq_rule_exp_t') }}
    ),
    project_details as (
        select
            project_id,
            project_name
        from {{ source('dq_config', 'dq_project_t') }}
    ),
    group_details as (
        select
            group_id,
            group_name,
            project_id
        from {{ source('dq_config', 'dq_group_t') }}
    ),
    rule_details as (
        select
            rule_id,
            rule_name,
            rule_type,
            dbt_test
        from {{ source('dq_config', 'dq_rule_t') }}
    )
    select
        proj.project_id, --[0]
        proj.project_name, --[1]
        grp.group_id, --[2]
        grp.group_name, --[3]
        rule.rule_id, --[4]
        rule.rule_name, --[5]
        rule.rule_type, --[6]
        rule.dbt_test, --[7]
        exp.exp_id, --[8]
        exp.exp_message, --[9]
        exp.param_value, --[10]
        exp.schema_name, --[11]
        exp.tbl_name, --[12]
        exp.full_tbl_name, --[13]
        exp.col_name, --[14]
        exp.exp_dt, --[15]
        exp.severity_level, --[16]
        exp.source_system, --[17]
        exp.escalation_poc, --[18]
        exp.active_flg --[19]
    from rule_expressions exp
        join rule_details rule on exp.exp_id = rule.rule_id
        join group_details grp on exp.group_id = grp.group_id
        join project_details proj on grp.project_id = proj.project_id
    where proj.project_name = '{{ v_project_name }}'
    and exp.active_flg = 'Y'
    {% endcall %}

    {% if execute %}
        {% set rule_exp_and_rules_result = load_result('fetch_rule_exp') %}
        {% if rule_exp_and_rules_result['data'] is none or rule_exp_and_rules_result['data']|length == 0 %}
            {{ log("No data returned from dq_rule_apply or query execution failed.", info=true) }}
        {% else %}
            {% for row in rule_exp_and_rules_result['data'] %}
                {% set param_value = fromjson(row[10]) %}
                {{ log("parsed param_value: " ~ param_value, info=true) }}

                {% set test_function_name = row[7] %}
                {% set test_function = custom_rule_mapping.get(test_function_name) %}

                {% if test_function is none %}
                    {{ log("No test function found for dbt_test : " ~ test_function_name, info=true) }}
                {% else %}
                    {% set key_column_str = param_value.get('key_column') | replace("'", "") | replace("[", "") | replace("]", "") %}

                    delete from {{ source('dq_audit', 'dq_result_t') }}
                    where project_id = '{{ row[0] }}'
                      and group_id = '{{ row[2] }}'
                      and rule_id = '{{ row[4] }}'
                      and exp_id = '{{ row[8] }}'
                      and batch_id = '{{ v_batch_id }}';

                    insert into {{ source('dq_audit', 'dq_result_t') }} (
                        project_id, project_name, group_id, group_name, rule_id, rule_name, rule_type, dbt_test, exp_id, exp_message,
                        dq_rule_schema_name, tbl_name, full_tbl_name, key_column, col_name, rule_execution_ts, result_status,
                        failure_record_count, total_record_count, failure_ratio, failure_record_query, severity_level, source_system,
                        escalation_poc, batch_id, etl_load_ts, etl_updt_ts
                    )
                    select
                        '{{ row[0] }}', -- project_id
                        '{{ row[1] }}', -- project_name
                        '{{ row[2] }}', -- group_id
                        '{{ row[3] }}', -- group_name
                        '{{ row[4] }}', -- rule_id
                        '{{ row[5] }}', -- rule_name
                        '{{ row[6] }}', -- rule_type
                        '{{ row[7] }}', -- dbt_test
                        '{{ row[8] }}', -- exp_id
                        '{{ row[9] }}', -- exp_message
                        '{{ row[11] }}', -- dq_rule_schema_name
                        '{{ row[12] }}', -- tbl_name
                        '{{ row[13] }}', -- full_tbl_name
                        '{{ key_column_str }}', -- key_column
                        '{{ row[14] }}', -- col_name
                        {{ test_function(param_value, v_batch_id) }},
                        '{{ row[16] }}', -- severity_level
                        '{{ row[17] }}', -- source_system
                        '{{ row[18] }}', -- escalation_poc
                        '{{ v_batch_id }}',
                        current_timestamp(),
                        current_timestamp()
                    ;
                {% endif %}
            {% endfor %}
        {% endif %}
        {{ log("completed m_load_dq_result macro", info=true) }}
    {% endif %}
{% endmacro %}
