{% macro dq_rule_not_null_id_vs_desc(param_dict, batch_id) %}
    {{ log("Starting dq_rule_not_null_id_vs_desc", info=raw) }}
    {% set v_table_name = param_dict["table_name"] %}
    {% set v_id_column = param_dict["id_column"] %}
    {% set v_desc_column = param_dict["desc_column"] %}

    {% call statement('run_not_null_id_vs_desc_query', fetch_result=True) %}
        with src as (
            select count(1) as total_record_count, 0 as failure_record_count
            from {{ v_table_name }}
            where batch_id = {{ batch_id }}
        ),
        failure as (
            select 0 as total_record_count, count(1) as failure_record_count
            from {{ v_table_name }}
            where (({{ v_id_column }} is not null and {{ v_desc_column }} is null) or ({{ v_id_column }} is null and {{ v_desc_column }} is not null))
            and batch_id = {{ batch_id }}
        )
        select a.total_record_count, b.failure_record_count
        from src a
        join failure b on 1=1
    {% endcall %}

    {% if execute %}
        {% set result = load_result('run_not_null_id_vs_desc_query') %}
        {% if result['data'] and result['data']|length > 0 %}
            {% set total_record_count = result['data'][0][0] %}
            {% set failure_record_count = result['data'][0][1] %}
            {% set failure_ratio = (failure_record_count / total_record_count * 100) if total_record_count > 0 else 0 %}
            {% set result_status = "Fail" if failure_record_count > 0 else "Success" %}
            {% set rule_execution_ts = modules.datetime.datetime.now().isoformat() %}

            {% do return({
                "rule_type": "Not Null id vs desc",
                "table_name": v_table_name,
                "id_column": v_id_column,
                "desc_column": v_desc_column,
                "rule_execution_ts": rule_execution_ts,
                "total_record_count": total_record_count,
                "failure_record_count": failure_record_count,
                "failure_ratio": failure_ratio,
                "batch_id": batch_id,
                "result_status": result_status
            }) %}
        {% else %}
            {{ log("No issues found", info=raw) }}
            {% do return({}) %}
        {% endif %}
    {% else %}
        {{ log("Query execution failed", info=raw) }}
        {% do return(false) %}
    {% endif %}
{% endmacro %}
