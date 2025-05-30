{% macro dq_rule_not_null(param_dict, batch_id, info=raw) %}
    {{ log("Starting dq rule not null for table, info=" ~ info, info=true) }}
    {% set v_table_name = param_dict["table_name"] %}
    {% set v_column_name = param_dict["column_name"] %}

    {% call statement('run_not_null_query', fetch_result=True) %}
        with src as (
            select count(1) as total_record_count, 0 as failure_record_count
            from {{ v_table_name }}
            where batch_id = {{ batch_id }}
        ),
        failure as (
            select 0 as total_record_count, count(1) as failure_record_count
            from {{ v_table_name }}
            where {{ v_column_name }} is null and batch_id = {{ batch_id }}
        )
        select a.total_record_count, b.failure_record_count
        from src a
        join failure b on 1=1
    {% endcall %}

    {% set result = load_result('run_not_null_query') %}
    {% if result['data'] and result['data'][0] %}
        {% set total_record_count = result['data'][0][0] %}
        {% set failure_record_count = result['data'][0][1] %}
        {% set failure_ratio = (failure_record_count / total_record_count * 100) if total_record_count > 0 else 0 %}
        {% set result_status = "Fail" if failure_record_count > 0 else "Success" %}
        {% set rule_execution_ts = modules.datetime.datetime.now().isoformat() %}
        
        {% do return({
            "rule_type": "Null & Empty Check",
            "table_name": v_table_name,
            "column_name": v_column_name,
            "rule_execution_ts": rule_execution_ts,
            "total_record_count": total_record_count,
            "failure_record_count": failure_record_count,
            "failure_ratio": failure_ratio,
            "result_status": result_status
        }) %}
    {% else %}
        {{ log("Query execution failed", info=raw) }}
        {% do return({}) %}
    {% endif %}
{% endmacro %}
