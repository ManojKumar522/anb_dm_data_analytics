{% macro dq_custom_rules() %}
{{
    return({
        'dq_rule_attribute_length_check': dq_rule_attribute_length_check,
        'dq_rule_attribute_within_range_check': dq_rule_attribute_within_range_check,
        'dq_rule_column_value_compare': dq_rule_column_value_compare,
        'dq_rule_conditional_null': dq_rule_conditional_null,
        'dq_rule_date_consistency': dq_rule_date_consistency,
        'dq_rule_not_null': dq_rule_not_null,
        'dq_rule_not_null_id_vs_desc': dq_rule_not_null_id_vs_desc,
        'dq_rule_referential_integrity': dq_rule_referential_integrity,
        'dq_rule_regex_check': dq_rule_regex_check,
        'dq_rule_scd2_change_in_attribute_value': dq_rule_scd2_change_in_attribute_value,
        'dq_rule_unexpected_values': dq_rule_unexpected_values,
        'dq_rule_unique': dq_rule_unique,
        'dq_rule_unique_typ2': dq_rule_unique_typ2,
        'dq_rule_value_comparision_check': dq_rule_value_comparision_check,
    })
}}
{% endmacro %}
