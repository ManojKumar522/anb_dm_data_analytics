{% set v_batch_id = m_get_batch_id(var('p_pipeline_name')) %}
{% set v_pre_hook = m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, v_batch_id) %}
{% set v_inc_load_ts = m_get_inc_load_ts_for_model_record_from_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, v_batch_id) %}

{{
    config(
        materialized = "incremental",
        strategy = "merge",
        unique_key = ["loan_nbr", "cross_ref_id"],
        merge_exclude_columns = ["etl_load_ts"],
        post_hook = [
            "{{ m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, m_get_batch_id(var('p_pipeline_name')), 'src_load_ts') }}"
        ]
    )
}}

select
    loan_nbr,
    cross_ref_id,
    src_load_ts,
    {{ v_batch_id }} as batch_id,
    current_timestamp() as etl_load_ts,
    current_timestamp() as etl_updt_ts
from {{ source('borrower_src', 'loan_nbr_cross_ref') }} src
{% if is_incremental() %}
    where src.src_load_ts > {{ v_inc_load_ts }}
{% endif %}