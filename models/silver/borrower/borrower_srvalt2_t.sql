{% set v_batch_id = m_get_batch_id(var('p_pipeline_name')) %}
{% set v_pre_hook = m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, v_batch_id) %}
{% set v_inc_load_ts = m_get_inc_load_ts_for_model_record_from_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, v_batch_id) %}

{{
    config(
        materialized = "incremental",
        strategy = "merge",
        unique_key = ["a2loan", "a2seq"],
        merge_exclude_columns = ["etl_load_ts"],
        post_hook = [
            "{{ m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, m_get_batch_id(var('p_pipeline_name')), 'load_dttm') }}"
        ]
    )
}}

select
    a2loan,
    a2seq,
    a2prct,
    a2scct,
    load_dttm,
	load_dttm as src_load_ts,
    {{ v_batch_id }} as batch_id,
    current_timestamp() as etl_load_ts,
    current_timestamp() as etl_updt_ts
from {{ source('borrower_src', 'srvalt2') }} src
{% if is_incremental() %}
    where src.load_dttm > {{ v_inc_load_ts }}
{% endif %}