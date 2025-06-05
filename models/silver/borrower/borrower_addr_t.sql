{% set v_batch_id = m_get_batch_id(var('p_pipeline_name')) %}
{% set v_pre_hook = m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, v_batch_id) %}
{% set v_inc_load_ts = m_get_inc_load_ts_for_model_record_from_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, v_batch_id) %}

{{
    config(
        materialized = "incremental",
        strategy = "merge",
        unique_key = ["addr_id"],
        merge_exclude_columns = ["etl_load_ts"],
        post_hook = [
            "{{ m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'silver_mcd', this, m_get_batch_id(var('p_pipeline_name')), 'last_modified') }}"
        ]
    )
}}

select
    addr_id,
    addr_line_1_txt,
    addr_line_2_txt,
    city_nm,
    pstl_cd,
    state_or_prov_cd,
    cntry_cd,
    last_modified,
	last_modified as src_load_ts,
    {{ v_batch_id }} as batch_id,
    current_timestamp() as etl_load_ts,
    current_timestamp() as etl_updt_ts
from {{ source('borrower_src', 'addr') }} src
{% if is_incremental() %}
    where src.last_modified > {{ v_inc_load_ts }}
{% endif %}