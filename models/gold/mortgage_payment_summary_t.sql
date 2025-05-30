{% set v_batch_id = m_get_batch_id(var('p_pipeline_name')) %}
{% set v_pre_hook = m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'gold_mcd', this, v_batch_id) %}
{% set v_inc_load_ts = m_get_inc_load_ts_for_model_record_from_dbt_model_audit(var('p_pipeline_name'), 'gold_mcd', this, v_batch_id) %}

{% set target_key_column_list = ["application_id"] %}
{% set target_context_typ2_column_list = [
    "total_payments", "latest_payment_date", "last_status",
    "customer_id", "application_date", "amount_requested", "property_type", "application_status"
] %}

{{ config(
    materialized = "incremental",
    strategy = "merge",
    unique_key = ["hash_sk", "hash_seq_num"],
    merge_exclude_columns = ["etl_load_ts"],
    post_hook = [
        "{{ m_upd_typ2_activ_records(this, 'hash_sk', 'hash_seq_num') }}",
        "{{ m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'gold_mcd', this, m_get_batch_id(var('p_pipeline_name')), 'src_load_ts') }}"
    ]
) }}

WITH payment_agg AS (
    SELECT
        p.application_id,
        SUM(p.payment_amount) AS total_payments,
        MAX(p.payment_date) AS latest_payment_date,
        COUNT(*) AS num_payments,
        MAX(p.payment_status) AS last_status,
        MAX(p.src_load_ts) AS src_load_ts
    FROM {{ ref('mortgage_payment_t') }} p
    {% if is_incremental() %}
      WHERE p.src_load_ts > {{ v_inc_load_ts }}
    {% endif %}
    GROUP BY p.application_id
),

app_attrs AS (
    SELECT
        a.application_id,
        a.customer_id,
        a.application_date,
        a.amount_requested,
        a.property_type,
        a.status AS application_status
    FROM {{ ref('mortgage_application_t') }} a
),

final_gold AS (
    SELECT
        pa.application_id,
        pa.total_payments,
        pa.latest_payment_date,
        pa.num_payments,
        pa.last_status,
        att.customer_id,
        att.application_date,
        att.amount_requested,
        att.property_type,
        att.application_status,
        pa.src_load_ts
    FROM payment_agg pa
    LEFT JOIN app_attrs att
      ON pa.application_id = att.application_id
),

payment_with_hash AS (
    SELECT
        src.*,
        {{ m_prep_hash_key_from_column_list(target_key_column_list) }} AS hash_sk,
        {{ m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} AS hash_seq_num
    FROM final_gold src
)

SELECT
    src.*,
    src_load_ts::timestamp_ntz AS start_ts,
    NULL::timestamp_ntz AS end_ts,
    'Y'::string AS active_flg,
    'Y'::string AS reporting_flg,
    {{ v_batch_id }} AS batch_id,
    current_timestamp() AS etl_load_ts,
    current_timestamp() AS etl_updt_ts
FROM payment_with_hash src
