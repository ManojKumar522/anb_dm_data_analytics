{% set v_batch_id = m_get_batch_id(var('p_pipeline_name')) %}
{% set v_pre_hook = m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'gold_mcd', this, v_batch_id) %}
{% set v_inc_load_ts = m_get_inc_load_ts_for_model_record_from_dbt_model_audit(var('p_pipeline_name'), 'gold_mcd', this, v_batch_id) %}

{% set target_key_column_list = ["loan_nbr", "borrower_position_nbr"] %}
{% set target_context_typ2_column_list = [
    "brnd_nm", "first_nm", "last_nm", "credit_score_value_nbr", "address_line_1_txt", "city_nm", "postal_cd",
    "state_or_province_cd", "country_nm", "credit_score_dt", "cell_auth_cd", "taxpyr_id_type_nm", "taxpyr_id_val",
    "gndr_type_cd", "mari_sts_type_cd", "affil_cd", "orgnl_cr_scr", "dsbl_id", "cr_rpt", "prim_cntct", "scnd_cntct",
    "deceased_ind", "frst_prty_fraud_ind", "id_theft_victim_ind", "rqr_enhanced_auth_ind", "pwr_of_atrny_ind",
    "borr_pstn", "prev_cr_rpt", "extnl_sys_id", "prim_borr", "gen_cd", "cust_disc_flg"
] %}

{{ config(
    materialized = "incremental",
    strategy = "merge",
    unique_key = ["hash_sk", "hash_seq_num"],
    merge_exclude_columns = ["etl_load_ts"],
    post_hook = [
        "{{ m_upd_typ2_activ_records(this, 'hash_sk', 'hash_seq_num') }}",
        "{{ m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_pipeline_name'), 'gold_mcd', this, m_get_batch_id(var('p_pipeline_name')), 'load_dttm') }}"
    ]
) }}

with base as (
    select
        cast(a1.a1loan as varchar) as loan_nbr,
        l.lsbrand as brnd_nm,
        cast(a1.a1seq as integer) as borrower_position_nbr,
        a1.a1fnam as first_nm,
        a1.a1mnam as middle_nm,
        a1.a1lnam as last_nm,
        a1.a1suff as suffix_nm,
        try_to_date(lpad(cast(a1.a1bdte as varchar), 8, '0'), 'mmddyyyy') as borrower_birth_dt,
        c.asofdate as credit_score_dt,
        coalesce(c.ficoscore, 0) as credit_score_value_nbr,
        trim(replace(replace(a1.a1add1, '\r', ''), '\n', '')) as address_line_1_txt,
        trim(coalesce(a1.a1add2, '')) as address_line_2_txt,
        trim(coalesce(a1.a1city, '')) as city_nm,
        '' as country_nm, 
        trim(coalesce(a1.a1zip, '')) as postal_cd,
        trim(coalesce(a1.a1stat, '')) as state_or_province_cd,
        trim(coalesce(a1.a1mcty, '')) as county_cd,
        trim(replace(replace(a1.a1sad1, '\r', ''), '\n', '')) as seasonal_address_line_1_txt,
        trim(coalesce(a1.a1sad2, '')) as seasonal_address_line_2_txt,
        trim(coalesce(a1.a1scit, '')) as seasonal_city_nm,
        trim(coalesce(a1.a1szip, '')) as seasonal_postal_cd,
        trim(coalesce(a1.a1sst, '')) as seasonal_state_or_province_cd,
        trim(coalesce(a1.a1scty, '')) as seasonal_county_cd,
        a1.lbebilfgb as ecorres_cd,
        a1.lbccontact as cell_auth_cd,
        'socialsecuritynumber' as taxpyr_id_type_nm,
        case
            when ltrim(rtrim(a1.a1soc)) = '0' or length(ltrim(rtrim(a1.a1soc))) >= 9
                then ltrim(rtrim(a1.a1soc))
            else right('000000000' || ltrim(rtrim(a1.a1soc)), 9)
        end as taxpyr_id_val,
        a1.a1gen as gndr_type_cd,
        a1.a1mast as mari_sts_type_cd,
        a1.a1afcd as affil_cd,
        a1.a1lngi as lng_ind,
        a1.a1fico as orgnl_cr_scr,
        'etl-insert' as ods_create_by_nm,
        current_timestamp() as ods_create_dttm,
        'etl-update' as ods_update_by_nm,
        current_timestamp() as ods_update_dttm,
        null as truncate_flag,
        null as dsbl_id,
        null as cr_rpt,
        s2.a2prct as prim_cntct,
        s2.a2scct as scnd_cntct,
        case
            when a1.a1afcd in ('06', '07') then 'y'
            else 'n'
        end as deceased_ind,
        max(case when col.collection_code = 'usfr' then 'y' else 'n' end) as frst_prty_fraud_ind,
        max(case when col.collection_code = 'usid' then 'y' else 'n' end) as id_theft_victim_ind,
        max(case when col.collection_code = 'usra' then 'y' else 'n' end) as rqr_enhanced_auth_ind,
        max(case when col.collection_code = '3prt' then 'y' else 'n' end) as pwr_of_atrny_ind,
        case when a1.a1seq = 1 then 'p' else 'c' end as borr_pstn,
        bo.lbprvcrd as prev_cr_rpt,
        bo.lbexternid as extnl_sys_id,
        coalesce(bo.lbpribr, 0) as prim_borr,
        t.j2gencd as gen_cd,
        v.custdscflg as cust_disc_flg,
        a1.load_dttm,
        a1.load_dttm as src_load_ts
    from {{ ref('borrower_srvalt1_t') }} a1
    left join {{ ref('borrower_lsloan00_t') }} l
        on a1.a1loan = l.lsln_n
    left join {{ ref('borrower_creditdataacc_t') }} c
        on a1.a1loan = c.loan_number
    left join {{ ref('borrower_lsborr00_t') }} bo
        on a1.a1loan = bo.lbln_n and a1.a1seq = bo.lbbor_n
    left join {{ ref('borrower_srvtrw2j2_t') }} t
        on bo.lbln_n = t.j2loan and bo.lbbor_n = t.j2seqno
    left join {{ ref('borrower_srvalt2_t') }} s2
        on a1.a1loan = s2.a2loan and a1.a1seq = s2.a2seq
    left join {{ ref('borrower_srvcoli_t') }} col
        on col.loan_nbr_num = a1.a1loan
    left join {{ ref('borrower_loanvehbor_t') }} v
        on a1.a1seq = v.borrseq and a1.a1loan = v.loan
    where a1.a1seq <> 9
        and (
            a1.load_dttm > {{ v_inc_load_ts }}
            or c.load_dttm > {{ v_inc_load_ts }}
            or bo.load_dttm > {{ v_inc_load_ts }}
            or s2.load_dttm > {{ v_inc_load_ts }}
            or col.load_dttm > {{ v_inc_load_ts }}
        )
    group by all
)

, with_hash as (
    select
        *,
        {{ m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
    from base src
)

select
    src.*,
    src.src_load_ts::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::string as active_flg,
    'Y'::string as reporting_flg,
    {{ v_batch_id }} as batch_id,
    current_timestamp() as etl_load_ts,
    current_timestamp() as etl_updt_ts
from with_hash src
