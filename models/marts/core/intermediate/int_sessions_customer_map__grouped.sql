{{
    config(
        dataset = 'int',
        materialized = 'view'
    )
}}

-- ga4 sessions
with sessions as (

    select * from {{ ref('fct_sessions') }}

),

-- group sessions to a user identity graph (customer_id = unique)
final as (

    -- select all identifiers and group on customer-id.
    select
        customer_id[safe_offset(0)].id as customer_id,
        max(session_start_at) as last_session_at,
        {{ target.schema + var('dataset_udf') }}.dedup_array(array_agg(struct(session_start_at as timestamp, fpc_id as id))) as fpc_id,
        {{ target.schema + var('dataset_udf') }}.dedup_array(array_concat_agg(gclid)) as gclid,
        {{ target.schema + var('dataset_udf') }}.dedup_array(array_concat_agg(transaction_id)) as transaction_id
    from sessions
    where customer_id[safe_offset(0)].id is not null
    group by 1

)

select * from final