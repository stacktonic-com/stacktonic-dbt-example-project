{{
    config(
        dataset = 'int',
        materialized = 'view'
    )
}}

-- sessions
with sessions as (

    select * from {{ ref('fct_sessions') }}

),

-- group sessions to day and channel
final as (

    -- select all identifiers and group on customer-id.
    select
        date(session_start_at) as date,
        session_source,
        session_medium,
        session_channel_grouping,
        count(distinct fpc_id) as users,
        count(*) as sessions,
        countif(session_engagement_time > 10) as sessions_engaged_10s,
        countif(session_engagement_time > 30) as sessions_engaged_30s,
        sum(newsletter_subscription) as newsletter_subscriptions,
        sum(transactions) as transactions,
        sum(transaction_value) as total_transaction_value
    from sessions
    group by 1,2,3,4
    order by sessions DESC

)

select * from final