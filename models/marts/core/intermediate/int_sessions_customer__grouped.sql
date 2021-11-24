{{
    config(
        dataset = 'int',
        materialized = 'view'
    )
}}

with customer_identity_graph as (

    select * from {{ ref('dim_customer_identity') }}

),

-- mapping between first-party-id (fpc_id) and customer_id
mapping_fpid_customer as (

    select
        fpc_id,
        (select x.customer_id from unnest(customer_ids) x order by x.timestamp desc limit 1) as customer_id
    from (
        select
            fpc_id.id as fpc_id,
            count(customer_id) as customer_ids_count,
            array_agg(struct(customer_id, fpc_id.timestamp)) as customer_ids,
        from customer_identity_graph, 
            unnest(fpc_id) as fpc_id
        group by 
            fpc_id
    ) 
    where
        customer_ids_count <= 3

),

-- join customer_id with sessions on first-party-id (fpc-id)
sessions_with_customer_id as (

    select 
        c.customer_id,
        s.* except(customer_id)
    from {{ ref('fct_sessions') }} as s
    left join mapping_fpid_customer as c
        on s.fpc_id = c.fpc_id

),

-- group sessions to users (customers)
final as (

    select
        * except (item_interactions),
        array_to_string(
            array(
                select 
                    x.item_id 
                from unnest(item_interactions) x 
                where x.event_name = "view_item" 
                group by x.item_id 
                order by max(x.event_timestamp) desc limit 5),
            "|"
        ) as last_viewed_items,
        array_to_string(
            array(
                select 
                    x.item_id 
                from unnest(item_interactions) x 
                where x.event_name = "view_item" 
                group by x.item_id order by count(1) desc limit 5),
            "|"
        ) as most_viewed_items,
        array_to_string(
            array(
                select 
                    x.item_category 
                from unnest(item_interactions) x 
                where x.event_name = "view_item" 
                group by x.item_category 
                order by count(1) desc limit 5),
            "|"
        ) as most_viewed_categories
    from (
        select
            customer_id,
            min(datetime(user_first_touch_at)) as user_first_touch_at,
            min(datetime(session_start_at)) as first_session_at,
            max(datetime(session_start_at)) as last_session_at,
            max(datetime(if(transactions > 0, session_start_at, null))) as last_transaction_at,
            count(session_id) as sessions,
            countif(date_diff(current_date, datetime(session_start_at), day) between 0 and 7) as sessions_1d_7d,
            countif(date_diff(current_date, datetime(session_start_at), day) between 8 and 30) as sessions_7d_30d,
            countif(date_diff(current_date, datetime(session_start_at), day) between 31 and 90) as sessions_30d_90d,
            countif(session_engaged = 1) as sessions_engaged,
            safe_divide(sum(session_engagement_time), countif(session_engaged = 1)) as engagement_time_avg,
            max(newsletter_subscription) as newsletter_subscription,
            max(if(newsletter_subscription = 1, session_start_at, null)) as newsletter_subscription_at,
            sum(transactions) as total_transactions,
            ifnull(sum(transaction_value), 0) as total_transaction_value,
            count(distinct device) as devices,
            count(distinct session_channel_grouping) as channels,
            date_diff(current_date(), cast((max(session_start_at)) as date), day) as days_since_last_session,
            date_diff(current_date(),cast((max(if(transactions > 0, session_start_at, null))) as date),day) as days_since_last_transaction,
            string_agg(device order by session_start_at asc limit 1) as device_first,
            string_agg(device order by session_start_at desc limit 1) as device_last,
            {{ target.schema + var('dataset_udf') }}.most_freq_value(array_agg(device)) as device_most_freq,
            string_agg(os order by session_start_at desc limit 1) as os_last,
            {{ target.schema + var('dataset_udf') }}.most_freq_value(array_agg(os)) as os_most_freq,
            string_agg(browser order by session_start_at desc limit 1) as browser_last,
            {{ target.schema + var('dataset_udf') }}.most_freq_value(array_agg(browser)) as browser_most_freq,
            max(user_source) as user_source,
            max(user_medium) as user_medium,
            max(user_campaign) as user_campaign,
            max(user_channel_grouping) as user_channel_grouping,
            string_agg(session_channel_grouping order by session_start_at asc limit 1) as channel_grouping_first,
            string_agg(session_channel_grouping order by session_start_at desc limit 1) as channel_grouping_last,
            --{{ target.schema + var('dataset_udf') }}.most_freq_value(array_agg(session_channel_grouping ignore nulls)) as channel_most_freq,
            array_concat_agg(item_interactions limit 200) as item_interactions
        from sessions_with_customer_id
        where
            customer_id is not null
        group by 1
    )
)

select * from final