{{
  config(
    dataset = 'int',
    materialized = 'incremental',
    partition_by = {'field': 'session_start_at', 'data_type': 'timestamp'},
    incremental_strategy = 'insert_overwrite',
    tags=['incremental', 'daily']
  )
}}

-- google analytics 4 events
with events as (
    
    select 
        * 
    from {{ ref('stg_google_analytics__events') }}

    
    {% if var('execution_date') != 'notset' %}
        
        where
            -- specific date set by variable (using the _table_suffix_ pseudo column for performance)
            table_suffix = '{{ var('execution_date') }}'

    {% elif is_incremental() %}

        where
            -- incremental data (using the _table_suffix_ pseudo column for performance)
            (table_suffix between format_date('%Y%m%d', date(_dbt_max_partition))
                and format_date('%Y%m%d', date_sub(current_date(), interval 1 day)))

    {% endif %}
),

-- prepare events to be grouped to sessiosn
events_sessionized as (

    select
        -- user identifiers
        fpc_id,
        session_id,
        if(user_id is not null,
            struct(timestamp_micros(event_timestamp) as timestamp, user_id as id), null
        ) as customer_id,
        if((select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'gclid') is not null,
            struct(timestamp_micros(event_timestamp) as timestamp, (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'gclid') as id), null
        ) as gclid,
        if(ecommerce.transaction_id is not null,
            struct(timestamp_micros(event_timestamp) as timestamp, ecommerce.transaction_id as id), null
        ) as transaction_id,
        -- prefilter session data
        timestamp_micros(event_timestamp) as event_timestamp,
        ifnull(safe_cast((select value.string_value from unnest(event_params) where key = 'session_engaged') as int64), 0) as session_engaged,
        safe_divide((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'), 1000) as engagement_time,
        lower(traffic_source.source) as user_source,
        lower(traffic_source.medium) as user_medium,
        lower(traffic_source.name) as user_campaign,
        timestamp_micros(user_first_touch_timestamp) as user_first_touch_at,
        device.category as device,
        lower(device.operating_system) as os,
        lower(device.web_info.browser) as browser,
        lower(geo.country) as country,
        lower(geo.city) as city,
        if(event_name in('page_view','user_engagement','scroll'), struct(
            event_timestamp,
            lower((select value.string_value from unnest(event_params) where key = 'source')) as source,
            lower((select value.string_value from unnest(event_params) where key = 'medium')) as medium,
            lower((select value.string_value from unnest(event_params) where key = 'campaign')) as campaign,
            (select value.int_value from unnest(event_params) where key = 'entrances') as is_entrance,
            (select value.int_value from unnest(event_params) where key = 'ignore_referrer') as ignore_referrer
        ), null) as session_channels,
        if(event_name = 'newsletter_subscribe', 1, 0) as newsletter_subscribe,
        if(event_name = 'purchase', 1, 0) as transaction,
        ecommerce.purchase_revenue as transaction_value,
        if(event_name = 'view_item' or event_name = 'add_to_cart', struct(
            event_timestamp,
            event_name,
            (select item_id from unnest(items) limit 1) as item_id,
            (select item_category from unnest(items) limit 1) as item_category
        ), null) as item_interaction
    from events
),

final as (
    select 
        fpc_id,
        session_id,
        {{ target.schema + var('dataset_udf') }}.dedup_array(
            array_agg(customer_id ignore nulls order by event_timestamp desc)
        ) as customer_id,
        {{ target.schema + var('dataset_udf') }}.dedup_array(
            array_agg(gclid ignore nulls order by event_timestamp desc)
        ) as gclid,
        {{ target.schema + var('dataset_udf') }}.dedup_array(
            array_agg(transaction_id ignore nulls order by event_timestamp desc)
        ) as transaction_id,
        min(event_timestamp) as session_start_at,
        max(session_engaged) as session_engaged,
        max(engagement_time) as session_engagement_time,
        max(user_first_touch_at) as user_first_touch_at,
        max(user_source) as user_source,
        max(user_medium) as user_medium,
        max(user_campaign) as user_campaign,
        {{ target.schema + var('dataset_udf') }}.channel_grouping(
            max(user_source),
            max(user_medium),
            max(user_campaign)
        ) as user_channel_grouping,
        max(device) as device,
        max(os) as os,
        max(browser) as browser,
        max(country) as country,
        max(city) as city,
        -- aggegrate session channels and select first one
        {{ target.schema + var('dataset_udf') }}.first_channel_session(
            array_agg(session_channels ignore nulls order by event_timestamp asc)
        ) as session_channel,
        countif(newsletter_subscribe = 1) as newsletter_subscription,
        countif(transaction = 1) as transactions,
        sum(transaction_value) as transaction_value,
        -- aggegrate selected item interactions (max. 100 events per session)
        array_agg(item_interaction ignore nulls order by event_timestamp desc limit 100) as item_interactions
    from events_sessionized
    group by 1,2

)

select * from final