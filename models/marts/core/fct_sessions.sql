{{
  config(
    materialized = 'incremental',
    partition_by = {'field': 'session_start_at', 'data_type': 'timestamp'},
    incremental_strategy = 'insert_overwrite',
    tags=['incremental','daily']
  )
}}

with sessions as (

    select 
      * 
    from {{ ref('int_events_sessions__grouped') }}

    {% if var('execution_date') != 'notset' %}  
        
        where
            date(session_start_at) = '{{ var('execution_date') }}'

    {% elif is_incremental() %}

        where
            date(session_start_at) >= date_sub(date(_dbt_max_partition), interval 1 day)

    {% endif %}

),

final as (

  select  
      fpc_id,
      session_id,
      customer_id,
      gclid,
      transaction_id,
      session_start_at,
      session_engaged,
      session_engagement_time,
      user_first_touch_at,
      user_source,
      user_medium,
      user_campaign,
      user_channel_grouping,
      device,
      os,
      browser,
      country,
      city,
      session_channel.source as session_source,
      session_channel.medium as session_medium,
      session_channel.campaign as session_campaign,
      {{ target.schema + var('dataset_udf') }}.channel_grouping(
            session_channel.source,
            session_channel.medium,
            session_channel.campaign
      ) as session_channel_grouping,
      newsletter_subscription,
      transactions,
      transaction_value,
      item_interactions
  from sessions

)

select * from final