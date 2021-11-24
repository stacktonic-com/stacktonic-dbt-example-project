{{
  config(
    materialized="view"
  )
}}

with source as (
    
    select 
      *,
      _table_suffix as table_suffix 
    from {{ source('google_analytics', 'events') }}
    where
        -- start date (using the _table_suffix_ pseudo column for performance)
        (_table_suffix between format_date('%Y%m%d', date('{{ var("start_date") }}'))
            and format_date('%Y%m%d', current_date()))
    
),

renamed as (
    
    select
      user_pseudo_id as fpc_id, -- first-party cookie-id
      concat(user_pseudo_id, '.', (select cast(value.int_value as string) from unnest(event_params) where key = 'ga_session_id')) as session_id,
      ifnull((select value.string_value from unnest(event_params) where key = 'traffic_type'), 'production') as traffic_type,
      *
    from source

),

filtered as (
  select 
    *
  from renamed
  where
    traffic_type not in ('development', 'internal')
)

select * from filtered