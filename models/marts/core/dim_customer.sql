{{
  config(
    materialized = 'table'
  )
}}

with users as (

  select 
    * 
  from {{ ref('int_sessions_customer__grouped') }}

),

final as (

  select
    *
  from users

)

select * from final