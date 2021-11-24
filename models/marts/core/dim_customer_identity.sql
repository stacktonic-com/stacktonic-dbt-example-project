{{
  config(
    materialized = 'table'
  )
}}

with customer_identity_graph as (

  select 
    * 
  from {{ ref('int_sessions_customer_map__grouped') }}

),

final as (

  select
    *
  from customer_identity_graph

)

select * from final