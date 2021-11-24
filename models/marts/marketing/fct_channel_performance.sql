{{
  config(
    materialized = 'incremental',
    partition_by = {'field': 'date', 'data_type': 'date'},
    incremental_strategy = 'insert_overwrite',
    tags=['incremental', 'daily']
  )
}}

with sessions_day as (

    select 
      * 
    from {{ ref('int_sessions_day__grouped') }}

    {% if var('execution_date') != 'notset'%}  
        
        where
            date = '{{ var('execution_date') }}'

    {% elif is_incremental() %}

        where
            date >= date_sub(date(_dbt_max_partition), interval 1 day)

    {% endif %}

),

final as (

    select  
       *
    from sessions_day

)

select * from final