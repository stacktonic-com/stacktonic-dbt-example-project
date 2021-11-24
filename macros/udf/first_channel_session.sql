{% macro first_channel_session() %}
    -- udf: get first channel from array of structs (within session)
    create or replace function {{ target.schema + var('dataset_udf') }}.first_channel_session(arr any type) as ((
        select 
            struct(
                x.source,
                x.medium,
                x.campaign
            )
        from unnest(arr) x 
        where 
            x.ignore_referrer is null 
        order by 
            x.event_timestamp asc limit 1
    ));
{% endmacro %}