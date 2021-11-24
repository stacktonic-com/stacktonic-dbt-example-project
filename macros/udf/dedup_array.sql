{% macro dedup_array() %}
    -- udf: deduplicate array of struct
    create or replace function {{ target.schema + var('dataset_udf') }}.dedup_array(arr ANY TYPE) as ((
        select 
            array_agg(t)
        from (
            select max(a.timestamp) as timestamp, a.id from unnest(arr) a 
            group by a.id 
            order by timestamp desc limit 100
        ) t
    ));
{% endmacro %}