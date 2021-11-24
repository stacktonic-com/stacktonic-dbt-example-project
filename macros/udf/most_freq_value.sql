{% macro most_freq_value() %}
    -- udf: get most frequent value from array
    create or replace function {{ target.schema + var('dataset_udf') }}.most_freq_value(arr any type) as ((
        select 
            x 
        from unnest(arr) x 
        group by x 
        order by count(1) desc limit 1
    ));
{% endmacro %}