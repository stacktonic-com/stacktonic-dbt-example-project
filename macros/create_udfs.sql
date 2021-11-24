{% macro create_udfs() %}

    create schema if not exists {{ target.schema + var('dataset_udf') }};

    -- create or update udfs
    {{ channel_grouping() }}
    {{ dedup_array() }}
    {{ first_channel_session() }}
    {{ most_freq_value() }}

{% endmacro %}