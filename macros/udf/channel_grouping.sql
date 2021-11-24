{% macro channel_grouping() %}
    -- udf: custom channel grouping definition
    create or replace function {{ target.schema + var('dataset_udf') }}.channel_grouping(tsource string, medium string, campaign string) as (
        case
            when (tsource = 'direct' or tsource is null) 
                and (regexp_contains(medium, r'^(\(not set\)|\(none\))$') or medium is null) 
                then 'direct'
            when regexp_contains(campaign, r'^(.*shop.*)$') 
                and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
                then 'shopping_paid'
            when regexp_contains(tsource, r'^(google|bing)$') 
                and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
                then 'search_paid'
            when regexp_contains(tsource, r'^(twitter|facebook|fb|instagram|ig|linkedin|pinterest)$')
                and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*|social_paid)$') 
                then 'social_paid'
            when regexp_contains(tsource, r'^(youtube)$')
                and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
                then 'video_paid'
            when regexp_contains(medium, r'^(display|banner|expandable|interstitial|cpm)$') 
                then 'display'
            when regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
                then 'other_paid'
            when regexp_contains(medium, r'^(.*shop.*)$') 
                then 'shopping_organic'
            when regexp_contains(tsource, r'^.*(twitter|t\.co|facebook|instagram|linkedin|lnkd\.in|pinterest).*') 
                or regexp_contains(medium, r'^(social|social_advertising|social-advertising|social_network|social-network|social_media|social-media|sm|social-unpaid|social_unpaid)$') 
                then 'social_organic'
            when regexp_contains(medium, r'^(.*video.*)$') 
                then 'video_organic'
            when regexp_contains(tsource, r'^(google|bing|yahoo|baidu|duckduckgo|yandex|ask)$') 
                or medium = 'organic'
                then 'search_organic'
            when regexp_contains(tsource, r'^(email|mail|e-mail|e_mail|e mail|mail\.google\.com)$') 
                or regexp_contains(medium, r'^(email|mail|e-mail|e_mail|e mail)$') 
                then 'email'
            when regexp_contains(medium, r'^(affiliate|affiliates)$') 
                then 'affiliate'
            when medium = 'referral'
                then 'referral'
            when medium = 'audio' 
                then 'audio'
            when medium = 'sms'
                then 'sms'
            when ends_with(medium, 'push')
                or regexp_contains(medium, r'.*(mobile|notification).*') 
                then 'mobile_push'
            else '(other)'
        end
    );
{% endmacro %}