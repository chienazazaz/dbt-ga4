  -- depends_on: {{ ref('stg_ga4__sessions_first_last_pageviews') }}

{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        tags = ["incremental"],
        on_schema_change = 'sync_all_columns',
        unique_key = ['session_key'],
        partition_by={
            "field": "session_partition_date",
            "data_type": "date",
            "granularity": "day"
        },
        merge_exclude_columns = [

        ]
    )
}}

with session_events as (
    select
        events.session_key
        ,events.event_timestamp
        ,events.event_source
        ,events.event_medium
        ,events.event_campaign
        ,events.event_content
        ,events.event_term
        ,source_categories.source_category
        ,events.event_date_dt
    from {{ref('stg_ga4__events')}} events
    left join {{ref('ga4_source_categories')}} source_categories on events.event_source = source_categories.source
    {% if is_incremental() %}
    inner join {{ref("stg_ga4__sessions_first_last_pageviews")}} pv 
    on events.session_key = pv.session_key and events.event_date_dt = date(pv.first_page_view_event_date)    
    {% endif %}
    where events.session_key is not null
    and event_name != 'session_start'
    and event_name != 'first_visit'
    {% if is_incremental() %}
      and date(pv.first_page_view_event_time) >= date_sub(current_date, interval {{var('static_incremental_days',3) | int}} day)
    {% endif %}
   ),
set_default_channel_grouping as (
    select
        *
        ,{{ga4.default_channel_grouping('event_source','event_medium','source_category', 'event_campaign')}} as default_channel_grouping
    from session_events
),
session_source as (
    select
        session_key
        ,FIRST_VALUE( event_date_dt IGNORE NULLS) OVER (session_window) AS session_partition_date
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN event_source END) IGNORE NULLS) OVER (session_window), '(direct)') AS session_source
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_medium, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_medium
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_source_category
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_campaign, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_campaign
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_content, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_content
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_term, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_term
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(default_channel_grouping, 'Direct') END) IGNORE NULLS) OVER (session_window), 'Direct') AS session_default_channel_grouping
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
select distinct * 
from session_source