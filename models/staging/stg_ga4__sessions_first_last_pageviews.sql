{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['session_key'],
        tags = ["incremental"],
        partition_by={
            "field": "first_page_view_event_time",
            "data_type": "timestamp",
            "granularity": "day"
        },
        on_schema_change = 'sync_all_columns',
        merge_exclude_columns = [
            'first_page_view_event_key',
            'first_page_view_event_time'
        ]
    )
}}
with page_views_first_last as (
    select
        session_key,
        FIRST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_event_key,
        LAST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_view_event_key,
        FIRST_VALUE(timestamp_micros(event_timestamp)) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_event_time,
        FIRST_VALUE(event_date_dt) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_event_date,
    from {{ref('stg_ga4__events')}}
    where event_name = 'session_start'
    {% if is_incremental() %}
        and event_date_dt >= date_sub(current_date, interval {{var('static_incremental_days',3) | int}} day)
    {% endif %}
),
page_views_by_session_key as (
    select distinct
        session_key,
        first_page_view_event_key,
        last_page_view_event_key,
        first_page_view_event_time,
        first_page_view_event_date
    from page_views_first_last
)

select * from page_views_by_session_key