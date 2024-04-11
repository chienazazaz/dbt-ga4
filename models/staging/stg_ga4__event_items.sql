{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
            "granularity": "day"
        },
        on_schema_change = 'sync_all_columns',
    )
}}


with items_with_params as (
    select
        event_key,
        event_name,
        event_date_dt,
        stream_id,
        {{ ga4.unnest_key('event_params', 'transaction_id') }},
        {{ ga4.unnest_key('event_params', 'currency') }},
        {{ ga4.unnest_key('event_params', 'value', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'tax', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'shipping', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'affiliation', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'coupon',) }},
        {{ ga4.unnest_key('event_params', 'delivery_method', ) }},
        {{ ga4.unnest_key('event_params', 'payment_type',) }},
        i.item_id,
        i.item_name,
        i.item_brand,
        i.item_variant,
        i.item_category,
        i.item_category2,
        i.item_category3,
        i.item_category4,
        i.item_category5,
        i.price_in_usd,
        i.price,
        i.quantity,
        i.item_revenue_in_usd,
        i.item_revenue,
        i.item_refund_in_usd,
        i.item_refund,
        i.location_id,
        i.item_list_id,
        i.item_list_name,
        i.item_list_index,
        i.promotion_id,
        i.promotion_name,
        i.creative_name,
        i.creative_slot
    from {{ref('stg_ga4__events')}},
        unnest(items) as i
    where event_name in ('add_payment_info', 'add_shipping_info', 'add_to_cart','add_to_wishlist','begin_checkout' ,'purchase','refund', 'remove_from_cart','select_item', 'select_promotion','view_item_list','view_promotion', 'view_item')
    {% if is_incremental() %}
      and event_date_dt >= date_sub(current_date, interval {{var('static_incremental_days',3) | int}} day)
    {% endif %}
)

select * from items_with_params