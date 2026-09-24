SELECT 
        DISTINCT
        order_id,
        order_item_id,
        payment_method,
        order_status,
        order_timestamp,
        order_created_timestamp,
        order_updated_timestamp,
        order_is_active,
        order_processed_at,
        silver_obt_created_timestamp,
        current_timestamp() AS order_gold_processed_at
FROM
    {{ source('walmart_silver', 'silver_obt') }}