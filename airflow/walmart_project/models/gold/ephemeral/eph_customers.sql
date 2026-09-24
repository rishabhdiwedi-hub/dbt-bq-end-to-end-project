
SELECT 
        DISTINCT
        customer_id,
        customer_First_Name,
        customer_Last_Name,
        customer_email,
        customer_Phone_Number,
        customer_City,
        customer_Province,
        customer_Country,
        customer_created_timestamp,
        customer_updated_timestamp,
        customer_is_active,
        customer_processed_at,
        current_timestamp() AS customer_gold_processed_at
FROM
    {{ source('walmart_silver', 'silver_obt') }}