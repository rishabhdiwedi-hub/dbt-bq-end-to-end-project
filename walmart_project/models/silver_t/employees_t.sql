{{
        config(
             materialized='incremental',
                unique_key='employee_id'
        )
}}

select 
      *,
      current_timestamp() as processed_at
        from {{ source('walmart', 'employees') }}


{% if is_incremental() %}
    WHERE updated_timestamp > (select coalesce(max(updated_timestamp), '1900-01-01') from {{ this }})
{% endif %}