{{
    config(
        unique_key='LGA_CODE'
    )
}}

select * from {{ ref('stg_LGA') }}