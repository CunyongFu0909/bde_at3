{{
    config(
        unique_key='LGA_CODE'
    )
}}

select * from {{ ref('stg_G01') }}