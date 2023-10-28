{{
    config(
        unique_key='LGA_CODE'
    )
}}

with

source as (
    select
        LGA_CODE as LGA_CODE,
        LGA_NAME as LGA_NAME
    from postgres.raw.NSW_LGA_CODE
)

select * from source