{{
    config(
        unique_key='LISTING_ID'
    )
}}

select * from {{ ref('stg_room') }}