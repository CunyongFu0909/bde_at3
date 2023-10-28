{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source as (
    SELECT DISTINCT
        ROOM_TYPE, 
        ACCOMMODATES
    FROM {{ ref('snapshot_room') }}
    WHERE ROOM_TYPE IS NOT NULL OR ACCOMMODATES IS NOT NULL
)

select * from source