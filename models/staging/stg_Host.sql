{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source as (
    SELECT DISTINCT
        HOST_ID,
        HOST_NAME,
        HOST_SINCE,
        HOST_IS_SUPERHOST,
        host_neighbourhood
    FROM {{ ref('snapshot_host') }}
    WHERE HOST_NAME IS NOT NULL OR HOST_NEIGHBOURHOOD IS NOT NULL
)

select * from source