{{
    config(
        unique_key='LISTING_ID'
    )
}}

WITH first_query AS (
    SELECT
        DISTINCT listing_id,
        listing_neighbourhood,
        INITCAP(REPLACE(property_type, 'Room In ', '')) AS property_type,
        room_type,
        accommodates
    FROM {{ ref('stg_listing (future fact table)') }}
),
second_query AS (
    SELECT
        DISTINCT listing_id,
        listing_neighbourhood,
        INITCAP(REPLACE(property_type, 'Shared Room In ', '')) AS property_type,
        room_type,
        accommodates
    FROM first_query
),
third_query AS (
    SELECT
        DISTINCT listing_id,
        listing_neighbourhood,
        INITCAP(REPLACE(property_type, 'Entire ', '')) AS property_type,
        room_type,
        accommodates
    FROM second_query
)

SELECT DISTINCT
    listing_id,
    listing_neighbourhood,
    INITCAP(REPLACE(property_type, 'Private room in ', '')) AS property_type,
    room_type,
    accommodates
FROM third_query


