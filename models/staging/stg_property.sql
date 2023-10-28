{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source as (
    SELECT DISTINCT
        PROPERTY_TYPE, 
        ROOM_TYPE, 
        ACCOMMODATES, 
        PRICE, 
        HAS_AVAILABILITY,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY, 
        REVIEW_SCORES_CLEANLINESS, 
        REVIEW_SCORES_CHECKIN, 
        REVIEW_SCORES_COMMUNICATION, 
        REVIEW_SCORES_VALUE
    FROM {{ ref('snapshot_property') }}
    WHERE PROPERTY_TYPE IS NOT NULL OR ROOM_TYPE IS NOT NULL
)

select * from source