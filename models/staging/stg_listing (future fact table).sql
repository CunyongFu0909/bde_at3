{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source as (
    select
        LISTING_ID::varchar as listing_id,
        SCRAPED_DATE::date as scraped_date,
        HOST_ID::varchar as host_id,
        HOST_NAME::varchar as host_name,
        HOST_IS_SUPERHOST::varchar as host_is_superhost,
        HOST_NEIGHBOURHOOD::varchar as host_neighbourhood,
        LISTING_NEIGHBOURHOOD::varchar as listing_neighbourhood,
        PROPERTY_TYPE::varchar as property_type,
        ROOM_TYPE::varchar as room_type,
        ACCOMMODATES::numeric as accommodates,
        PRICE::decimal(32,2) as price,
        HAS_AVAILABILITY::varchar as has_availability,
        AVAILABILITY_30::numeric as availability_30,
        NUMBER_OF_REVIEWS::numeric as number_of_reviews,
        REVIEW_SCORES_RATING::numeric as review_scores_rating,
        REVIEW_SCORES_ACCURACY::numeric as review_scores_accuracy,
        REVIEW_SCORES_CLEANLINESS::numeric as review_scores_cleanliness,
        REVIEW_SCORES_CHECKIN::numeric as review_scores_checkin,
        REVIEW_SCORES_COMMUNICATION::numeric as review_scores_communication,
        REVIEW_SCORES_VALUE::numeric as review_scores_value
    from postgres.raw.listings
)

select * from source