{{
    config(
        unique_key='LISTING_ID'
    )
}}



SELECT
    scraped_date,
    listing_id,
    host_id,
    host_is_superhost,
    suburb_id as host_suburb_id,
    LGA_CODE,
    accommodates,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
FROM {{ ref('stg_listing (future fact table)') }}
LEFT JOIN {{ ref('stg_LGA') }}
    ON {{ ref('stg_listing (future fact table)') }}.listing_neighbourhood = {{ ref('stg_LGA') }}.lga_name
LEFT JOIN {{ ref('stg_Suburb') }}
    ON {{ ref('stg_listing (future fact table)') }}.host_neighbourhood = {{ ref('stg_Suburb') }}.suburb_name
