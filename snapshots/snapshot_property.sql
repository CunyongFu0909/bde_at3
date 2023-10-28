{% snapshot snapshot_property %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='listing_id',
          check_cols=['PROPERTY_TYPE', 'ROOM_TYPE', 'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30', 'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_VALUE'],
        )
    }}

select * from {{ source('raw', 'listings') }}

{% endsnapshot %}