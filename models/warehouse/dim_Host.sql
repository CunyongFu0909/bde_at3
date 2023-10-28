{{
    config(
        unique_key='LISTING_ID'
    )
}}

select 
    DISTINCT host_id,
    host_name,
    host_neighbourhood
from {{ ref('stg_Host') }}