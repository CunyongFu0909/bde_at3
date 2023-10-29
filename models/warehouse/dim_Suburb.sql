{{
    config(
        unique_key='LGA_NAME'
    )
}}

select 
    ROW_NUMBER() OVER (ORDER BY suburb_name) as suburb_id, 
    INITCAP(suburb_name) as suburb_name, 
    INITCAP(lga_name) as lga_name
from {{ ref('stg_Suburb') }}