{{
    config(
        unique_key='LGA_NAME'
    )
}}

with

source as (
    select
        ROW_NUMBER() OVER (ORDER BY suburb_name) as suburb_id, -- add an ID number for each suburb
        INITCAP(suburb_name) as suburb_name, -- use INITCAP()function to convert values format for later use 
        INITCAP(lga_name) as lga_name
    from postgres.raw.NSW_LGA_SUBURB
)

select * from source