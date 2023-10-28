{{
    config(
        unique_key='LGA_CODE'
    )
}}

with

source as (
    select
        SUBSTR(LGA_CODE_2016::varchar,4) as lga_code,
        Median_age_persons::int as median_age_persons,
        Median_mortgage_repay_monthly::int as median_mortgage_repay_monthly,
        Median_tot_prsnl_inc_weekly::int as median_tot_prsnl_inc_weekly,
        Median_rent_weekly::int as median_rent_weekly,
        Median_tot_fam_inc_weekly::int as median_tot_fam_inc_weekly,
        Average_num_psns_per_bedroom::decimal(32, 2) as average_num_psns_per_bedroom,
        Median_tot_hhd_inc_weekly::int as median_tot_hhd_inc_weekly,
        Average_household_size::decimal(32, 2) as average_household_size
    from postgres.raw.Census_G02_NSW_LGA
)

select * from source