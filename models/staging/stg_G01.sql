{{
    config(
        unique_key='LGA_CODE'
    )
}}

with

source as (
    select
        SUBSTR(LGA_CODE_2016::varchar,4) as lga_code,
        Tot_P_M::int as total_population_male,
        Tot_P_F::int as total_population_female,
        Tot_P_P::int as total_population,
        Age_0_4_yr_M::int as age_0_4yr_m,
        Age_0_4_yr_F::int as age_0_4yr_f,
        Age_0_4_yr_P::int as age_0_4yr_total,
        Age_5_14_yr_M::int as age_5_14yr_m,
        Age_5_14_yr_F::int as age_5_14yr_f,
        Age_5_14_yr_P::int as age_5_14yr_total,
        Age_15_19_yr_M::int as age_15_19yr_m,
        Age_15_19_yr_F::int as age_15_19yr_f,
        Age_15_19_yr_P::int as age_15_19yr_total,
        Age_20_24_yr_M::int as age_20_24yr_m,
        Age_20_24_yr_F::int as age_20_24yr_f,
        Age_20_24_yr_P::int as age_20_24yr_total,
        Age_25_34_yr_M::int as age_25_34yr_m,
        Age_25_34_yr_F::int as age_25_34yr_f,
        Age_25_34_yr_P::int as age_25_34yr_total,
        Age_35_44_yr_M::int as age_35_44yr_m,
        Age_35_44_yr_F::int as age_35_44yr_f,
        Age_35_44_yr_P::int as age_35_44yr_total,
        Age_45_54_yr_M::int as age_45_54yr_m,
        Age_45_54_yr_F::int as age_45_54yr_f,
        Age_45_54_yr_P::int as age_45_54yr_total,
        Age_55_64_yr_M::int as age_55_64yr_m,
        Age_55_64_yr_F::int as age_55_64yr_f,
        Age_55_64_yr_P::int as age_55_64yr_total,
        Age_65_74_yr_M::int as age_65_74yr_m,
        Age_65_74_yr_F::int as age_65_74yr_f,
        Age_65_74_yr_P::int as age_65_74yr_total,
        Age_75_84_yr_M::int as age_75_84yr_m,
        Age_75_84_yr_F::int as age_75_84yr_f,
        Age_75_84_yr_P::int as age_75_84yr_total,
        Age_85ov_M::int as age_85_over_yr_m,
        Age_85ov_F::int as age_85_over_yr_f,
        Age_85ov_P::int as age_85_over_yr_total
    from postgres.raw.Census_G01_NSW_LGA
)

select * from source
