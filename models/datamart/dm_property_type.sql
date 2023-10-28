SELECT
    d.property_type,
    d.room_type,
    d.accommodates,
    d.Month_year,
    active_listing_rate,
    Minimum_price,
    Maximum_price,
    Median_price,
    Average_price,
    Number_of_distinct_hosts,
    Superhost_rate,
    Average_of_review_scores,
    ((c.active_final - LAG(c.active_final, 1) OVER (PARTITION BY d.property_type, d.room_type, d.accommodates ORDER BY d.Month_year)) / LAG(c.active_final, 1) OVER (PARTITION BY d.property_type, d.room_type, d.accommodates ORDER BY d.Month_year) * 100)::decimal(32,2) as percent_change_rate_ative_listings,
    ((c.inactive_final - LAG(c.inactive_final, 1) OVER (PARTITION BY d.property_type, d.room_type, d.accommodates ORDER BY d.Month_year)) / LAG(c.inactive_final, 1) OVER (PARTITION BY d.property_type, d.room_type, d.accommodates ORDER BY d.Month_year) * 100)::decimal(32,2) as percent_change_rate_inative_listings,
    Total_Number_of_stays,
    Average_Estimated_revenue_per_active_listings
FROM
(
    SELECT 
        property_type,
        room_type,
        a.accommodates,
        DATE_TRUNC('month', scraped_date) as Month_year,
        (SUM(CASE WHEN has_availability = 't' THEN 1 ELSE null END)*100/COUNT(*))::decimal(32,2) as active_listing_rate,
        MIN(CASE WHEN has_availability = 't' THEN price ELSE null END) as Minimum_price,
        MAX(CASE WHEN has_availability = 't' THEN price ELSE null END) as Maximum_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN a.has_availability = 't' THEN a.price END) as Median_price,
        AVG (CASE WHEN has_availability = 't' THEN price ELSE null END)::decimal(32,2) as Average_price,
        COUNT(DISTINCT host_id) as Number_of_distinct_hosts,
        (COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id ELSE null END)*100/COUNT(DISTINCT host_id))::decimal(32,2) as Superhost_rate,
        AVG (CASE WHEN has_availability = 't' THEN review_scores_rating END)::decimal(32,2) as Average_of_review_scores,
        SUM (CASE WHEN has_availability = 't' THEN 30 - availability_30 ELSE null END) as Total_Number_of_stays,
        AVG (CASE WHEN has_availability = 't' THEN (30 - availability_30)*price ELSE null END)::decimal(32,2) as Average_Estimated_revenue_per_active_listings
    FROM {{ ref('facts_final') }} a
    LEFT JOIN {{ ref('facts_listings') }} b
    ON a.listing_id = b.listing_id and a.accommodates = b.accommodates
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4
) d
LEFT JOIN
(
    SELECT 
        property_type,
        room_type,
        a.accommodates,
        DATE_TRUNC('month', scraped_date) as Month_year,
        SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) as active_final,
        (LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY property_type, room_type, a.accommodates ORDER BY DATE_TRUNC('month', scraped_date))) as active_original,
        ((SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) - LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY property_type, room_type, a.accommodates ORDER BY DATE_TRUNC('month', scraped_date))) / LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY property_type, room_type, a.accommodates ORDER BY DATE_TRUNC('month', scraped_date)) * 100)::decimal(32,2) as percent_change_rate_ative_listings,
        SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END) as inactive_final,
        (LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY property_type, room_type, a.accommodates ORDER BY DATE_TRUNC('month', scraped_date))) as inactive_original,
        ((SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END) - LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY property_type, room_type, a.accommodates ORDER BY DATE_TRUNC('month', scraped_date))) / LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY property_type, room_type, a.accommodates ORDER BY DATE_TRUNC('month', scraped_date)) * 100)::decimal(32,2) as percent_change_rate_inative_listings
    FROM {{ ref('facts_final') }} a
    LEFT JOIN {{ ref('facts_listings') }} b
    ON a.listing_id = b.listing_id and a.accommodates = b.accommodates
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4
) c
ON d.property_type = c.property_type and 
d.room_type = c.room_type and
d.accommodates = c.accommodates and
d.Month_year = c.Month_year
ORDER BY 1,2,3,4







