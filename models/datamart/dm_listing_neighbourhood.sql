-- Subquery (d) to calculate various statistics per "listing_neighbourhood" and "month/year"
SELECT
    c.lga_name AS listing_neighbourhood,    -- LGA name as listing_neighbourhood
    d.Month_year,                           -- Month and year
    active_listing_rate,                     -- Active listings rate
    Minimum_price,                          -- Minimum price for active listings
    Maximum_price,                          -- Maximum price for active listings
    (
        -- Subquery to calculate the median price for active listings
        SELECT price
        FROM {{ ref('facts_final') }} AS sub
        WHERE has_availability = 't'
          AND DATE_TRUNC('month', sub.scraped_date) = DATE_TRUNC('month', d.Month_year)
        ORDER BY price
        LIMIT 1
    ) AS Median_price,
    Average_price,                          -- Average price for active listings
    Number_of_distinct_hosts,               -- Number of distinct hosts
    Superhost_rate,                         -- Superhost rate
    Average_of_review_scores,               -- Average review_scores_rating for active listings
    percent_change_rate_active_listings,    -- Percentage change for active listings
    percent_change_rate_inactive_listings,  -- Percentage change for inactive listings
    Total_Number_of_stays,                  -- Total number of stays
    Average_Estimated_revenue_per_active_listings  -- Average estimated revenue per active listings
FROM
(
    -- Subquery (d) to calculate statistics related to availability and pricing
    SELECT
        lga_code,
        DATE_TRUNC('month', scraped_date) AS Month_year,
        (SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) * 100 / COUNT(*))::decimal(32, 2) AS active_listing_rate,
        MIN(CASE WHEN has_availability = 't' THEN price ELSE NULL END) AS Minimum_price,
        MAX(CASE WHEN has_availability = 't' THEN price ELSE NULL END) AS Maximum_price,
        AVG(CASE WHEN has_availability = 't' THEN price ELSE NULL END)::decimal(32, 2) AS Average_price,
        COUNT(DISTINCT host_id) AS Number_of_distinct_hosts,
        (COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id ELSE NULL END) * 100 / COUNT(DISTINCT host_id))::decimal(32, 2) AS Superhost_rate,
        AVG(CASE WHEN has_availability = 't' THEN review_scores_rating END)::decimal(32, 2) AS Average_of_review_scores,
        SUM(CASE WHEN has_availability = 't' THEN 30 - availability_30 ELSE NULL END) AS Total_Number_of_stays,
        AVG(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE NULL END)::decimal(32, 2) AS Average_Estimated_revenue_per_active_listings
    FROM {{ ref('facts_final') }}
    GROUP BY 1, 2
) AS d

-- Subquery (c) to calculate percentage change rates
LEFT JOIN
(
    SELECT
        a.lga_code,
        b.lga_name,
        DATE_TRUNC('month', scraped_date) AS Month_year,
        SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) AS active_final,
        LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY a.lga_code ORDER BY DATE_TRUNC('month', scraped_date)) AS active_original,
        ((SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) - LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY a.lga_code ORDER BY DATE_TRUNC('month', scraped_date))) / LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY a.lga_code ORDER BY DATE_TRUNC('month', scraped_date)) * 100)::decimal(32, 2) AS percent_change_rate_active_listings,
        SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END) AS inactive_final,
        LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY a.lga_code ORDER BY DATE_TRUNC('month', scraped_date)) AS inactive_original,
        ((SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END) - LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY a.lga_code ORDER BY DATE_TRUNC('month', scraped_date))) / LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE NULL END), 1) OVER (PARTITION BY a.lga_code ORDER BY DATE_TRUNC('month', scraped_date)) * 100)::decimal(32, 2) AS percent_change_rate_inactive_listings
    FROM {{ ref('facts_final') }} AS a
    LEFT JOIN {{ ref('dim_LGA') }} AS b ON a.lga_code = b.lga_code
    GROUP BY 1, 2, 3
) AS c
ON d.lga_code = c.lga_code AND d.Month_year = c.Month_year
ORDER BY 1, 2






