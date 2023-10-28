SELECT
    host_neighbourhood_lga,
    DATE_TRUNC('month', scraped_date) as Month_year,
    COUNT(DISTINCT d.host_id) as Number_of_distinct_hosts,
    SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END)::decimal(32, 2) as Estimated_revenue,
    (SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) / COUNT(DISTINCT d.host_id))::decimal(32, 2) as Estimated_revenue_per_distinct_host
FROM {{ ref('facts_final') }} d
LEFT JOIN
(
    SELECT
        host_id,
        host_name,
        LGA_NAME as host_neighbourhood_lga
    FROM {{ ref('dim_Host') }} a
    LEFT JOIN {{ ref('dim_Suburb') }} b
    ON a.host_neighbourhood = b.suburb_name
) c
ON d.host_id = c.host_id
GROUP BY 1,2
ORDER BY 1,2







