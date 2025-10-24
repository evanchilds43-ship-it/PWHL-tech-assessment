WITH customer_city_sales AS (
  SELECT
    v.home_city,
    c.acct_id,
    COUNT(DISTINCT f.event_date) AS event_count
  FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_venue` AS v
    ON f.venue_id = v.venue_id
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_customer` AS c
    ON f.customer_id = c.customer_id
  GROUP BY v.home_city, c.acct_id
)
SELECT
  home_city,
  COUNTIF(event_count > 1) AS repeat_customers,
  COUNT(*) AS total_customers,
  ROUND(COUNTIF(event_count > 1) / COUNT(*) * 100, 2) AS repeat_customer_pct
FROM customer_city_sales
GROUP BY home_city
ORDER BY repeat_customer_pct DESC;