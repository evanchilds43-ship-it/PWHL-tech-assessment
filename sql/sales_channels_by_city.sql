WITH city_channel_sales AS (
  SELECT
    v.home_city,
    c.purchase_channel,
    SUM(f.num_tickets) AS total_tickets_sold
  FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_channel` AS c
    ON f.channel_id = c.channel_id
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_venue` AS v
    ON f.venue_id = v.venue_id
  GROUP BY v.home_city, c.purchase_channel
)

SELECT
  home_city,
  purchase_channel,
  total_tickets_sold,
  ROUND(
    total_tickets_sold / SUM(total_tickets_sold) OVER (PARTITION BY home_city) * 100,
    2
  ) AS percent_of_city_tickets
FROM city_channel_sales
ORDER BY home_city, percent_of_city_tickets DESC;