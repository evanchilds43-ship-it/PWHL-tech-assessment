SELECT
  f.event_date,
  v.home_city,
  v.section,
  v.section_capacity,
  SUM(f.num_tickets) AS total_tickets_sold,
  ROUND(SUM(f.num_tickets) / v.section_capacity * 100, 2) AS percent_full
FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_venue` AS v
  ON f.venue_id = v.venue_id
GROUP BY
  f.event_date,
  v.home_city,
  v.section,
  v.section_capacity
ORDER BY
  v.home_city,
  v.section,
  f.event_date,
  percent_full DESC;