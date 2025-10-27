WITH per_section AS (
  SELECT
    v.event_date,
    v.home_city,
    v.section,
    MAX(v.section_capacity) AS section_capacity,
    SUM(f.num_tickets)       AS tickets_sold
  FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_venue` AS v
    ON f.venue_id = v.venue_id
  GROUP BY v.event_date, v.home_city, v.section
)
SELECT
  event_date,
  home_city,
  SUM(section_capacity) AS total_capacity,
  SUM(tickets_sold)     AS total_attendance,
  ROUND(100 * SUM(tickets_sold) / SUM(section_capacity), 2) AS pct_attendance
FROM per_section
GROUP BY event_date, home_city
ORDER BY event_date, home_city;