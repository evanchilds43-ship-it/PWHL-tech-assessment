WITH event_summary AS (
  SELECT
    f.event_date,
    w.avg_temp_c,
    CASE
      WHEN w.avg_temp_c < 0 THEN 'Below 0°C'
      ELSE 'Above or equal 0°C'
    END AS temp_category,
    SUM(f.total_spend) AS total_spend_in_event,
    SUM(f.num_tickets) AS total_tickets_in_event,
    COUNT(DISTINCT f.customer_id) AS customers_in_event
  FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_weather` AS w
    ON f.weather_id = w.weather_id
  GROUP BY f.event_date, w.avg_temp_c, temp_category
)

SELECT
  temp_category,
  ROUND(AVG(total_spend_in_event), 2) AS avg_total_sales_per_event,
  ROUND(AVG(total_spend_in_event / customers_in_event), 2) AS avg_spend_per_customer,
  ROUND(AVG(total_tickets_in_event), 2) AS avg_tickets_sold_per_event,
  ROUND(AVG(total_tickets_in_event / customers_in_event), 2) AS avg_tickets_sold_per_customer,
  SUM(total_tickets_in_event) AS total_tickets_all_events
FROM event_summary
GROUP BY temp_category
ORDER BY temp_category;