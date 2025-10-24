WITH event_summary AS (
  SELECT
    f.event_date,
    v.home_city,
    SUM(f.total_spend) AS total_spend_in_event,
    SUM(f.num_tickets) AS total_tickets_in_event,
    COUNT(DISTINCT f.customer_id) AS customers_in_event
  FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_venue` AS v
    ON f.venue_id = v.venue_id
  GROUP BY f.event_date, v.home_city
)
SELECT
  home_city,
  ROUND(AVG(total_spend_in_event), 2) AS avg_total_sales_per_event,
  ROUND(AVG(total_spend_in_event / customers_in_event), 2) AS avg_spend_per_customer,
  ROUND(AVG(total_tickets_in_event), 2) AS avg_tickets_sold_per_event,
  ROUND(AVG(total_tickets_in_event / customers_in_event), 2) AS avg_tickets_sold_per_customer,
  ROUND(AVG(total_spend_in_event / total_tickets_in_event), 2) AS avg_ticket_price,
  SUM(total_tickets_in_event) AS total_tickets_all_events
FROM event_summary
GROUP BY home_city
ORDER BY avg_total_sales_per_event DESC;