WITH event_summary AS (
  SELECT
    f.event_date,
    d.weekday,
    d.weekday_name,
    SUM(f.total_spend) AS total_spend_in_event,
    SUM(f.num_tickets) AS total_tickets_in_event,
    COUNT(DISTINCT f.customer_id) AS customers_in_event
  FROM `grand-icon-475820-e5.pwhl_analytics_assessment.fact_ticket_sales` AS f
  JOIN `grand-icon-475820-e5.pwhl_analytics_assessment.dim_date` AS d
    ON f.date_id = d.date_id
  GROUP BY f.event_date, d.weekday, d.weekday_name
)
SELECT
  weekday,
  weekday_name,
  ROUND(AVG(total_spend_in_event), 2) AS avg_total_sales_per_event,
  ROUND(AVG(total_spend_in_event / customers_in_event), 2) AS avg_spend_per_customer,
  ROUND(AVG(total_tickets_in_event), 2) AS avg_tickets_sold_per_event,
  ROUND(AVG(total_tickets_in_event / customers_in_event), 2) AS avg_tickets_sold_per_customer,
  SUM(total_tickets_in_event) AS total_tickets_all_events
FROM event_summary
GROUP BY weekday, weekday_name
ORDER BY weekday;