SELECT c.state,
       ROUND(SUM(f.price), 2)                                        AS revenue,
       COUNT(DISTINCT f.order_id)                                    AS orders,
       ROUND(100.0 * SUM(f.price) / SUM(SUM(f.price)) OVER (), 2)    AS revenue_share_pct
FROM fact_order_items f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.state
ORDER BY revenue DESC;