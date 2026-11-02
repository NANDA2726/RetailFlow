WITH customer_orders AS (
    SELECT DISTINCT c.customer_unique_id,
           f.order_id,
           date_trunc('month', f.order_purchase_ts) AS order_month
    FROM fact_order_items f
    JOIN dim_customer c ON f.customer_key = c.customer_key
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
),
first_order AS (
    SELECT customer_unique_id, MIN(order_month) AS cohort_month
    FROM customer_orders
    GROUP BY 1
),
activity AS (
    SELECT DISTINCT o.customer_unique_id,
           fo.cohort_month,
           date_diff('month', fo.cohort_month, o.order_month) AS month_offset
    FROM customer_orders o
    JOIN first_order fo USING (customer_unique_id)
),
cohort_size AS (
    SELECT cohort_month, COUNT(*) AS cohort_customers
    FROM first_order
    GROUP BY 1
)
SELECT a.cohort_month,
       s.cohort_customers,
       a.month_offset,
       COUNT(*) AS active_customers,
       ROUND(100.0 * COUNT(*) / s.cohort_customers, 2) AS retention_pct
FROM activity a
JOIN cohort_size s ON a.cohort_month = s.cohort_month
WHERE a.month_offset BETWEEN 0 AND 6
GROUP BY a.cohort_month, s.cohort_customers, a.month_offset
ORDER BY a.cohort_month, a.month_offset;