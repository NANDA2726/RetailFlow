WITH monthly AS (
    SELECT make_date(d.year, d.month, 1)  AS month_start,
           ROUND(SUM(f.price), 2)         AS revenue,
           COUNT(DISTINCT f.order_id)     AS orders
    FROM fact_order_items f
    JOIN dim_date d ON f.purchase_date_key = d.date_key
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1
)
SELECT month_start,
       revenue,
       orders,
       ROUND(AVG(revenue) OVER (
           ORDER BY month_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS revenue_3m_avg,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month_start))
             / NULLIF(LAG(revenue) OVER (ORDER BY month_start), 0), 1)        AS mom_growth_pct
FROM monthly
ORDER BY month_start;