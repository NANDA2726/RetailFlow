WITH order_reviews AS (
    -- aggregate to one row per order first, so joining reviews can't multiply rows
    SELECT order_id, AVG(review_score) AS review_score
    FROM fact_reviews
    GROUP BY 1
),
seller_orders AS (
    SELECT s.seller_id,
           s.state,
           f.order_id,
           SUM(f.price)                      AS order_revenue,
           MAX(CAST(f.is_late AS INTEGER))   AS is_late
    FROM fact_order_items f
    JOIN dim_seller s ON f.seller_key = s.seller_key
    WHERE f.is_late IS NOT NULL
      AND f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY s.seller_id, s.state, f.order_id
),
seller_stats AS (
    SELECT so.seller_id,
           so.state,
           COUNT(*)                          AS delivered_orders,
           ROUND(SUM(so.order_revenue), 2)   AS revenue,
           ROUND(100.0 * AVG(so.is_late), 2) AS late_delivery_pct,
           ROUND(AVG(r.review_score), 2)     AS avg_review_score
    FROM seller_orders so
    LEFT JOIN order_reviews r ON so.order_id = r.order_id
    GROUP BY so.seller_id, so.state
    HAVING COUNT(*) >= 30
)
SELECT *,
       RANK() OVER (ORDER BY revenue DESC)           AS revenue_rank,
       RANK() OVER (ORDER BY late_delivery_pct DESC) AS lateness_rank
FROM seller_stats
ORDER BY revenue DESC;