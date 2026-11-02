SELECT category, product_id, revenue, units, category_rank
FROM (
    SELECT p.category,
           p.product_id,
           ROUND(SUM(f.price), 2) AS revenue,
           COUNT(*)               AS units,
           RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.price) DESC) AS category_rank
    FROM fact_order_items f
    JOIN dim_product p ON f.product_key = p.product_key
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY p.category, p.product_id
)
WHERE category_rank <= 3
ORDER BY category, category_rank;