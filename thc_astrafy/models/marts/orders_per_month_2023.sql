{{ config(materialized='table') }}

-- Number of orders per month for the year 2023
SELECT
	FORMAT_DATE('%Y-%m', so.order_date) AS month,
	COUNT(DISTINCT so.order_id) AS total_orders
FROM 
	{{ ref('stg_orders') }} AS so
WHERE 
	EXTRACT(YEAR FROM so.order_date) = 2023
GROUP BY 
	FORMAT_DATE('%Y-%m', so.order_date)
ORDER BY 
	FORMAT_DATE('%Y-%m', so.order_date)
