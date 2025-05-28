{{ config(materialized='table') }}

-- Number of orders per month for the year 2023
SELECT
	FORMAT_DATE('%Y-%m', order_date) AS month,
	COUNT(DISTINCT order_id) AS total_orders
FROM 
	{{ ref('stg_orders') }}
WHERE 
	EXTRACT(YEAR FROM order_date) = 2023
GROUP BY 
	month
ORDER BY 
	month
