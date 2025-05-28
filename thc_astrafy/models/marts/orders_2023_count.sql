{{ config(materialized='table') }}

-- Total number of orders in 2023
SELECT
	COUNT(DISTINCT order_id) AS total_orders_2023
FROM 
	{{ ref('stg_orders') }}
WHERE 
	EXTRACT(YEAR FROM order_date) = 2023
