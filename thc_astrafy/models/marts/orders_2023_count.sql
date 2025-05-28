{{ config(materialized='table') }}

-- Total number of orders in 2023
SELECT
	COUNT(DISTINCT so.order_id) AS total_orders_2023
FROM 
	{{ ref('stg_orders') }} AS so
WHERE 
	EXTRACT(YEAR FROM so.order_date) = 2023
