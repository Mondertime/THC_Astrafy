{{ config(materialized='table') }}

-- Orders placed in 2023 with segmentation
WITH base_orders AS (
	SELECT
		order_id,
		customer_id,
		order_date
	FROM 
		{{ ref('stg_orders') }} AS so
	WHERE 
		EXTRACT(YEAR FROM order_date) = 2023
),

past_orders AS (
	SELECT
		bo.order_id,
		COUNTIF(
			so.order_date BETWEEN DATE_SUB(bo.order_date, INTERVAL 12 MONTH) 
			AND DATE_SUB(bo.order_date, INTERVAL 1 DAY)
		) AS past_orders_count
	FROM 
		base_orders AS bo
	LEFT JOIN 
		{{ ref('stg_orders') }} AS so
		ON bo.customer_id = so.customer_id
	WHERE 
		so.order_date < bo.order_date
	GROUP BY 
		bo.order_id
)

SELECT
	bo.order_id,
	bo.customer_id,
	bo.order_date,
	CASE 
		WHEN po.past_orders_count = 0 THEN 'New'
		WHEN po.past_orders_count BETWEEN 1 AND 3 THEN 'Returning'
		WHEN po.past_orders_count >= 4 THEN 'VIP'
		ELSE 'Unknown'
	END AS order_segment
FROM 
	base_orders AS bo
INNER JOIN 
	past_orders AS po ON bo.order_id = po.order_id
