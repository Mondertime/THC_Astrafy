{{ config(materialized='table') }}

WITH qty_per_order AS (
	SELECT
		order_id,
		SUM(quantity) AS total_products
	FROM 
		{{ ref('stg_sales') }} AS ss
	WHERE 
		EXTRACT(YEAR FROM sale_date) IN (2022, 2023)
	GROUP BY 
		order_id
),

base_orders AS (
	SELECT
		order_id,
		customer_id,
		order_date,
		net_sales
	FROM 
		{{ ref('stg_orders') }} AS so
	WHERE 
		EXTRACT(YEAR FROM order_date) IN (2022, 2023)
),

past_orders AS (
	SELECT
		bo.order_id,
		COUNTIF(
			so.order_date BETWEEN DATE_SUB(bo.order_date, INTERVAL 12 MONTH)
			AND DATE_SUB(bo.order_date, INTERVAL 1 DAY)
		) AS previous_order_count
	FROM 
		base_orders AS bo
	LEFT JOIN 
		{{ ref('stg_orders') }} AS so ON bo.customer_id = so.customer_id
	WHERE 
		so.order_date < bo.order_date
	GROUP BY 
		bo.order_id
)

SELECT
	bo.order_id,
	bo.customer_id,
	bo.order_date,
	bo.net_sales,
	IFNULL(qo.total_products, 0) AS total_products,
	IFNULL(po.previous_order_count, 0),
	CASE 
		WHEN po.previous_order_count = 0 THEN 'New'
		WHEN po.previous_order_count BETWEEN 1 AND 3 THEN 'Returning'
		WHEN po.previous_order_count >= 4 THEN 'VIP'
		ELSE 'Unknown'
	END AS order_segment
FROM 
	base_orders AS bo
LEFT JOIN 
	qty_per_order AS qo ON bo.order_id = qo.order_id
LEFT JOIN 
	past_orders AS po ON bo.order_id = po.order_id
