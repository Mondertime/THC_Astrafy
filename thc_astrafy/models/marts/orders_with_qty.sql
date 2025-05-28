{{ config(materialized='table') }}

-- Orders from 2022 and 2023 enriched with total quantity of products per order
WITH products_per_order AS (
	SELECT
		order_id,
		SUM(quantity) AS total_products
	FROM 
		{{ ref('stg_sales') }}
	WHERE 
		EXTRACT(YEAR FROM sale_date) IN (2022, 2023)
	GROUP BY 
		order_id
)

SELECT
	so.order_id,
	so.customer_id,
	so.order_date,
	so.net_sales,
	IFNULL(ppo.total_products, 0) AS total_products
FROM 
	{{ ref('stg_orders') }} AS so
INNER JOIN 
	products_per_order AS ppo ON so.order_id = ppo.order_id
WHERE 
	EXTRACT(YEAR FROM so.order_date) IN (2022, 2023)
