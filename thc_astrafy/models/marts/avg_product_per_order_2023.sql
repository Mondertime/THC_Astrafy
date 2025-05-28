{{ config(materialized='table') }}

-- Average number of products per order for each month in 2023
WITH sales_per_order AS (
	SELECT
		order_id,
		SUM(quantity) AS total_products,
		DATE_TRUNC(sale_date, MONTH) AS month
	FROM 
		{{ ref('stg_sales') }}
	WHERE 
		EXTRACT(YEAR FROM sale_date) = 2023
	GROUP BY 
		order_id, month
)

SELECT
	FORMAT_DATE('%Y-%m', month) AS month,
	ROUND(AVG(total_products), 2) AS avg_products_per_order
FROM 
	sales_per_order
GROUP BY 
	month
ORDER BY 
	month
