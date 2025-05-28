{{ config(materialized='table') }}

-- Average number of products per order for each month in 2023
WITH sales_per_order AS (
	SELECT
		ss.order_id,
		SUM(ss.quantity) AS total_products,
		DATE_TRUNC(ss.sale_date, MONTH) AS month
	FROM 
		{{ ref('stg_sales') }} AS ss
	WHERE 
		EXTRACT(YEAR FROM ss.sale_date) = 2023
	GROUP BY 
		ss.order_id, 
		DATE_TRUNC(ss.sale_date, MONTH)
)

SELECT
	FORMAT_DATE('%Y-%m', spo.month) AS month,
	ROUND(AVG(spo.total_products), 2) AS avg_products_per_order
FROM 
	sales_per_order AS spo
GROUP BY 
	FORMAT_DATE('%Y-%m', spo.month)
ORDER BY 
	FORMAT_DATE('%Y-%m', spo.month)
