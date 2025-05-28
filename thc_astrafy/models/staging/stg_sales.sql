{{ config(materialized='view') }}

-- Cleaned staging model for sales table
SELECT
	date_date AS sale_date,
	CAST(customer_id AS STRING) AS customer_id,
	CAST(order_id AS STRING) AS order_id,
	CAST(products_id AS STRING) AS product_id,
	CAST(net_sales AS FLOAT64) AS net_sales,
	CAST(qty AS INT64) AS quantity
FROM 
	{{ source('raw', 'sales') }}
