{{ config(materialized='view') }}

-- Cleaned staging model for orders table
SELECT
	date_date AS order_date,
	CAST(customers_id AS STRING) AS customer_id,
	CAST(orders_id AS STRING) AS order_id,
	CAST(net_sales AS FLOAT64) AS net_sales
FROM 
	{{ source('raw', 'orders') }}
