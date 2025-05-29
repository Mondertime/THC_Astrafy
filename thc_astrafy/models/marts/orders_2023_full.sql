{{ config(materialized='table') }}

-- Final enriched orders table for 2023 with segmentation
SELECT
	ows.order_id,
	ows.customer_id,
	ows.order_date,
	so.net_sales,
	ows.order_segment
FROM 
	{{ ref('orders_2023_with_segmentation') }} AS ows
INNER JOIN 
	{{ ref('stg_orders') }} AS so ON ows.order_id = so.order_id
