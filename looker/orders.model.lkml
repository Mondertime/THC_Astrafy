connection: "bigquery" # Replace with the name of the connector

include: "orders.view.lkml"

explore: orders_full {
	label: "Orders"
	description: "Mart exposing order data, including segmentation, products and sales"

}
