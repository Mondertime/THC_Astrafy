view: orders_full {
	sql_table_name: dbt_thc.orders_full ;;

	# Dimensions
	dimension: order_id {
		primary_key: yes
		type: string
		sql: ${TABLE}.order_id ;;
	}

	dimension: customer_id {
		type: string
		sql: ${TABLE}.customer_id ;;
	}

	dimension: order_date {
		type: date
		sql: ${TABLE}.order_date ;;
	}

	dimension: order_segment {
		type: string
		sql: ${TABLE}.order_segment ;;
	}

	dimension: previous_order_count {
		type: number
		sql: ${TABLE}.previous_order_count ;;
	}

	dimension: total_products {
		type: number
		sql: ${TABLE}.total_products ;;
	}

	dimension: year {
		type: number
		sql: EXTRACT(YEAR FROM ${order_date}) ;;
	}

	dimension: month {
		type: string
		sql: FORMAT_DATE('%Y-%m', ${order_date}) ;;
	}

	# Measures
	measure: total_orders {
		type: count
		sql: ${order_id} ;;
	}

	measure: total_net_sales {
		type: sum
		sql: ${net_sales} ;;
	}

	measure: avg_net_sales {
		type: average
		sql: ${net_sales} ;;
	}

	measure: avg_products_per_order {
		type: average
		sql: ${total_products} ;;
	}

	measure: vip_orders {
		type: count
		filters: [order_segment: "VIP"]
		sql: ${order_id} ;;
	}

	measure: new_orders {
		type: count
		filters: [order_segment: "New"]
		sql: ${order_id} ;;
	}

	measure: returning_orders {
		type: count
		filters: [order_segment: "Returning"]
		sql: ${order_id} ;;
	}
}
