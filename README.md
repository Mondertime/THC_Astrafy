# THC_Astrafy

## Part 1 : dbt + BigQuery Setup

First Step : Created a new GitHub repository : `https://github.com/Mondertime/THC_Astrafy`

Second Step : Setup the environment

I had several troubles for the setup, here a non-exhaustive list with the solution I brought :

| Problem                                  | Solution                                 |
|:----------------------------------------|:-----------------------------------------|
| OAuth failed during `dbt debug`         | Switched to service account auth         |
| BigQuery table access denied            | Moved project out of OneDrive sync folder |
| Excel import would not work in BigQuery | Converted file to CSV                    |
| `dbt init` ran in wrong subfolder       | Restarted whole project in a clean folder |
| dbt compatibility with Python 3.13      | Downgraded Python version to 3.10        |

Now that the project is setup, here are the steps I took:

1. **Created dataset** in BigQuery: `dbt_thc` in project `thc-astrafy-461109`
2. **Initialized dbt project** with `dbt init thc_astrafy` in the root folder
3. **Created staging layer** with:
   - `stg_orders.sql`: cleaned columns, cast types
   - `stg_sales.sql`: same structure for sales data
4. **Created source file** `staging_sources.yml` in `models/staging/`
   - Declared source tables `orders` and `sales` from dataset `dbt_thc`
   - Added column descriptions for documentation
5. **Ran `dbt run --select staging`**
   - Verified that staging models were materialized as views in BigQuery
   - Confirmed schema correctness

Following that, I created **mart models** to answer each exercise of the challenge:

**First exercise**

`orders_2023_count.sql`

```sql
{{ config(materialized='table') }}

-- Total number of orders in 2023
SELECT
	COUNT(DISTINCT so.order_id) AS total_orders_2023
FROM 
	{{ ref('stg_orders') }} AS so
WHERE 
	EXTRACT(YEAR FROM so.order_date) = 2023
```

**Second exercise**

`order_per_month_2023.sql`
```sql
{{ config(materialized='table') }}

-- Number of orders per month for the year 2023
SELECT
	FORMAT_DATE('%Y-%m', so.order_date) AS month,
	COUNT(DISTINCT so.order_id) AS total_orders
FROM 
	{{ ref('stg_orders') }} AS so
WHERE 
	EXTRACT(YEAR FROM so.order_date) = 2023
GROUP BY 
	FORMAT_DATE('%Y-%m', so.order_date)
ORDER BY 
	FORMAT_DATE('%Y-%m', so.order_date)
```

**Third exercise**

`avg_product_per_order_2023.sql`
```sql
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
```

**Fourth exercise**

`orders_with_qty.sql`
```sql
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
```

**Fifth exercise**

`orders_2023_with_segmentation.sql`
```sql
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
```
For verification purposes, I have created a second version of the file with the previous order count:
```sql
{{ config(materialized='table') }}

-- Orders placed in 2023 with segmentation and previous order count for verification
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
	po.past_orders_count AS previous_order_count,
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
```

**Sixth exercise**

Here I learned that I could use intricate marts in the model, so I reused the table generated for Exercise#4 :

`orders_2023_full.sql`
```sql
{{ config(materialized='table') }}
Add commentMore actions
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
```

**Bonus**

After reading the rest of the subject, I realized that to make a meaningful dashboard, it would be handy to have a version of Exercise#6 but with temporal history, so I created this additional mart:

```sql
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
```

## Part 2 : Looker Semantic Layer
Having never had anything to do with Looker, I first decided to follow Google's training courses to familiarize myself with this tool. Google then delivered this highly honored certification to me :

`https://www.credly.com/badges/e7a51f83-5a10-4e8c-a77a-ce686799e179`

The main problem I encountered was access to Looker. Being a proprietary pay tool, it was impossible for me to access it. I therefore coded at the root of the project the semantic model I would have used for this project.

`\looker\orders.model.lkml`:
```yaml
connection: "bigquery" # Replace with the name of the connector

include: "orders.view.lkml"

explore: orders_full {
	label: "Orders"
	description: "Mart exposing order data, including segmentation, products and sales"

}
```

`\looker\orders.view.lkml` :
```yaml
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
```

## Part 3 : Dashboard

Due to the lack of access to Looker, I used Looker Studio as a workaround to build the visual layer. However, I was unfamiliar with the tool and encountered several usability issues.

I struggled with:
- Creating **calculated metrics** directly in Looker Studio
- Differentiating between **global filters** (that apply to all elements) and **individual filters** (applied per chart)

Due to these limitations, I created a very simple and minimal dashboard, with placeholders. I've decided to make a monthly filtered dashboard.

#### KPIs section:
- **Net Sales**
- **Average Basket** (Net Sales / Customer Count)
- **Average Customers per Day**
- **Net Margin**

I planned to compare each KPI between selected period (N) and previous year (N-1), and show the variation ((N - N-1) / N) colored green/red for quick reading

#### Donut Chart:
- Displays distribution of customers by segment:
  - New
  - Returning
  - VIP

#### Line Chart:
- Shows sales over time
- Intention: overlay Year N and Year N-1 to make the temporal comparison
- Ideally, the month filter should be ignored for this specific chart to show a full-year trend

---

### What I Would Have Built with Better Tool Access

If I had full access to Looker, and more kownledge of Looker Studio, I would have:

- Created reusable Looker measures (e.g. dynamic N/N-1 logic, net margin, product return rate)
- Built:
  - Customer churn analysis by detecting inactive clients
  - Product-level insights: best sellers, repeat purchase rate
  - Category segmentation if product metadata was available
---

**Bonus**

I reflected on how I would approach the bonus question about predicting future sales trends.

Since I had already built a clean and structured dataset (`orders_full`), I would have leveraged this foundation to implement a forecasting pipeline.

**1. Forecasting methodology**

I would have calculated a smoothed value over the last 3 or 6 months to approximate the trend.

Example of what the query could look like:

```sql
SELECT
	DATE_TRUNC(order_date, MONTH) AS forecast_month,
	AVG(net_sales) OVER (
		ORDER BY DATE_TRUNC(order_date, MONTH)
		ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
	) AS smoothed_net_sales
FROM
	dbt_thc.orders_full
WHERE
	EXTRACT(YEAR FROM order_date) IN (2022, 2023)
```

This would provide a basic rolling average forecast, which I could then extrapolate visually in Looker Studio using the last known values to project the next month manually.

If more robustness was needed, I could also compute:
- A trendline from linear regression using SQL logic
- A weighted average (more recent months weighted higher)