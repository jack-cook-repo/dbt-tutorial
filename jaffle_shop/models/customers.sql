{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers')}}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders')}}
),

customer_orders AS (
    SELECT
    customer_id,
    -- A very convoluted example of Jinja templated SQL
    {% for (agg, prefix) in [("MIN", 'first'), ("MAX", 'most_recent')] %}
    {{agg}}(order_date) as {{prefix}}_order_date,
    {% endfor %}
    COUNT(order_id) AS number_of_orders

    FROM orders

    GROUP BY 1
)

SELECT
customers.customer_id,
customers.first_name,
customers.last_name,
customer_orders.first_order_date,
customer_orders.most_recent_order_date,
COALESCE(customer_orders.number_of_orders, 0) AS number_of_orders

FROM customers

LEFT JOIN customer_orders USING (customer_id)