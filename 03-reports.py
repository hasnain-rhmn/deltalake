# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS dev.gold.view_customer_demographic AS
# MAGIC
# MAGIC WITH country_sales AS (
# MAGIC     -- Aggregate total sales and orders per country
# MAGIC     SELECT 
# MAGIC         g.country,
# MAGIC         COUNT(DISTINCT f.customer_key) AS total_customers,
# MAGIC         COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC         SUM(f.total_amount) AS total_sales
# MAGIC     FROM dev.gold.fact_order f
# MAGIC     JOIN dev.gold.dim_geo g ON f.geo_key = g.geo_key
# MAGIC     GROUP BY g.country
# MAGIC ),
# MAGIC demographic_sales AS (
# MAGIC     -- Aggregate total sales and orders per demographic group and country
# MAGIC     SELECT 
# MAGIC         g.country,
# MAGIC         c.age_group,
# MAGIC         c.gender,
# MAGIC         COUNT(DISTINCT f.customer_key) AS customer_count,
# MAGIC         COUNT(DISTINCT f.order_id) AS order_count,
# MAGIC         SUM(f.total_amount) AS total_revenue
# MAGIC     FROM dev.gold.fact_order f
# MAGIC     JOIN dev.gold.dim_customers c ON f.customer_key = c.customer_id
# MAGIC     JOIN dev.gold.dim_geo g ON f.geo_key = g.geo_key
# MAGIC     GROUP BY g.country, c.age_group, c.gender
# MAGIC )
# MAGIC SELECT 
# MAGIC     d.country,
# MAGIC     d.age_group,
# MAGIC     d.gender,
# MAGIC     d.customer_count,
# MAGIC     d.order_count,
# MAGIC     d.total_revenue,
# MAGIC     ROUND((d.total_revenue / cs.total_sales) * 100, 2) AS revenue_pct_share
# MAGIC FROM demographic_sales d
# MAGIC JOIN country_sales cs ON d.country = cs.country
# MAGIC ORDER BY d.country, revenue_pct_share DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS dev.gold.view_monthly_sales_summary AS
# MAGIC
# MAGIC WITH monthly_sales AS (
# MAGIC     -- Aggregate total revenue, orders, and total customers per month
# MAGIC     SELECT 
# MAGIC         g.country,
# MAGIC         DATE_TRUNC('MONTH', f.order_date) AS month,
# MAGIC         COUNT(DISTINCT f.order_id) AS order_count,
# MAGIC         COUNT(DISTINCT f.customer_key) AS total_customers,
# MAGIC         SUM(f.total_amount) AS total_revenue
# MAGIC     FROM dev.gold.fact_order f
# MAGIC     JOIN dev.gold.dim_geo g ON f.geo_key = g.geo_key
# MAGIC     GROUP BY g.country, DATE_TRUNC('MONTH', f.order_date)
# MAGIC ),
# MAGIC new_customers AS (
# MAGIC     -- Find new customers per country and month
# MAGIC     SELECT 
# MAGIC         g.country,
# MAGIC         DATE_TRUNC('MONTH', f.order_date) AS first_purchase_month,
# MAGIC         COUNT(DISTINCT f.customer_key) AS new_customers
# MAGIC     FROM dev.gold.fact_order f
# MAGIC     JOIN dev.gold.dim_geo g ON f.geo_key = g.geo_key
# MAGIC     WHERE f.customer_key IN (
# MAGIC         -- Identify customers who made their first purchase in the current month
# MAGIC         SELECT customer_key
# MAGIC         FROM dev.gold.fact_order
# MAGIC         GROUP BY customer_key
# MAGIC         HAVING MIN(order_date) = DATE_TRUNC('MONTH', MIN(order_date))
# MAGIC     )
# MAGIC     GROUP BY g.country, DATE_TRUNC('MONTH', f.order_date)
# MAGIC )
# MAGIC SELECT 
# MAGIC     ms.country,
# MAGIC     ms.month,
# MAGIC     ms.order_count,
# MAGIC     COALESCE(nc.new_customers, 0) AS new_customers,  -- Ensure NULLs are replaced with 0
# MAGIC     ms.total_revenue,
# MAGIC     -- Month-over-Month Growth
# MAGIC     ROUND(((ms.total_revenue - LAG(ms.total_revenue) OVER (PARTITION BY ms.country ORDER BY ms.month)) / 
# MAGIC            LAG(ms.total_revenue) OVER (PARTITION BY ms.country ORDER BY ms.month)) * 100, 2) AS revenue_mom_growth
# MAGIC FROM monthly_sales ms
# MAGIC LEFT JOIN new_customers nc 
# MAGIC     ON ms.country = nc.country 
# MAGIC     AND ms.month = nc.first_purchase_month
# MAGIC ORDER BY ms.country, ms.month;
# MAGIC
