CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
	new_customers_count INT, 
	new_customers_revenue NUMERIC(10, 2), 
	returning_customers_count INT, 
	returning_customers_revenue NUMERIC(10, 2), 
	refunded_customer_count INT, 
	customers_refunded NUMERIC(10, 2), 
	period_id INT, 
	period_name VARCHAR(10), 
	item_id INT, 
	city_id INT	
);

WITH 
	customers AS (
		SELECT *
	    	FROM mart.f_sales
	    	JOIN mart.d_calendar ON f_sales.date_id = d_calendar.date_id
	    	WHERE week_of_year = DATE_PART('week', '{{ds}}'::DATE)
	 ),
	 new_customers AS (
	 	SELECT customer_id
	    	FROM customers
	    	WHERE status = 'shipped'
	    	GROUP BY customer_id
	    	HAVING count(*) = 1
	 ),
	 returning_customers AS (
	 	SELECT customer_id
	    	FROM customers
	    	WHERE status = 'shipped'
	    	GROUP BY customer_id
	    	HAVING count(*) > 1
	 ),
	 refunded_customers AS (
	 	SELECT customer_id
	 	FROM customers
	 	WHERE status = 'refunded'
	 	GROUP BY customer_id
	)
INSERT INTO mart.f_customer_retention (
	new_customers_count, 
	new_customers_revenue, 
	returning_customers_count, 
	returning_customers_revenue, 
	refunded_customer_count, 
	customers_refunded, 
	period_id, 
	period_name, 
	item_id, 
	city_id
)
SELECT 
	COALESCE(new_customers.customers, 0)     AS new_customers_count,
	COALESCE(new_customers.revenue, 0)         AS new_customers_revenue,
	COALESCE(returning_customers.customers, 0) AS returning_customers_count,
	COALESCE(returning_customers.revenue, 0)    AS returning_customers_revenue,
	COALESCE(refunded_customers.customers, 0)   AS refunded_customers_count,
	COALESCE(refunded_customers.refunded, 0)    AS customers_refunded,
	COALESCE(new_customers.week_of_year, returning_customers.week_of_year, refunded_customers.week_of_year) as period_id,
	'week' as period_name,
	COALESCE(new_customers.item_id, returning_customers.item_id, refunded_customers.item_id) AS item_id,
	COALESCE(new_customers.city_id, returning_customers.city_id, refunded_customers.city_id) AS city_id
FROM (
	SELECT 
	week_of_year,
        city_id,
        item_id,
        sum(payment_amount) AS revenue,
        sum(quantity)       AS items,
        count(*)            AS customers
     FROM customers
     WHERE 
     	status = 'shipped' AND customer_id IN (
     		SELECT customer_id 
     		FROM new_customers
     	)
     GROUP BY week_of_year, city_id, item_id
) AS new_customers
FULL JOIN (
	SELECT 
		week_of_year,
        city_id,
        item_id,
        sum(payment_amount) as revenue,
        sum(quantity)       as items,
        count(*)            as customers
     FROM customers
     WHERE status = 'shipped' AND customer_id IN (
     	SELECT customer_id 
     	FROM returning_customers
     )
     GROUP BY week_of_year, city_id, item_id
) AS returning_customers
     ON new_customers.week_of_year = returning_customers.week_of_year
         AND new_customers.item_id = returning_customers.item_id
         AND new_customers.city_id = returning_customers.city_id
FULL JOIN (
	SELECT 
		week_of_year,
        city_id,
        item_id,
        sum(payment_amount) AS refunded,
        sum(quantity)       AS items,
        count(*)            AS customers
	FROM customers
    WHERE status = 'refunded' AND customer_id IN (
    	SELECT customer_id 
    	FROM refunded_customers
    )
	GROUP BY week_of_year, city_id, item_id
) AS refunded_customers
     ON new_customers.week_of_year = refunded_customers.week_of_year
         AND new_customers.item_id = refunded_customers.item_id
         AND new_customers.city_id = refunded_customers.city_id;

