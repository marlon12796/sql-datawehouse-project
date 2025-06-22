USE DataWarehouse;

SELECT 
    dt.year AS year,
    dt.month_name AS month,
    SUM(fs.sales_amount) AS total_sales,
    COUNT(DISTINCT fs.customer_key) AS total_customers,
    SUM(fs.quantity) AS total_quantity
FROM 
    gold.fact_sales fs
JOIN 
    gold.dim_time dt ON dt.date_key = fs.order_date_key
WHERE 
    fs.order_date_key != -1
GROUP BY 
    dt.year,
    dt.month_of_year,
    dt.month_name
ORDER BY 
    dt.year,
    dt.month_of_year;

----------------------------------------------
SELECT 
    dt.year AS year,
    dt.month_name AS month,
    SUM(fs.sales_amount) AS total_sales,
	SUM(SUM(fs.sales_amount)) OVER (
	  ORDER BY  dt.year, dt.month_of_year
	) AS  running_total,
    -- Promedio móvil de 3 meses
    AVG(SUM(fs.sales_amount)) OVER (
        ORDER BY dt.year, dt.month_of_year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_months

FROM 
    gold.fact_sales fs
JOIN 
    gold.dim_time dt ON dt.date_key = fs.order_date_key
WHERE 
    fs.order_date_key != -1
GROUP BY 
    dt.year,
    dt.month_of_year,
    dt.month_name
ORDER BY 
    dt.year,
    dt.month_of_year;
-----------------------------------
WITH yearly_product_sales AS (
	SELECT
	dt.year              AS order_year,
	dp.product_key       AS product_key,
	dp.product_name      AS product_name,
	SUM(fs.sales_amount) AS total_sales 
	FROM 
	gold.fact_sales fs
	LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
	LEFT JOIN gold.dim_time dt ON dt.date_key= fs.order_date_key
	WHERE dt.year != 1900
	GROUP BY dt.year,dp.product_key, product_name
),
average_sales AS (
	SELECT 
	 order_year  AS year,
	 product_key AS product_key,
	AVG(total_sales) OVER (PARTITION BY product_key ) AS average_sales
	FROM yearly_product_sales
),
previous_year_sales AS (
	SELECT 
		ps1.product_key,
		ps1.order_year  AS year,
		ps1.total_sales AS current_year_sales,
		ps2.total_sales AS previous_year_sales
	FROM 
	   yearly_product_sales AS ps1
	LEFT JOIN yearly_product_sales AS ps2 ON ps1.product_key = ps2.product_key AND ps1.order_year = ps2.order_year + 1
)
SELECT
	ps.order_year,
	ps.product_name,
	ps.total_sales, 
	avgs.average_sales AS average_sales,
	pys.previous_year_sales,
	CASE 
        WHEN ps.total_sales > avgs.average_sales THEN 'Above Average'
        WHEN ps.total_sales < avgs.average_sales THEN 'Below Average'
        ELSE 'Average'
    END AS performance_vs_average,
    CASE 
        WHEN ps.total_sales > pys.previous_year_sales THEN 'Increased'
        WHEN ps.total_sales < pys.previous_year_sales THEN 'Decreased'
        ELSE 'Stable'
    END AS performance_vs_previous_year
FROM 
    yearly_product_sales ps
JOIN average_sales avgs ON ps.product_key=avgs.product_key AND ps.order_year = avgs.year
LEFT JOIN 
    previous_year_sales pys ON ps.product_key = pys.product_key AND ps.order_year = pys.year
ORDER BY 
    ps.product_key, ps.order_year;

---------------------------------------------------------------------------------------

WITH category_sales AS (
SELECT 
	dp.category,
	SUM(fs.sales_amount) AS total_sales,
	SUM(fs.sales_amount) * 1.0 / SUM(SUM(fs.sales_amount)) OVER( ) AS part_of_whole
FROM gold.fact_sales fs
JOIN gold.dim_products dp ON fs.product_key = dp.product_key
GROUP BY dp.category
)
SELECT
cs.category,
cs.total_sales,
CONCAT((CAST(part_of_whole AS DECIMAL(10,2))) * 100, '%') AS percentage_of_total
FROM category_sales cs

--------------------------------------------------------------------------------------
WITH product_segmentation AS (
	SELECT 
		product_key,
		product_name,
		cost,
	CASE WHEN cost < 100 THEN 'Bellow 100'
		 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
		 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		 ELSE 'Above 1000'
	END cost_range
	FROM
	gold.dim_products
	WHERE product_status = 'Active'
)
SELECT cost_range,COUNT(product_key) AS total_products
FROM product_segmentation 
GROUP BY cost_range
ORDER BY total_products DESC

---------------------------------------------------------------------------------
--Agrupa a los clientes en tres segmentos basados en su comportamiento de gasto:

--VIP : al menos 12 meses de historia y gasto superior a €5,000.
--Regular : al menos 12 meses de historia pero gasto de €5,000 o menos.
--New : vida útil (lifespan) de menos de 12 meses.


WITH total_spending AS (
	SELECT 
		dc.customer_key,
		SUM(fs.sales_amount)   AS total_spending,
		MIN(dt.full_date) AS first_date,
		MAX(dt.full_date) AS last_date,
		DATEDIFF(MONTH,MIN(dt.full_date),MAX(dt.full_date)) AS lifespan
	FROM 
	gold.fact_sales fs
	LEFT JOIN gold.dim_customers dc ON  fs.customer_key = dc.customer_key
	LEFT JOIN gold.dim_time dt ON dt.date_key = fs.order_date_key
	WHERE fs.order_date_key!=-1
	GROUP BY dc.customer_key
),
customer_segmentation AS (
	SELECT
		customer_key,
		total_spending,
		lifespan,
		CASE WHEN total_spending > 5000 AND lifespan >= 12 THEN 'VIP'
			 WHEN total_spending <= 5000 AND lifespan >= 12 THEN 'Regular'
			 ELSE 'New'
	END customer_segment
	FROM total_spending
)
SELECT 
customer_segment,
COUNT(customer_key) AS total_customers
FROM 
customer_segmentation
GROUP BY customer_segment
ORDER BY total_customers DESC


-----------------------------------------
-- Informe de Cliente
--Propósito:
--Este informe consolida las métricas clave y comportamientos de los clientes.
--Destacados:
--Recopila campos esenciales como nombres, edades y detalles de transacciones.
--Segmenta a los clientes en categorías (VIP, Regular, Nuevo) y grupos de edad.
--Agrega métricas a nivel de cliente:
--Total de pedidos
--Total de ventas
--Cantidad total comprada
--Total de productos
--Duración (en meses)
--Calcula KPIs valiosos:
--Recencia (meses desde el último pedido)
--Valor promedio del pedido
--Gasto mensual promedio
CREATE VIEW gold.report_customers AS
WITH base_query AS (
	SELECT 
		fs.order_number, 
		fs.product_key, 
		dt.full_date, 
		fs.sales_amount, 
		fs.quantity, 
		dc.customer_key, 
		dc.customer_number, 
		CONCAT(dc.first_name,' ',dc.last_name) AS customer_name, 
		DATEDIFF(year, dc.birth_date, GETDATE()) AS age  
	FROM gold.fact_sales fs  
	LEFT JOIN gold.dim_customers dc 
		ON dc.customer_key = fs.customer_key  
	LEFT JOIN gold.dim_time dt 
		ON dt.date_key = fs.order_date_key  
	WHERE order_date_key != -1
), 
customer_aggregation AS (
	SELECT  
		customer_key, 
		customer_number, 
		customer_name, 
		age, 
		COUNT(DISTINCT order_number) AS total_orders, 
		SUM(sales_amount) AS total_sales, 
		SUM(quantity) AS total_quantity, 
		COUNT(DISTINCT product_key) AS total_products, 
		MAX(full_date) AS last_order_date, 
		DATEDIFF(MONTH, MIN(full_date), MAX(full_date)) AS lifespan  
	FROM base_query  
	GROUP BY customer_key, customer_number, customer_name, age  
)
SELECT  
	customer_key, 
	customer_number, 
	customer_name, 
	age, 
	CASE 
		WHEN age < 20 THEN 'Under 20'      
		WHEN age BETWEEN 20 AND 29 THEN '20-29'       
		WHEN age BETWEEN 30 AND 39 THEN '30-39'       
		WHEN age BETWEEN 40 AND 49 THEN '40-49'  	
		ELSE '50 and above' 
	END AS age_group, 
	CASE 
		WHEN total_quantity > 5000 AND lifespan >= 12 THEN 'VIP' 			
		WHEN total_quantity <= 5000 AND lifespan >= 12 THEN 'Regular' 			
		ELSE 'New' 
	END AS customer_segment, 
	last_order_date, 
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency, 
	total_orders, 
	total_sales, 
	total_quantity, 
	total_products,  
	lifespan, 
	CASE 
		WHEN total_orders = 0 THEN 0      
		ELSE CAST(total_sales / total_orders AS DECIMAL(10,2)) 
	END AS average_order_value,  
	CASE 
		WHEN lifespan = 0 THEN 0       
		ELSE CAST(total_sales / lifespan AS DECIMAL(10,2)) 
	END AS avg_monthly_spend  
FROM customer_aggregation;

select * from gold.report_customers
-----------
------------ PRODUCT REPORT
----------------


--1 BASE QUERY: Retrieve core columns from fact_sales and dim_products

SELECT
fs.fact_sales_key,
fs.order_number,
fs.customer_key,
fs.product_key,
fs.order_date_key,
fs.sales_amount,
fs.quantity,
fs.price,
dp.product_name,
dp.category,
dp.subcategory,
dp.cost
FROM
gold.fact_sales fs
LEFT JOIN gold.dim_products dp ON dp.product_key = fs.product_key 
WHERE fs.order_date_key != -1
