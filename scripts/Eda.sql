-- *************************************************************
-- *** DATABASE EXPLORATION - Explora objetos en la base de datos ***
-- *************************************************************

USE DataWarehouse;
GO

--  Mostrar todas las tablas existentes en la base de datos
SELECT 
    TABLE_SCHEMA AS Esquema,
    TABLE_NAME AS Tabla,
    TABLE_TYPE AS TipoTabla
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;

--  Mostrar todas las columnas de una tabla específica (ejemplo: gold.dim_customers)
SELECT 
    TABLE_NAME AS Tabla,
    COLUMN_NAME AS Columna,
    DATA_TYPE AS TipoDato,
    IS_NULLABLE AS PermiteNulos
FROM INFORMATION_SCHEMA.COLUMNS
WHERE UPPER(TABLE_NAME) = 'DIM_CUSTOMERS'
  AND TABLE_SCHEMA = 'gold'
ORDER BY ORDINAL_POSITION;

-- *************************************************************
-- *** DIM CUSTOMERS EXPLORATION - Información sobre clientes ***
-- *************************************************************
--  Ver todos los países de origen de los clientes (valores únicos)
SELECT DISTINCT 
    country AS PaisOrigen
FROM gold.dim_customers
WHERE country IS NOT NULL
ORDER BY country;

-- *************************************************************
-- *** DIM PRODUCTS EXPLORATION - Categorías y subcategorías ***
-- *************************************************************

--  Mostrar todas las categorías y subcategorías únicas
SELECT DISTINCT 
    category AS Categoria,
    subcategory AS Subcategoria
FROM gold.dim_products
WHERE category IS NOT NULL
ORDER BY category, subcategory;
-- *************************************************************
-- ***                   DATE EXPLORATION                    ***
-- *************************************************************
--  Encontrar la fecha del primer y último pedido
--     Calcular cuántos años de historial de ventas hay
SELECT
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS sales_span_years
FROM gold.fact_sales;

-- Obtener la fecha de nacimiento más antigua y más reciente, y calcular edades
--  Encontrar la edad del cliente más joven y el más viejo
SELECT 
    MIN(birth_date) AS oldest_birth_date,
    DATEDIFF(YEAR, MIN(birth_date), GETDATE()) AS oldest_age,
    MAX(birth_date) AS youngest_birth_date,
    DATEDIFF(YEAR, MAX(birth_date), GETDATE()) AS youngest_age
FROM gold.dim_customers
-- Promedio de edad de los clientes
SELECT 
    AVG(DATEDIFF(YEAR, birth_date, GETDATE())) AS average_customer_age
FROM gold.dim_customers;

-- *************************************************************
-- ***                  MEASURES EXPLORATION                 ***
-- *************************************************************
--  1. Ventas Totales (Total Sales)
SELECT 
    SUM(sales_amount) AS total_sales_amount
FROM gold.fact_sales;

-- 2. Cantidad Total de Ítems Vendidos (Total Items Sold)
SELECT 
    SUM(quantity) AS total_items_sold
FROM gold.fact_sales;

--  3. Precio Promedio de Venta (Average Selling Price)
-- Calcula el promedio del precio unitario ponderado por cantidad vendida
SELECT SUM(sales_amount) / NULLIF(SUM(quantity), 0) AS avg_selling_price FROM gold.fact_sales;

--  4. Número Total de Órdenes (Total Orders)
SELECT COUNT(DISTINCT order_number) FROM gold.fact_sales

--  5. Número Total de Productos Únicos Vendidos (Unique Products Sold)
SELECT 
    COUNT(DISTINCT product_key) AS total_unique_products_sold
FROM gold.fact_sales;

--  6. Número Total de Clientes Registrados
SELECT 
COUNT(DISTINCT customer_number) AS total_customers
FROM gold.dim_customers

--  7. Número de Clientes que Han Realizado al Menos una Orden (Customers with Orders)
SELECT 
    COUNT(DISTINCT customer_key) AS customers_with_orders
FROM gold.fact_sales;

-- *************************************************************
-- ***                  MEASURES EXPLORATION                 ***
-- ***         Reporte Consolidado de Métricas Clave         ***
-- *************************************************************

SELECT 
    'Total Sales' AS metric_name,
    CAST(SUM(sales_amount) AS DECIMAL(18,2)) AS value,
    'Ventas totales del negocio' AS description
FROM gold.fact_sales
UNION ALL
SELECT 
    'Total Items Sold',
    CAST(SUM(quantity) AS INT),
    'Unidades vendidas'
FROM gold.fact_sales
UNION ALL
SELECT 
    'Average Selling Price',
    CAST(SUM(sales_amount) / NULLIF(SUM(quantity), 0) AS DECIMAL(18,2)),
    'Precio promedio ponderado por cantidad vendida'
FROM gold.fact_sales
UNION ALL
SELECT 
    'Total Orders',
    CAST(COUNT(DISTINCT order_number) AS INT),
    'Número total de órdenes únicas'
FROM gold.fact_sales
UNION ALL
SELECT 
    'Unique Products Sold',
    CAST(COUNT(DISTINCT product_key) AS INT),
    'Productos distintos vendidos'
FROM gold.fact_sales
UNION ALL
SELECT 
    'Total Customers Registered',
    CAST(COUNT(DISTINCT customer_key) AS INT),
    'Clientes registrados en el sistema'
FROM gold.dim_customers
UNION ALL
SELECT 
    'Customers with Orders',
    CAST(COUNT(DISTINCT customer_key) AS INT),
    'Clientes que han realizado al menos una compra'
FROM gold.fact_sales;


-- *************************************************************
-- ***                  MAGNITUD ANALYSIS                    ***
-- *************************************************************

----------  Encuentra total de clientes por pais ---------------
SELECT
country,
COUNT(customer_number) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY 2 DESC

-----------  Encuentra total de clientes por genero ---------------
SELECT
gender,
COUNT(customer_number) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY 2 DESC

-- Ventas Totales por Categoría de Producto
SELECT 
    p.category AS Categoria,
    SUM(s.sales_amount) AS total_sales,
    SUM(s.quantity) AS total_units_sold,
    COUNT(DISTINCT s.order_number) AS total_orders
FROM gold.fact_sales s
JOIN gold.dim_products p ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;

-- Calcular el total de ganancias generada por cliente
SELECT 
    dc.customer_key AS customer_key,
    dc.first_name AS firstname,
    dc.last_name AS lastname,
    SUM(fs.sales_amount ) AS total_venta
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc 
    ON dc.customer_key = fs.customer_key
GROUP BY dc.customer_key,dc.first_name, dc.last_name
ORDER BY total_venta DESC;
------ calcular la distribucion de productos vendidos por paises
SELECT 
  dc.country,
  dp.category,
  SUM(fs.quantity) AS total_productos_vendidos
FROM gold.fact_sales fs
JOIN gold.dim_customers dc 
  ON fs.customer_key = dc.customer_key
JOIN gold.dim_products dp 
  ON fs.product_key = dp.product_key
WHERE fs.quantity > 0
GROUP BY dc.country, dp.category
ORDER BY dc.country, total_productos_vendidos DESC;


-- *************************************************************
-- ***                  RANKING ANALYSIS                    ***
-- *************************************************************
-- 5 productos con las mas altas ventas historicas

SELECT TOP 5
dp.product_name      AS product_name,
SUM(fs.sales_amount) AS sales
FROM 
gold.fact_sales fs
JOIN gold.dim_products dp 
  ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY 2 DESC

-- 5 productos con las mas menores ventas historicas
SELECT TOP 5
dp.product_name      AS product_name,
SUM(fs.sales_amount) AS sales
FROM 
gold.fact_sales fs
JOIN gold.dim_products dp 
  ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY 2 ASC

-- Obtiene los 10 clientes con mayores ventas totales
SELECT TOP 10
    dc.customer_key,
    dc.first_name + ' ' + dc.last_name AS full_name,
    SUM(fs.sales_amount) AS total_sales,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.sales_amount) DESC) AS rank_by_sales
FROM gold.fact_sales fs
JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_key,dc.first_name, dc.last_name
ORDER BY total_sales DESC;

----- Los tres clientes con menos pedidos realizados
SELECT TOP 3
    dc.customer_key,
    dc.first_name + ' ' + dc.last_name AS full_name,
    COUNT(DISTINCT fs.order_number) AS total_orders,
    ROW_NUMBER() OVER (ORDER BY SUM(fs.sales_amount) DESC) AS rank_by_sales
FROM gold.fact_sales fs
JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_key,first_name, dc.last_name
ORDER BY total_orders ASC;