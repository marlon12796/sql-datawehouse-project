USE DataWarehouse;
GO

-- =============================================================================
--  Dimension: gold.dim_customers
-- =============================================================================
CREATE OR ALTER PROCEDURE gold.sp_load_dim_customers AS
BEGIN 
	SET NOCOUNT ON;
	DECLARE @inserted_rows INT;
	PRINT 'Inicio de carga: gold.sp_load_dim_customers';
	WITH crm_cust_info_not_duplicated AS  (
		SELECT 
			ci.*
		FROM silver.crm_cust_info ci
		LEFT JOIN gold.dim_customers dc 
			ON dc.customer_id = ci.cst_id
		WHERE dc.customer_id IS NULL
	)
	INSERT INTO gold.dim_customers (
	customer_id,
	customer_number,
	first_name,
	last_name,
	country,
	marital_status,
	gender,
	birth_date,
	create_date
	)
	SELECT
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE 
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
		ca.bdate AS birth_date,
		ci.cst_create_date AS create_date
	FROM crm_cust_info_not_duplicated as ci
	LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
	LEFT JOIN DataWarehouse.silver.erp_loc_a101 la ON ci.cst_key = la.cid;
	SET @inserted_rows = @@ROWCOUNT;
	PRINT 'Carga finalizada exitosamente: gold.sp_load_dim_customers';
	PRINT 'Total de registros insertados: ' + CAST(@inserted_rows AS VARCHAR);
END;
GO
-- =============================================================================
--  Dimension: gold.dim_products
-- =============================================================================
CREATE OR ALTER PROCEDURE gold.sp_load_dim_products AS
BEGIN 
    SET NOCOUNT ON;
	DECLARE @inserted_rows INT;
    PRINT 'Inicio de carga: gold.load_dim_products;';
	WITH crm_prd_info_not_duplicated AS (
		SELECT
		pn.*
		FROM silver.crm_prd_info pn
		LEFT JOIN gold.dim_products dp ON pn.prd_id=dp.product_id 
		WHERE dp.product_id IS NULL
	)
	INSERT INTO gold.dim_products (
		product_id      ,
		product_number  ,
		product_name    ,
		category_id     ,
		category        ,
		subcategory     ,
		maintenance     ,
		cost            ,
		product_line    ,
		start_date      ,
		end_date        ,
		product_status  
	)
	SELECT
		pn.prd_id       AS product_id,
		pn.prd_key      AS product_number,
		pn.prd_nm       AS product_name,
		pn.cat_id       AS category_id,
		pc.cat          AS category,
		pc.subcat       AS subcategory,
		pc.maintenance  AS maintenance,
		pn.prd_cost     AS cost,
		pn.prd_line     AS product_line,
		pn.prd_start_dt AS start_date,
		pn.prd_end_dt   AS end_date,    
		CASE WHEN pn.prd_end_dt IS NULL THEN 'Active' ELSE 'Inactive' END AS product_status
	FROM 
		crm_prd_info_not_duplicated AS pn
	LEFT JOIN 
		DataWarehouse.silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id;
    SET @inserted_rows = @@ROWCOUNT;

    PRINT 'Carga finalizada exitosamente: gold.load_dim_products';
    PRINT 'Total de registros insertados: ' + CAST(@inserted_rows AS VARCHAR);
END;
GO


-- =============================================================================
--  Dimension: gold.dim_tiempo
-- =============================================================================
CREATE OR ALTER PROCEDURE gold.sp_load_dim_time
AS
BEGIN
    SET NOCOUNT ON;
    SET LANGUAGE Spanish;
    DECLARE @inserted_rows INT = 0;

    PRINT 'Inicio de carga: gold.sp_load_dim_time';

    -- Insertar fila especial para fechas desconocidas si no existe
    IF NOT EXISTS (
        SELECT 1 FROM gold.dim_time WHERE date_key = -1
    )
    BEGIN
        INSERT INTO gold.dim_time (
            date_key,
            full_date,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month_name,
            month_of_year,
            quarter,
            year,
            is_weekend
        )
        VALUES (
            -1, '1900-01-01', 1, 'Desconocido', 0, 0, 0, 'Desconocido', 0, 0, 1900, 0
        );

        PRINT 'Fila para fecha desconocida insertada en gold.dim_time';
    END ;
    -- Cargar fechas reales desde ventas
    WITH full_dates AS (
        SELECT sls_order_dt AS date FROM silver.crm_sales_details
        UNION
        SELECT sls_ship_dt FROM silver.crm_sales_details
        UNION
        SELECT sls_due_dt FROM silver.crm_sales_details
    ),
    full_dates_not_duplicated AS (
        SELECT fd.date
        FROM full_dates fd
        LEFT JOIN gold.dim_time dt ON dt.full_date = fd.date
        WHERE dt.full_date IS NULL
    )
    INSERT INTO gold.dim_time (
        date_key,
        full_date,
        day_of_week,
        day_name,
        day_of_month,
        day_of_year,
        week_of_year,
        month_name,
        month_of_year,
        quarter,
        year,
        is_weekend
    )
    SELECT 
        CONVERT(INT, FORMAT(date, 'yyyyMMdd')) AS date_key,
        date                                AS full_date,
        DATEPART(WEEKDAY, date)             AS day_of_week,
        DATENAME(WEEKDAY, date)             AS day_name,
        DATEPART(DAY, date)                 AS day_of_month,
        DATEPART(DAYOFYEAR, date)           AS day_of_year,
        DATEPART(WEEK, date)                AS week_of_year,
        DATENAME(MONTH, date)               AS month_name,
        DATEPART(MONTH, date)               AS month_of_year,
        DATEPART(QUARTER, date)             AS quarter,
        YEAR(date)                          AS year,
        IIF(
            UPPER(DATENAME(WEEKDAY, date)) COLLATE Latin1_General_CI_AI 
            IN ('SÁBADO', 'DOMINGO'), 1, 0
        ) AS is_weekend
    FROM full_dates_not_duplicated
    WHERE date IS NOT NULL;

    -- Contador de registros insertados
    SET @inserted_rows = @@ROWCOUNT;

    PRINT 'Carga finalizada: gold.sp_load_dim_time';
    PRINT 'Total de fechas insertadas: ' + CAST(@inserted_rows AS VARCHAR);
END;
GO

-- =============================================================================
-- Fact Table: gold.fact_sales
-- =============================================================================
CREATE OR ALTER PROCEDURE gold.sp_load_fact_sales AS
BEGIN
   SET NOCOUNT ON;
   DECLARE @inserted_rows INT = 0;
   PRINT 'Inicio de carga: gold.sp_load_fact_sales';

   WITH ProductMatches AS (
		SELECT
			sd.sls_ord_num,
			sd.sls_prd_key,
			sd.sls_order_dt,
			dp.product_key,
			dp.start_date,
			dp.end_date,
			dp.cost,
			dp.product_line,
			ROW_NUMBER() OVER(
		   PARTITION BY sd.sls_ord_num 
			 ORDER BY 
				IIF( dp.start_date<=sd.sls_order_dt ,0,1 ),
				ABS(DATEDIFF(DAY, dp.start_date, sd.sls_order_dt))
			) AS rn
       
		FROM
			DataWarehouse.silver.crm_sales_details sd
		LEFT JOIN gold.dim_products dp 
			ON dp.product_number = sd.sls_prd_key
	)

	INSERT INTO gold.fact_sales (
		order_number,
		customer_key,
		product_key,
		order_date_key,
		shipping_date_key,
		due_date_key,
		sales_amount,
		quantity,
		price,
		audit_is_product_later_version
	)
	SELECT
		sd.sls_ord_num AS order_number,
		ISNULL(dc.customer_key, -1) AS customer_key,
		ISNULL(pm.product_key, -1) AS product_key,
		ISNULL(CONVERT(INT, CONVERT(CHAR(8), sd.sls_order_dt, 112)),-1) AS order_date_key,
		CONVERT(INT, CONVERT(CHAR(8), sd.sls_ship_dt,  112)) AS shipping_date_key,
		CONVERT(INT, CONVERT(CHAR(8), sd.sls_due_dt,   112)) AS due_date_key,
		sd.sls_sales AS sales_amount,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price,

		IIF(pm.start_date > sd.sls_order_dt, 1, 0) AS audit_is_product_later_version 
	FROM
		DataWarehouse.silver.crm_sales_details sd
	LEFT JOIN gold.dim_customers dc 
		ON dc.customer_id = sd.sls_cust_id
	LEFT JOIN ProductMatches pm 
		ON sd.sls_ord_num = pm.sls_ord_num AND pm.rn = 1
   -- Contador de registros insertados
   SET @inserted_rows = @@ROWCOUNT;
   PRINT 'Carga finalizada: gold.sp_load_fact_sales';
   PRINT 'Total de registros insertados: ' + CAST(@inserted_rows AS VARCHAR);
END;