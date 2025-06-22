USE DataWarehouse;
GO
-- ============================================
-- PROCEDURE: silver.load_crm_cust_info
-- Description: Load deduplicated and cleaned customer info from bronze
-- ============================================
CREATE OR ALTER PROCEDURE silver.load_crm_cust_info AS
BEGIN
    DECLARE @start_time DATETIME = GETDATE(), @end_time DATETIME;

    PRINT UPPER('>> [START] Loading silver.crm_cust_info');
	PRINT '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
    
	WITH latest_customers AS (
		SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
		FROM DataWarehouse.bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	)
	INSERT INTO DataWarehouse.silver.crm_cust_info (
        cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
        cst_marital_status,
		cst_gndr,
		cst_create_date 
    )
	SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM latest_customers
    WHERE rn = 1

    SET @end_time = GETDATE();
    PRINT '>> [END] Load completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds.';
END;
GO

-- ============================================
-- PROCEDURE: silver.load_crm_prd_info
-- Description: Load and transform product information
-- ============================================
CREATE OR ALTER PROCEDURE silver.load_crm_prd_info AS
BEGIN
    DECLARE @start_date DATETIME = GETDATE(), @end_time DATETIME;

    PRINT UPPER('>> [START] Loading silver.crm_prd_info');
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO DataWarehouse.silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
    FROM DataWarehouse.bronze.crm_prd_info;

    SET @end_time = GETDATE();
    PRINT '>> [END] silver.crm_prd_info loaded successfully: completed in ' + CAST(DATEDIFF(SECOND, @start_date, @end_time) AS NVARCHAR(10)) + ' seconds.';
END;
GO

-- ============================================
-- PROCEDURE: silver.load_crm_sales_details
-- Description: Load and correct sales transaction details
-- ============================================
CREATE OR ALTER PROCEDURE silver.load_crm_sales_details AS
BEGIN
    DECLARE @start_date DATETIME = GETDATE(), @end_time DATETIME;

    PRINT UPPER('>> [START] Loading silver.crm_sales_details');
    PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO DataWarehouse.silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END,
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END,
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END,
        CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
        ISNULL(sls_quantity, 0) AS sls_quantity,
        CASE 
            WHEN (sls_price IS NULL OR sls_price <= 0) AND ISNULL(sls_quantity, 0) > 0
                THEN  ABS(ISNULL(sls_sales, 0)) / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM DataWarehouse.bronze.crm_sales_details;

    SET @end_time = GETDATE();
    PRINT '>> [END] silver.crm_sales_details loaded successfully: completed in ' + CAST(DATEDIFF(SECOND, @start_date, @end_time) AS NVARCHAR(10)) + ' seconds.';
END;
GO

-- ============================================
-- PROCEDURE: silver.load_erp_cust_az12
-- Description: Normalize and clean ERP customer info
-- ============================================
CREATE OR ALTER PROCEDURE silver.load_erp_cust_az12 AS
BEGIN
    DECLARE @start_date DATETIME = GETDATE(), @end_time DATETIME;

    PRINT UPPER('>> [START] Loading silver.erp_cust_az12');
    PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE DataWarehouse.silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';

    INSERT INTO DataWarehouse.silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        cid,
        CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM DataWarehouse.bronze.erp_cust_az12;

    SET @end_time = GETDATE();
    PRINT '>> [END] silver.erp_cust_az12 loaded successfully: completed in ' + CAST(DATEDIFF(SECOND, @start_date, @end_time) AS NVARCHAR(10)) + ' seconds.';
END;
GO

-- ============================================
-- PROCEDURE: silver.load_erp_loc_a101
-- Description: Normalize country codes from ERP locations
-- ============================================
CREATE OR ALTER PROCEDURE silver.load_erp_loc_a101 AS
BEGIN
    DECLARE @start_date DATETIME = GETDATE(), @end_time DATETIME;

    PRINT UPPER('>> [START] Loading silver.erp_loc_a101');

    PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE DataWarehouse.silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO DataWarehouse.silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM DataWarehouse.bronze.erp_loc_a101;

    SET @end_time = GETDATE();
    PRINT '>> [END] silver.erp_loc_a101 loaded successfully: completed in ' + CAST(DATEDIFF(SECOND, @start_date, @end_time) AS NVARCHAR(10)) + ' seconds.';
END;
GO

-- ============================================
-- PROCEDURE: silver.load_erp_px_cat_g1v2
-- Description: Copy ERP product categories as-is
-- ============================================
CREATE OR ALTER PROCEDURE silver.load_erp_px_cat_g1v2 AS
BEGIN
    DECLARE @start_date DATETIME = GETDATE(), @end_time DATETIME;

    PRINT UPPER('>> [START] Loading silver.erp_px_cat_g1v2');
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO DataWarehouse.silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM DataWarehouse.bronze.erp_px_cat_g1v2;

    SET @end_time = GETDATE();
    PRINT '>> [END] silver.erp_px_cat_g1v2 loaded successfully: completed in ' + CAST(DATEDIFF(SECOND, @start_date, @end_time) AS NVARCHAR(10)) + ' seconds.';
END;
GO

-- ============================================
-- PROCEDURE: silver.load_all
-- Description: Orquestador para carga completa de capa Silver
-- ============================================

CREATE OR ALTER PROCEDURE silver.load_all AS
BEGIN 
   DECLARE @global_start DATETIME = GETDATE(), @global_end DATETIME;
   PRINT '====================================================';
   PRINT UPPER('>> [START] Carga completa de la capa Silver');
   PRINT '>> Inicio global: ' + CONVERT(NVARCHAR, @global_start, 120);
   PRINT '====================================================';
   BEGIN TRY
    	PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
		EXEC silver.load_crm_cust_info;
		EXEC silver.load_crm_prd_info;
		EXEC silver.load_crm_sales_details;
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		EXEC silver.load_erp_cust_az12;
        EXEC silver.load_erp_loc_a101;
        EXEC silver.load_erp_px_cat_g1v2;
   END TRY
   BEGIN CATCH
        PRINT '>> [ERROR] Se produjo un error durante la ejecución de uno de los procedimientos.';
		PRINT '>> Mensaje: '+ ERROR_MESSAGE();
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
   END CATCH
   PRINT '====================================================';
   PRINT '>> [END] Carga completa de Silver finalizada';
   PRINT '>> Tiempo total: ' + CAST(DATEDIFF(SECOND, @global_start, @global_end) AS NVARCHAR) + ' segundos.';
   PRINT '>> Fin global: ' + CONVERT(NVARCHAR,@global_end,120);
   PRINT '====================================================';
END;

EXEC silver.load_all;


SELECT
*
FROM silver.crm_prd_info
ORDER BY  prd_nm,prd_end_dt


select * from   DataWarehouse.bronze.crm_sales_details WHERE sls_prd_key='TT-R982';