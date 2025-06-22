USE DataWarehouse;
GO
-- ================================
-- CREACIÓN DE ESQUEMA SI NO EXISTE
-- ================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- ================================
-- 1. silver.crm_cust_info
-- ================================
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
    cst_id INT PRIMARY KEY,
    cst_key NVARCHAR(55),
    cst_firstname NVARCHAR(55),
    cst_lastname NVARCHAR(55),
    cst_gndr NVARCHAR(25),
	cst_create_date    DATE,
    cst_marital_status NVARCHAR(25),
    dwh_load_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ================================
-- 2. silver.crm_prd_info
-- ================================
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id INT PRIMARY KEY,
    prd_nm NVARCHAR(100),
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_cost DECIMAL(10, 2),
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_load_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ================================
-- 3. silver.crm_sales_details
-- ================================
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(100),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(12, 2),
    sls_quantity INT,
    sls_price DECIMAL(10, 2),
    dwh_load_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ================================
-- 4. silver.erp_cust_az12
-- ================================
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(55) PRIMARY KEY,
    bdate DATE,
    gen NVARCHAR(25),
    dwh_load_date  DATETIME2 DEFAULT GETDATE()
);
GO

-- ================================
-- 5. silver.erp_loc_a101
-- ================================
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(55) PRIMARY KEY,
    cntry NVARCHAR(55),
    dwh_load_date  DATETIME2 DEFAULT GETDATE()
);
GO

-- ================================
-- 6. silver.erp_px_cat_g1v2
-- ================================
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2 (
    id  NVARCHAR(50) PRIMARY KEY,
    cat NVARCHAR(100),
    subcat NVARCHAR(100),
    maintenance    NVARCHAR(25),
    dwh_load_date  DATETIME2 DEFAULT GETDATE()
);
GO
