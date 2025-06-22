
/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF SCHEMA_ID('bronze') IS NULL
    EXEC('CREATE SCHEMA bronze');
GO
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info (
   cst_id INT,
   cst_key NVARCHAR(50),
   cst_firstname NVARCHAR(55),
   cst_lastname NVARCHAR(55),
   cst_marital_status NVARCHAR(10),
   cst_gndr NVARCHAR(10),
   cst_create_date NVARCHAR(20), -- <-- ahora como texto
   dwh_load_date DATETIME NOT NULL DEFAULT GETDATE()
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info(
   prd_id       INT ,
   prd_key      NVARCHAR(50),
   prd_nm       NVARCHAR(155),
   prd_cost     DECIMAL(18,2),
   prd_line     NVARCHAR(50),    
   prd_start_dt DATETIME,
   prd_end_dt   DATETIME,
   dwh_load_date DATETIME NOT NULL DEFAULT GETDATE()
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details(
   sls_ord_num    NVARCHAR(55),
   sls_prd_key    NVARCHAR(50),
   sls_cust_id    INT,
   sls_order_dt   INT,          -- Formato YYYYMMDD
   sls_ship_dt    INT,          -- Formato YYYYMMDD
   sls_due_dt     INT,
   sls_sales      DECIMAL(18,2),
   sls_quantity   INT,          -- Cantidad vendida
   sls_price      DECIMAL(18,2), -- Precio unitario
   dwh_load_date DATETIME NOT NULL DEFAULT GETDATE()
);

GO
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL 
   DROP TABLE bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101(
   cid    NVARCHAR(50),
   cntry  NVARCHAR(50),
   dwh_load_date DATETIME NOT NULL DEFAULT GETDATE()
);

GO
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL 
   DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
   cid   NVARCHAR(50),
   bdate  DATE,
   gen    NVARCHAR(50),
   dwh_load_date DATETIME NOT NULL DEFAULT GETDATE()
);

GO
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_load_date DATETIME NOT NULL DEFAULT GETDATE()
);
GO