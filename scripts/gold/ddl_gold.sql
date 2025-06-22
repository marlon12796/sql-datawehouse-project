USE DataWarehouse;
GO

-- Eliminar la tabla dependiente primero
IF OBJECT_ID('gold.fact_sales','U') IS NOT NULL
    DROP TABLE gold.fact_sales;
GO

-- Ahora puedes eliminar las tablas de dimensiones
IF OBJECT_ID('gold.dim_customers','U') IS NOT NULL
    DROP TABLE gold.dim_customers;
GO

IF OBJECT_ID('gold.dim_products','U') IS NOT NULL
    DROP TABLE gold.dim_products;
GO

IF OBJECT_ID('gold.dim_time', 'U') IS NOT NULL
    DROP TABLE gold.dim_time;
GO

-- Crear dim_customers
CREATE TABLE gold.dim_customers (
    customer_key     INT IDENTITY(1,1) PRIMARY KEY,
    customer_id      INT NOT NULL,
    customer_number  VARCHAR(50) NOT NULL,
    first_name       VARCHAR(55),
    last_name        VARCHAR(55),
    country          VARCHAR(55) NOT NULL,
    marital_status   VARCHAR(50) NOT NULL,
    gender           VARCHAR(20) NOT NULL,
    birth_date       DATE,
    create_date      DATE
);
GO

-- Crear dim_products
CREATE TABLE gold.dim_products (
    product_key     INT IDENTITY(1,1) PRIMARY KEY,
    product_id      INT NOT NULL,
    product_number  VARCHAR(55) NOT NULL,
    product_name    VARCHAR(100) NOT NULL,
    category_id     VARCHAR(55) NOT NULL,
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    maintenance     VARCHAR(25),
    cost            DECIMAL(10,2) NOT NULL,
    product_line    VARCHAR(50) NOT NULL,
    start_date      DATE,
    end_date        DATE,
    product_status  VARCHAR(20) NOT NULL
);
GO

-- Crear dim_time
CREATE TABLE gold.dim_time (
    date_key         INT PRIMARY KEY,
    full_date        DATE,       
    day_of_week      TINYINT,       
    day_name         VARCHAR(10),   
    day_of_month     TINYINT,    
    day_of_year      SMALLINT,     
    week_of_year     TINYINT,        
    month_name       VARCHAR(25),   
    month_of_year    TINYINT,      
    quarter          TINYINT,      
    year             SMALLINT,       
    is_weekend       BIT NOT NULL    
);

GO

-- Crear fact_sales con claves foráneas
CREATE TABLE gold.fact_sales (
    fact_sales_key   INT IDENTITY(1,1) PRIMARY KEY,
    order_number     VARCHAR(50) NOT NULL,
    customer_key     INT NOT NULL,
    product_key      INT NOT NULL,
	--- date
    order_date_key   INT NOT NULL,        -- FK a dim_time
    shipping_date_key INT NOT NULL,       -- FK a dim_time
    due_date_key     INT NOT NULL,        -- FK a dim_time
    sales_amount     DECIMAL(12,2) NOT NULL,
    quantity         INT NOT NULL,
    price            DECIMAL(10,2) NOT NULL,
	---auditoria
	audit_is_product_later_version BIT NOT NULL,
    FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key),
    FOREIGN KEY (order_date_key) REFERENCES gold.dim_time(date_key),
    FOREIGN KEY (shipping_date_key) REFERENCES gold.dim_time(date_key),
    FOREIGN KEY (due_date_key) REFERENCES gold.dim_time(date_key)
);
GO


