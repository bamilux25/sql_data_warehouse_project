/*
================================================================================
Script Name:    Gold_Layer_View_Creation.sql
Description:    Creates Gold layer dimension and fact views for analytics
Author:         Ahmed Shittu
Created:        07/02/2026
Database:       DatawareHouse
Schema:         Gold
Version:        1.0
================================================================================

Purpose:
    This script creates the Gold layer views that form a star schema for
    analytics and business intelligence. The Gold layer consolidates data
    from multiple Silver layer tables into denormalized, analysis-ready
    dimension and fact tables.

Gold Layer Components:
    1. dim_customer  - Customer dimension (360-degree customer view)
    2. dim_product   - Product dimension (current products only)
    3. fact_sales    - Sales fact table (transaction grain)

Schema Design:
    Star Schema - Optimized for analytical queries and BI tools
    - Dimensions contain descriptive attributes
    - Facts contain measures and foreign keys to dimensions
    - Surrogate keys (ROW_NUMBER) for dimension relationships

Dependencies:
    - Silver layer tables must exist and be populated
    - Tables required:
        - Sliver.CRM_Cust_Info
        - Sliver.ERP_Cust_az12
        - Sliver.ERP_loc_az101
        - Sliver.CRM_Prod_Info
        - Sliver.ERP_px_cat_g1v2
        - Sliver.CRM_sales_details

Usage:
    Execute this script to create or recreate all Gold layer views.
    Run after Silver layer is loaded via Sliver.Bulk_load procedure.

Notes:
    - Views are dropped and recreated (idempotent script)
    - Data refreshes automatically when queried (no manual refresh needed)
    - For performance, consider materializing as tables in production

================================================================================
*/

USE DatawareHouse;
GO

PRINT '================================================================================';
PRINT 'Starting Gold Layer View Creation';
PRINT 'Execution Time: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '================================================================================';
PRINT '';
GO

/*
================================================================================
VIEW 1: Gold.dim_customer
================================================================================
Purpose:
    Consolidated customer dimension combining CRM customer master data with
    ERP demographic and location information. Provides a complete 360-degree
    view of customer attributes for analytics.

Source Tables:
    - Sliver.CRM_Cust_Info (base table - customer master)
    - Sliver.ERP_Cust_az12 (demographics - birthdate, gender)
    - Sliver.ERP_loc_az101 (location - country)

Business Logic:
    - Joins CRM and ERP data on customer_key = cid
    - LEFT JOINs preserve all CRM customers
    - Gender reconciliation: CRM gender takes priority, ERP as fallback
    - Surrogate key generated via ROW_NUMBER()

Grain:
    One row per customer (cust_id)

Key Features:
    - customer_key: Surrogate key for dimension relationships
    - customer_gender: Reconciled from both CRM and ERP sources
    - Complete demographic profile when ERP data available
================================================================================
*/

PRINT '>> Creating View: Gold.dim_customer';

-- Drop view if exists
IF OBJECT_ID('Gold.dim_customer', 'V') IS NOT NULL 
    DROP VIEW Gold.dim_customer;
GO

-- Create customer dimension view
CREATE VIEW Gold.dim_customer
AS
WITH CTE_JOSH AS (
    -- Consolidate customer data from CRM and ERP sources
    SELECT  
        x.cust_id,
        x.cust_key,
        x.cust_firstname,
        x.cust_lastname,
        x.cust_maritial_status,
        x.cust_gender,
        x.cust_created_date,
        y.bdate,                    -- Birth date from ERP demographics
        y.gen,                      -- Gender from ERP (fallback)
        z.cntry                     -- Country from ERP location
    FROM [Sliver].[CRM_Cust_Info] x
    -- Join demographics (birthdate, gender)
    LEFT JOIN [Sliver].[ERP_Cust_az12] y
        ON y.cid = x.cust_key
    -- Join location data (country)
    LEFT JOIN [Sliver].[ERP_loc_az101] z
        ON z.cid = x.cust_key
)
SELECT
    -- Generate surrogate key for dimension
    ROW_NUMBER() OVER (ORDER BY cust_id) AS customer_key,
    
    -- Business keys and identifiers
    cust_id AS customer_id,
    cust_key AS customer_number,
    
    -- Customer name attributes
    cust_firstname AS customer_firstname,
    cust_lastname AS customer_lastname,
    
    -- Location attribute
    cntry AS country,
    
    -- Demographic attributes
    cust_maritial_status AS maritial_status,
    
    -- Gender reconciliation logic: CRM first, then ERP, default to 'n/a'
    CASE 
        WHEN cust_gender <> 'n/a' THEN cust_gender
        ELSE COALESCE(gen, 'n/a')
    END AS customer_gender,
    
    -- Additional demographic data
    bdate AS customer_birthdate,
    
    -- Account metadata
    cust_created_date AS created_date
FROM CTE_JOSH;
GO

PRINT '   ✓ View created: Gold.dim_customer';
PRINT '';
GO

/*
================================================================================
VIEW 2: Gold.dim_product
================================================================================
Purpose:
    Product dimension containing current/active products with category hierarchy.
    Excludes historical product versions to maintain a clean dimension table.

Source Tables:
    - Sliver.CRM_Prod_Info (base table - product master)
    - Sliver.ERP_px_cat_g1v2 (category hierarchy)

Business Logic:
    - Filters to current products only (prod_end_date IS NULL)
    - Joins category hierarchy from ERP
    - Surrogate key generated via ROW_NUMBER()
    - Ordered by start date and product key for consistency

Grain:
    One row per current/active product

Key Features:
    - product_key: Surrogate key for dimension relationships
    - Only active products (no historical versions)
    - Complete category hierarchy (category, subcategory)
    - Product cost and line classification
================================================================================
*/

PRINT '>> Creating View: Gold.dim_product';

-- Drop view if exists
IF OBJECT_ID('Gold.dim_product', 'V') IS NOT NULL 
    DROP VIEW Gold.dim_product;
GO

-- Create product dimension view
CREATE VIEW Gold.dim_product 
AS
WITH CTE_YUSF AS (
    -- Select current products with category information
    SELECT 
        a.prod_id,
        a.prod_key,
        a.prod_cat_id,
        a.prod_name,
        a.prod_cost,
        a.prod_line,
        a.prod_start_date,
        b.cat,                      -- Category name
        b.subcat,                   -- Subcategory name
        b.maintenance               -- Maintenance indicator
    FROM [Sliver].[CRM_Prod_Info] a
    -- Join category hierarchy
    LEFT JOIN [Sliver].[ERP_px_cat_g1v2] b
        ON a.prod_cat_id = b.id
    -- CRITICAL FILTER: Only current/active products
    WHERE prod_end_date IS NULL 
)
SELECT
    -- Generate surrogate key for dimension (ordered for consistency)
    ROW_NUMBER() OVER (ORDER BY prod_start_date, prod_key) AS product_key,
    
    -- Business keys and identifiers
    prod_id AS product_id,
    prod_key AS product_number,
    
    -- Product attributes
    prod_name AS product_name,
    
    -- Category hierarchy
    prod_cat_id AS category_id,
    cat AS category,
    subcat AS subcategory,
    maintenance,
    
    -- Financial and classification attributes
    prod_cost AS cost,
    prod_line AS prodcut_line,
    
    -- Temporal attribute
    prod_start_date AS start_date_c
FROM CTE_YUSF;
GO

PRINT '   ✓ View created: Gold.dim_product';
PRINT '';
GO

/*
================================================================================
VIEW 3: Gold.fact_sales
================================================================================
Purpose:
    Sales transaction fact table containing all sales orders with references
    to customer and product dimensions. Stores measures (amounts, quantities)
    and foreign keys for analytical reporting.

Source Tables:
    - Sliver.CRM_sales_details (base table - sales transactions)
    - Gold.dim_customer (dimension - for customer_key lookup)
    - Gold.dim_product (dimension - for product_key lookup)

Business Logic:
    - Joins dimension tables to get surrogate keys
    - LEFT JOINs preserve all transactions (may result in NULL keys)
    - No aggregation - transaction grain maintained

Grain:
    One row per sales transaction line item

Key Features:
    - product_key & customer_key: Foreign keys to dimensions
    - sales_amount, quanity, price: Additive/semi-additive measures
    - Date fields for time-based analysis
    - May contain NULL surrogate keys for inactive products

Important Notes:
    - NULL product_key expected for inactive/historical products
    - NULL customer_key indicates data quality issue
    - Use INNER JOIN in queries to exclude orphaned records if needed
================================================================================
*/

PRINT '>> Creating View: Gold.fact_sales';

-- Drop view if exists
IF OBJECT_ID('Gold.fact_sales', 'V') IS NOT NULL 
    DROP VIEW Gold.fact_sales;
GO

-- Create sales fact view
CREATE VIEW Gold.fact_sales 
AS
SELECT
    -- Business key
    cx.sls_ord_num AS order_number,
    
    -- Foreign keys to dimensions (surrogate keys)
    prd.product_key,                -- Links to dim_product
    cst.customer_key,               -- Links to dim_customer
    
    -- Date attributes (for time-based analysis)
    cx.sls_order_dt AS order_date,
    cx.sls_ship_dt AS shipping_date,
    cx.sls_due_dt AS due_date,
    
    -- Measures (facts)
    cx.sls_sales AS sales_amount,   -- Additive measure (revenue)
    cx.sls_quantity AS quanity,     -- Additive measure (units)
    cx.sls_price AS price           -- Semi-additive measure (unit price)
    
FROM [Sliver].[CRM_sales_details] cx

-- Lookup customer surrogate key
LEFT JOIN Gold.dim_customer cst 
    ON cx.sls_cust_id = cst.customer_id

-- Lookup product surrogate key
LEFT JOIN Gold.dim_product prd
    ON cx.sls_prd_key = prd.product_number;
GO

PRINT '   ✓ View created: Gold.fact_sales';
PRINT '';
GO

/*
================================================================================
VALIDATION QUERIES
================================================================================
Run these queries to validate the Gold layer views were created successfully
and contain expected data.
================================================================================
*/

PRINT '================================================================================';
PRINT 'Gold Layer View Creation Completed!';
PRINT '';
PRINT 'Validation Queries:';
PRINT '--------------------------------------------------------------------------------';
PRINT '';
PRINT '-- Check view row counts';
PRINT 'SELECT ''dim_customer'' AS view_name, COUNT(*) AS row_count FROM Gold.dim_customer';
PRINT 'UNION ALL';
PRINT 'SELECT ''dim_product'', COUNT(*) FROM Gold.dim_product';
PRINT 'UNION ALL';
PRINT 'SELECT ''fact_sales'', COUNT(*) FROM Gold.fact_sales;';
PRINT '';
PRINT '-- Check for NULL surrogate keys in fact table';
PRINT 'SELECT ';
PRINT '    COUNT(*) AS total_transactions,';
PRINT '    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_keys,';
PRINT '    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS null_product_keys';
PRINT 'FROM Gold.fact_sales;';
PRINT '';
PRINT '-- Sample customer dimension data';
PRINT 'SELECT TOP 10 * FROM Gold.dim_customer ORDER BY customer_key;';
PRINT '';
PRINT '-- Sample product dimension data';
PRINT 'SELECT TOP 10 * FROM Gold.dim_product ORDER BY product_key;';
PRINT '';
PRINT '-- Sample fact table data';
PRINT 'SELECT TOP 10 * FROM Gold.fact_sales ORDER BY order_date DESC;';
PRINT '';
PRINT '================================================================================';
PRINT 'Completion Time: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '================================================================================';
GO

/*
================================================================================
USAGE EXAMPLES
================================================================================

-- Example 1: Sales by Customer
--------------------------------------------------------------------------------
SELECT 
    c.customer_firstname + ' ' + c.customer_lastname AS customer_name,
    c.country,
    SUM(f.sales_amount) AS total_revenue,
    SUM(f.quanity) AS total_units,
    COUNT(DISTINCT f.order_number) AS order_count
FROM Gold.fact_sales f
INNER JOIN Gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_firstname, c.customer_lastname, c.country
ORDER BY total_revenue DESC;

-- Example 2: Sales by Product Category
--------------------------------------------------------------------------------
SELECT 
    p.category,
    p.subcategory,
    p.prodcut_line,
    SUM(f.sales_amount) AS total_revenue,
    SUM(f.quanity) AS units_sold,
    COUNT(DISTINCT f.order_number) AS order_count,
    AVG(f.price) AS avg_price
FROM Gold.fact_sales f
INNER JOIN Gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory, p.prodcut_line
ORDER BY total_revenue DESC;

-- Example 3: Monthly Sales Trend
--------------------------------------------------------------------------------
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(sales_amount) AS monthly_revenue,
    SUM(quanity) AS monthly_units,
    COUNT(DISTINCT order_number) AS order_count,
    AVG(sales_amount) AS avg_order_value
FROM Gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- Example 4: Top Customers by Country
--------------------------------------------------------------------------------
SELECT 
    c.country,
    c.customer_firstname + ' ' + c.customer_lastname AS customer_name,
    SUM(f.sales_amount) AS total_spent,
    COUNT(DISTINCT f.order_number) AS purchase_count
FROM Gold.fact_sales f
INNER JOIN Gold.dim_customer c ON f.customer_key = c.customer_key
WHERE c.country <> 'n/a'
GROUP BY c.country, c.customer_firstname, c.customer_lastname
ORDER BY c.country, total_spent DESC;

-- Example 5: Product Performance Analysis
--------------------------------------------------------------------------------
SELECT 
    p.product_name,
    p.category,
    p.prodcut_line,
    SUM(f.sales_amount) AS total_revenue,
    SUM(f.quanity) AS units_sold,
    SUM(f.sales_amount) / NULLIF(SUM(f.quanity), 0) AS avg_unit_price,
    p.cost,
    SUM(f.sales_amount) - (SUM(f.quanity) * p.cost) AS gross_profit
FROM Gold.fact_sales f
INNER JOIN Gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.prodcut_line, p.cost
ORDER BY total_revenue DESC;

================================================================================
*/

/*
================================================================================
End of Script
================================================================================

Maintenance Log:
--------------------------------------------------------------------------------
Date         Author          Version    Description
--------------------------------------------------------------------------------
07/02/2026      Ahmed Shittu     1.0        Initial Gold layer view creation
--------------------------------------------------------------------------------

Next Steps:
1. Run validation queries to verify data
2. Check for NULL surrogate keys in fact table
3. Create indexes on dimension keys (if materializing)
4. Connect to BI tools (Power BI, Tableau, etc.)
5. Build analytical dashboards and reports

For Questions or Issues:
- Technical Owner: [Data Engineering Team]
- Business Owner: [Analytics/BI Team]
- Documentation: See Data_Catalog_Gold_Layer.sql

================================================================================
*/
