/*
================================================================================
Script Name:    Silver_Layer_Table_Creation.sql
Description:    Creates Silver layer tables for the Data Warehouse ETL pipeline.
                Silver layer contains cleaned and standardized data from Bronze layer.
Author:         [Ahmed Shittu]
Created:        [05/02/2026]
Database:       DatawareHouse
Schema:         Sliver
Version:        1.0
================================================================================

Purpose:
    This script creates the Silver layer tables that store transformed and 
    cleansed data from various source systems (CRM and ERP). The Silver layer
    serves as an intermediate staging area where data quality issues are 
    resolved before loading into the Gold layer.

Tables Created:
    1. Sliver.CRM_Cust_Info      - Customer information from CRM system
    2. Sliver.CRM_Prod_Info      - Product catalog from CRM system
    3. Sliver.CRM_sales_details  - Sales transaction details from CRM
    4. Sliver.ERP_Cust_az12      - Customer demographics from ERP system
    5. Sliver.ERP_loc_az101      - Customer location data from ERP
    6. Sliver.ERP_px_cat_g1v2    - Product category hierarchy from ERP

Dependencies:
    - Database: DatawareHouse must exist
    - Schema: Sliver schema must exist
    - Bronze layer tables must be populated

Usage:
    Execute this script to create or recreate all Silver layer tables.
    Note: DROP IF EXISTS will remove existing tables and all data.

================================================================================
*/

-- Set the database context to DatawareHouse
USE DatawareHouse;
GO

PRINT '================================================================================';
PRINT 'Starting Silver Layer Table Creation';
PRINT 'Execution Time: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '================================================================================';
PRINT '';
GO

/*
--------------------------------------------------------------------------------
Table: Sliver.CRM_Cust_Info
Description: Stores cleaned and standardized customer information from CRM system
Source: Bronze.CRM_Cust_Info
Transformations Applied:
    - Deduplication by cust_id
    - Name field trimming
    - Gender and marital status standardization
    - Invalid record filtering
--------------------------------------------------------------------------------
*/
PRINT 'Creating table: Sliver.CRM_Cust_Info';

IF OBJECT_ID('DatawareHouse.Sliver.CRM_Cust_Info', 'u') IS NOT NULL 
    DROP TABLE Sliver.CRM_Cust_Info;

CREATE TABLE Sliver.CRM_Cust_Info (
    cust_id                 INT,                    -- Customer unique identifier
    cust_key                NVARCHAR(50),           -- Customer business key
    cust_firstname          NVARCHAR(100),          -- Customer first name (trimmed)
    cust_lastname           NVARCHAR(100),          -- Customer last name (trimmed)
    cust_maritial_status    NVARCHAR(50),           -- Marital status (standardized: Married/Single/n/a)
    cust_gender             NVARCHAR(50),           -- Gender (standardized: Male/Female/n/a)
    cust_created_date       DATE,                   -- Customer account creation date
    dwh_sliver_createdt     DATETIME2(7) DEFAULT GETDATE()  -- Record creation timestamp in Silver layer
);

PRINT '   ✓ Table created successfully';
PRINT '';
GO

/*
--------------------------------------------------------------------------------
Table: Sliver.CRM_Prod_Info
Description: Product catalog with derived attributes and standardized values
Source: Bronze.CRM_Prod_Info
Transformations Applied:
    - Product category ID derivation from prod_key
    - Product key normalization
    - Product line standardization
    - End date calculation using LEAD function
    - Missing cost handling (defaulted to 0)
--------------------------------------------------------------------------------
*/
PRINT 'Creating table: Sliver.CRM_Prod_Info';

IF OBJECT_ID('DatawareHouse.Sliver.CRM_Prod_Info', 'u') IS NOT NULL 
    DROP TABLE Sliver.CRM_Prod_Info;

CREATE TABLE Sliver.CRM_Prod_Info (
    prod_id                 INT,                    -- Product unique identifier
    prod_key                NVARCHAR(100),          -- Product business key (normalized)
    prod_cat_id             NVARCHAR(100),          -- Product category ID (derived from prod_key)
    prod_name               NVARCHAR(150),          -- Product name
    prod_cost               INT,                    -- Product cost (nulls replaced with 0)
    prod_line               NVARCHAR(50),           -- Product line (standardized: Mountain/Road/Touring/Other Sales/n/a)
    prod_start_date         DATE,                   -- Product effective start date
    prod_end_date           DATE,                   -- Product effective end date (calculated)
    dwh_sliver_createdt     DATETIME2(7) DEFAULT GETDATE()  -- Record creation timestamp in Silver layer
);

PRINT '   ✓ Table created successfully';
PRINT '';
GO

/*
--------------------------------------------------------------------------------
Table: Sliver.CRM_sales_details
Description: Sales transaction details with validated dates and calculated fields
Source: Bronze.CRM_sales_details
Transformations Applied:
    - Invalid date handling (0 or incorrect length dates set to NULL)
    - Date format conversion (YYYYMMDD integer to DATE)
    - Sales amount calculation from quantity and price when missing
    - Price derivation from sales and quantity when missing
    - Negative price correction using ABS function
--------------------------------------------------------------------------------
*/
PRINT 'Creating table: Sliver.CRM_sales_details';

IF OBJECT_ID('DatawareHouse.Sliver.CRM_sales_details', 'u') IS NOT NULL 
    DROP TABLE Sliver.CRM_sales_details;

CREATE TABLE Sliver.CRM_sales_details (
    sls_ord_num             NVARCHAR(50),           -- Sales order number
    sls_prd_key             NVARCHAR(50),           -- Product key reference
    sls_cust_id             INT,                    -- Customer ID reference
    sls_order_dt            DATE,                   -- Order date (validated and converted)
    sls_ship_dt             DATE,                   -- Ship date (validated and converted)
    sls_due_dt              DATE,                   -- Due date (validated and converted)
    sls_sales               INT,                    -- Total sales amount (calculated if missing)
    sls_quantity            INT,                    -- Quantity ordered
    sls_price               INT,                    -- Unit price (calculated if missing, absolute value)
    dwh_sliver_createdt     DATETIME2(7) DEFAULT GETDATE()  -- Record creation timestamp in Silver layer
);

PRINT '   ✓ Table created successfully';
PRINT '';
GO

/*
--------------------------------------------------------------------------------
Table: Sliver.ERP_Cust_az12
Description: Customer demographic data from ERP system
Source: Bronze.ERP_Cust_az12
Transformations Applied:
    - Removal of 'NAS' prefix from customer IDs
    - Future birthdate validation (set to NULL if > current date)
    - Gender standardization (Male/Female/n/a)
--------------------------------------------------------------------------------
*/
PRINT 'Creating table: Sliver.ERP_Cust_az12';

IF OBJECT_ID('DatawareHouse.Sliver.ERP_Cust_az12', 'u') IS NOT NULL 
    DROP TABLE Sliver.ERP_Cust_az12;

CREATE TABLE Sliver.ERP_Cust_az12 (
    cid                     NVARCHAR(50),           -- Customer ID (cleaned from invalid prefixes)
    bdate                   DATE,                   -- Birth date (validated for future dates)
    gen                     NVARCHAR(50),           -- Gender (standardized: Male/Female/n/a)
    dwh_sliver_createdt     DATETIME2(7) DEFAULT GETDATE()  -- Record creation timestamp in Silver layer
);

PRINT '   ✓ Table created successfully';
PRINT '';
GO

/*
--------------------------------------------------------------------------------
Table: Sliver.ERP_loc_az101
Description: Customer location and country information from ERP
Source: Bronze.ERP_loc_az101
Transformations Applied:
    - Customer ID hyphen removal for consistency
    - Country code standardization (US/USA → United State, DE → Germany)
    - Empty/NULL country handling (set to 'n/a')
--------------------------------------------------------------------------------
*/
PRINT 'Creating table: Sliver.ERP_loc_az101';

IF OBJECT_ID('DatawareHouse.Sliver.ERP_loc_az101', 'u') IS NOT NULL 
    DROP TABLE Sliver.ERP_loc_az101;

CREATE TABLE Sliver.ERP_loc_az101 (
    cid                     NVARCHAR(50),           -- Customer ID (normalized, hyphens removed)
    cntry                   NVARCHAR(50),           -- Country (standardized country names)
    dwh_sliver_createdt     DATETIME2(7) DEFAULT GETDATE()  -- Record creation timestamp in Silver layer
);

PRINT '   ✓ Table created successfully';
PRINT '';
GO

/*
--------------------------------------------------------------------------------
Table: Sliver.ERP_px_cat_g1v2
Description: Product category hierarchy from ERP system
Source: Bronze.ERP_px_cat_g1v2
Transformations Applied:
    - Direct pass-through (no transformations required)
    - Data quality is acceptable from source
--------------------------------------------------------------------------------
*/
PRINT 'Creating table: Sliver.ERP_px_cat_g1v2';

IF OBJECT_ID('DatawareHouse.Sliver.ERP_px_cat_g1v2', 'u') IS NOT NULL 
    DROP TABLE Sliver.ERP_px_cat_g1v2;

CREATE TABLE Sliver.ERP_px_cat_g1v2 (
    id                      NVARCHAR(50),           -- Category ID
    cat                     NVARCHAR(50),           -- Category name
    subcat                  NVARCHAR(50),           -- Subcategory name
    maintenance             NVARCHAR(50),           -- Maintenance flag/indicator
    dwh_sliver_createdt     DATETIME2(7) DEFAULT GETDATE()  -- Record creation timestamp in Silver layer
);

PRINT '   ✓ Table created successfully';
PRINT '';
GO

PRINT '================================================================================';
PRINT 'Silver Layer Table Creation Completed Successfully!';
PRINT 'Total Tables Created: 6';
PRINT 'Completion Time: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '================================================================================';
GO

/*
================================================================================
End of Script
================================================================================
*/
