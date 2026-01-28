/*
================================================================================
SCRIPT PURPOSE: Bronze Layer Table Creation - Data Warehouse ETL Process
================================================================================

DESCRIPTION:
This script creates the foundational Bronze Layer tables for the Data Warehouse,
following the Medallion Architecture pattern (Bronze-Silver-Gold). The Bronze 
layer serves as the raw data landing zone, storing unprocessed data directly 
from source systems without transformation.

ARCHITECTURE LAYER: Bronze (Raw Data)
DATABASE: DatawareHouse
SCHEMA: Bronze

DATA SOURCES:
  1. CRM System (Customer Relationship Management)
     - Customer Information (cust_info.csv)
     - Product Information (prd_info.csv)
     - Sales Details (sales_details.csv)
  
  2. ERP System (Enterprise Resource Planning)
     - Customer Demographics (CUST_AZ12.csv)
     - Location Data (LOC_A101.csv)
     - Product Categories (PX_CAT_G1V2.csv)

TABLES CREATED:
  - Bronze.CRM_Cust_Info       : Customer master data from CRM
  - Bronze.CRM_Prod_Info       : Product master data from CRM
  - Bronze.CRM_sales_details   : Sales transaction details from CRM
  - Bronze.ERP_Cust_az12       : Customer demographics from ERP
  - Bronze.ERP_loc_az101       : Customer location data from ERP
  - Bronze.ERP_px_cat_g1v2     : Product categorization from ERP

EXECUTION NOTES:
  - Tables are dropped and recreated if they already exist (idempotent)
  - Data types preserve source format for complete audit trail
  - No transformations or cleansing applied at this stage
  - These tables will be populated via BULK INSERT operations

NEXT STEPS:
  1. Run Bronze.Bulk_Insert_record procedure to load data
  2. Transform data into Silver layer for cleansing and validation
  3. Create Gold layer dimensional models for analytics

AUTHOR: [Your Name]
CREATED: [Current Date]
VERSION: 1.0
================================================================================
*/

-- Change the workspace to the DatawareHouse database
USE DatawareHouse;
GO

/*
--------------------------------------------------------------------------------
CRM SOURCE TABLES
--------------------------------------------------------------------------------
*/

-- Create the table (Bronze.CRM_Cust_Info)
-- Purpose: Stores raw customer information from CRM system
IF OBJECT_ID('DatawareHouse.Bronze.CRM_Cust_Info', 'U') IS NOT NULL 
    DROP TABLE Bronze.CRM_Cust_Info;

CREATE TABLE Bronze.CRM_Cust_Info (
    cust_id                 INT,
    cust_key                NVARCHAR(50),
    cust_firstname          NVARCHAR(100),
    cust_lastname           NVARCHAR(100),
    cust_maritial_status    NVARCHAR(50),
    cust_gender             NVARCHAR(50),
    cust_created_date       DATE
);
GO

-- Create the table (Bronze.CRM_Prod_Info)
-- Purpose: Stores raw product catalog from CRM system
IF OBJECT_ID('DatawareHouse.Bronze.CRM_Prod_Info', 'U') IS NOT NULL 
    DROP TABLE Bronze.CRM_Prod_Info;

CREATE TABLE Bronze.CRM_Prod_Info (
    prod_id             INT,
    prod_key            NVARCHAR(100),
    prod_name           NVARCHAR(150),
    prod_cost           INT,
    prod_line           NVARCHAR(50),
    prod_start_date     DATE,
    prod_end_date       DATE
);
GO

-- Create the table (Bronze.CRM_sales_details)
-- Purpose: Stores raw sales transactions from CRM system
IF OBJECT_ID('DatawareHouse.Bronze.CRM_sales_details', 'U') IS NOT NULL 
    DROP TABLE Bronze.CRM_sales_details;

CREATE TABLE Bronze.CRM_sales_details (
    sls_ord_num         NVARCHAR(50),
    sls_prd_key         NVARCHAR(50),
    sls_cust_id         INT,
    sls_order_dt        INT,
    sls_ship_dt         INT,
    sls_due_dt          INT,
    sls_sales           INT,
    sls_quantity        INT,
    sls_price           INT
);
GO

/*
--------------------------------------------------------------------------------
ERP SOURCE TABLES
--------------------------------------------------------------------------------
*/

-- Create the table (Bronze.ERP_Cust_az12)
-- Purpose: Stores customer demographic data from ERP system
IF OBJECT_ID('DatawareHouse.Bronze.ERP_Cust_az12', 'U') IS NOT NULL 
    DROP TABLE Bronze.ERP_Cust_az12;

CREATE TABLE Bronze.ERP_Cust_az12 (
    cid     NVARCHAR(50),
    bdate   DATE,
    gen     NVARCHAR(50)
);
GO

-- Create the table (Bronze.ERP_loc_az101)
-- Purpose: Stores customer location/country information from ERP system
IF OBJECT_ID('DatawareHouse.Bronze.ERP_loc_az101', 'U') IS NOT NULL 
    DROP TABLE Bronze.ERP_loc_az101;

CREATE TABLE Bronze.ERP_loc_az101 (
    cid     NVARCHAR(50),
    cntry   NVARCHAR(50)
);
GO

-- Create the table (Bronze.ERP_px_cat_g1v2)
-- Purpose: Stores product category and maintenance data from ERP system
IF OBJECT_ID('DatawareHouse.Bronze.ERP_px_cat_g1v2', 'U') IS NOT NULL 
    DROP TABLE Bronze.ERP_px_cat_g1v2;

CREATE TABLE Bronze.ERP_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50)
);
GO

-- End of Bronze Layer Table Creation Script
PRINT 'Bronze Layer tables created successfully!';
GO
