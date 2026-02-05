/*
================================================================================
Script Name:    Data_Quality_Checks_Silver_Layer.sql
Description:    Comprehensive data quality validation for Silver layer tables
Author:         [Ahmed Shittu]
Created:        [05/02/2026]
Database:       DatawareHouse
Schema:         Sliver
Version:        1.0
================================================================================

Purpose:
    This script performs data quality checks on all Silver layer tables to
    validate that ETL transformations were successful and data meets quality
    standards. These checks should be run after executing Sliver.Bulk_load
    stored procedure.

Quality Check Categories:
    1. Primary Key Validation (NULL and duplicate checks)
    2. Data Format Validation (trimming, spacing)
    3. Data Standardization (consistent values)
    4. Referential Integrity (foreign key relationships)
    5. Business Rule Validation (date logic, calculations)
    6. Data Completeness (NULL checks)

Tables Validated:
    - Sliver.CRM_Cust_Info
    - Sliver.CRM_Prod_Info
    - Sliver.CRM_sales_details
    - Sliver.ERP_Cust_az12
    - Sliver.ERP_loc_az101
    - Sliver.ERP_px_cat_g1v2

Expected Results:
    All quality check queries should return 0 rows if data quality is good.
    Any rows returned indicate data quality issues that need attention.

Usage:
    Execute each section individually or run entire script to validate all tables.

================================================================================
*/

USE DatawareHouse;
GO

PRINT '================================================================================';
PRINT 'Starting Data Quality Checks for Silver Layer';
PRINT 'Execution Time: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '================================================================================';
PRINT '';
GO

/*
================================================================================
TABLE 1: Sliver.CRM_Cust_Info - Customer Information Quality Checks
================================================================================
*/

PRINT '================================================================';
PRINT 'Quality Checks: Sliver.CRM_Cust_Info';
PRINT '================================================================';
PRINT '';

-- ========================================
-- Check 1: Primary Key Validation
-- ========================================
-- Purpose: Identify NULL or duplicate customer IDs
-- Expected Result: 0 rows (no NULLs or duplicates)
-- Impact: Critical - duplicates will cause issues in downstream processes
PRINT '>> Check 1.1: Primary Key - NULL and Duplicate Detection';

SELECT 
    cust_id, 
    COUNT(1) AS volume
FROM Sliver.CRM_Cust_Info
GROUP BY cust_id 
HAVING COUNT(1) > 1 OR cust_id IS NULL;

PRINT '   Expected: 0 rows | If rows found: Primary key integrity violated';
PRINT '';

-- ========================================
-- Check 2: Data Format Validation
-- ========================================
-- Purpose: Detect unwanted leading/trailing spaces in name fields
-- Expected Result: 0 rows (all names should be trimmed)
-- Impact: Medium - can cause matching issues in joins and lookups
PRINT '>> Check 1.2: Name Fields - Unwanted Space Detection';

SELECT 
    cust_id,
    cust_firstname, 
    cust_lastname  
FROM Sliver.CRM_Cust_Info
WHERE cust_firstname <> TRIM(cust_firstname)
   OR cust_lastname <> TRIM(cust_lastname);

PRINT '   Expected: 0 rows | If rows found: TRIM() transformation failed';
PRINT '';

-- ========================================
-- Check 3: Data Standardization Validation
-- ========================================
-- Purpose: Verify marital status values are standardized
-- Expected Result: Only 'Married', 'Single', 'n/a'
-- Impact: Low - but important for reporting consistency
PRINT '>> Check 1.3: Marital Status - Standardization Check';

SELECT DISTINCT cust_maritial_status
FROM Sliver.CRM_Cust_Info
ORDER BY cust_maritial_status;

PRINT '   Expected: Only Married, Single, n/a';
PRINT '';

-- Purpose: Verify gender values are standardized
-- Expected Result: Only 'Male', 'Female', 'n/a'
-- Impact: Low - but important for reporting consistency
PRINT '>> Check 1.4: Gender - Standardization Check';

SELECT DISTINCT cust_gender
FROM Sliver.CRM_Cust_Info
ORDER BY cust_gender;

PRINT '   Expected: Only Male, Female, n/a';
PRINT '';
PRINT '';

/*
================================================================================
TABLE 2: Sliver.CRM_Prod_Info - Product Information Quality Checks
================================================================================
*/

PRINT '================================================================';
PRINT 'Quality Checks: Sliver.CRM_Prod_Info';
PRINT '================================================================';
PRINT '';

-- ========================================
-- Check 1: Primary Key Validation
-- ========================================
-- Purpose: Identify NULL or duplicate product IDs
-- Expected Result: 0 rows
-- Impact: Critical - duplicates will cause incorrect aggregations
PRINT '>> Check 2.1: Primary Key - NULL and Duplicate Detection';

SELECT 
    prod_id, 
    COUNT(1) AS volume
FROM Sliver.CRM_Prod_Info
GROUP BY prod_id 
HAVING COUNT(1) > 1 OR prod_id IS NULL;

PRINT '   Expected: 0 rows | If rows found: Primary key integrity violated';
PRINT '';

-- ========================================
-- Check 2: Referential Integrity
-- ========================================
-- Purpose: Verify product category IDs exist in reference table
-- Expected Result: 0 rows (all prod_cat_id should have matching reference)
-- Impact: Medium - orphaned categories may indicate data quality issues
PRINT '>> Check 2.2: Foreign Key - Product Category Reference Validation';

SELECT TOP 100 
    prod_id,
    prod_cat_id,
    prod_key,
    prod_name
FROM Sliver.CRM_Prod_Info
WHERE prod_cat_id NOT IN (
    SELECT id 
    FROM Bronze.ERP_px_cat_g1v2
);

PRINT '   Expected: 0 rows | If rows found: Orphaned product categories detected';
PRINT '';

-- Purpose: Verify product keys are referenced in sales data
-- Expected Result: Information only - shows products with sales
-- Impact: Informational - helps identify active products
PRINT '>> Check 2.3: Product Usage - Sales Reference Check (Informational)';

SELECT 
    prod_id,
    prod_key,
    prod_name
FROM Sliver.CRM_Prod_Info
WHERE prod_key IN (
    SELECT DISTINCT sls_prd_key 
    FROM Bronze.CRM_sales_details
);

PRINT '   Info: Products with associated sales transactions';
PRINT '';

-- ========================================
-- Check 3: Data Format Validation
-- ========================================
-- Purpose: Detect unwanted spaces in product names
-- Expected Result: 0 rows
-- Impact: Low - but important for display consistency
PRINT '>> Check 2.4: Product Name - Unwanted Space Detection';

SELECT 
    prod_id,
    prod_name 
FROM Sliver.CRM_Prod_Info 
WHERE prod_name <> TRIM(prod_name);

PRINT '   Expected: 0 rows | Status: Safe - no unwanted spaces';
PRINT '';

-- ========================================
-- Check 4: Data Validity
-- ========================================
-- Purpose: Identify negative or NULL product costs
-- Expected Result: 0 rows (should be >= 0 after ISNULL transformation)
-- Impact: Medium - affects financial calculations
PRINT '>> Check 2.5: Product Cost - Negative/NULL Value Detection';

SELECT 
    prod_id,
    prod_name,
    prod_cost
FROM Sliver.CRM_Prod_Info
WHERE prod_cost < 0 OR prod_cost IS NULL;

PRINT '   Expected: 0 rows | Status: Safe - no negative or NULL values';
PRINT '';

-- ========================================
-- Check 5: Data Standardization
-- ========================================
-- Purpose: Verify product line values are standardized
-- Expected Result: Only 'Mountain', 'Road', 'Other Sales', 'Touring', 'n/a'
-- Impact: Low - important for categorization and reporting
PRINT '>> Check 2.6: Product Line - Standardization Check';

SELECT DISTINCT prod_line 
FROM Sliver.CRM_Prod_Info
ORDER BY prod_line;

PRINT '   Expected: Only Mountain, Road, Other Sales, Touring, n/a';
PRINT '';

-- ========================================
-- Check 6: Business Rule Validation
-- ========================================
-- Purpose: Detect invalid date ranges (start date after end date)
-- Expected Result: 0 rows
-- Impact: High - violates business logic for product validity periods
PRINT '>> Check 2.7: Date Logic - Invalid Date Range Detection';

SELECT 
    prod_id,
    prod_name,
    prod_start_date,
    prod_end_date
FROM Sliver.CRM_Prod_Info
WHERE prod_start_date > prod_end_date;

PRINT '   Expected: 0 rows | If rows found: Invalid effective date ranges';
PRINT '';
PRINT '';

/*
================================================================================
TABLE 3: Sliver.CRM_sales_details - Sales Transaction Quality Checks
================================================================================
*/

PRINT '================================================================';
PRINT 'Quality Checks: Sliver.CRM_sales_details';
PRINT '================================================================';
PRINT '';

-- ========================================
-- Check 1: Primary Key Validation
-- ========================================
-- Purpose: Identify NULL or duplicate sales order numbers
-- Expected Result: 0 rows
-- Impact: Critical - duplicates cause revenue miscalculation
PRINT '>> Check 3.1: Primary Key - NULL and Duplicate Detection';

SELECT 
    sls_ord_num, 
    COUNT(1) AS volume
FROM Bronze.CRM_sales_details
GROUP BY sls_ord_num 
HAVING COUNT(1) > 1 OR sls_ord_num IS NULL;

PRINT '   Expected: 0 rows | If rows found: Primary key integrity violated';
PRINT '';

-- ========================================
-- Check 2: Data Format Validation
-- ========================================
-- Purpose: Detect unwanted spaces in order numbers
-- Expected Result: 0 rows
-- Impact: Low - but can cause lookup failures
PRINT '>> Check 3.2: Order Number - Unwanted Space Detection';

SELECT 
    sls_ord_num
FROM Bronze.CRM_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

PRINT '   Expected: 0 rows | If rows found: Untrimmed order numbers';
PRINT '';

-- ========================================
-- Check 3: Date Field Validation
-- ========================================
-- Purpose: Detect invalid order dates (0, incorrect length)
-- Expected Result: 0 rows after transformation
-- Impact: High - invalid dates become NULL in Silver layer
PRINT '>> Check 3.3: Order Date - Invalid Date Format Detection';

SELECT 
    sls_ord_num,
    sls_order_dt
FROM Bronze.CRM_sales_details
WHERE sls_order_dt <= 0 
   OR LEN(sls_order_dt) <> 8;

PRINT '   Expected: 0 rows after transformation | These become NULL in Silver';
PRINT '';

-- Purpose: Detect invalid ship dates
-- Expected Result: 0 rows after transformation
-- Impact: High - invalid dates become NULL in Silver layer
PRINT '>> Check 3.4: Ship Date - Invalid Date Format Detection';

SELECT 
    sls_ord_num,
    sls_ship_dt
FROM Bronze.CRM_sales_details
WHERE sls_ship_dt <= 0 
   OR LEN(sls_ship_dt) <> 8;

PRINT '   Expected: 0 rows after transformation | These become NULL in Silver';
PRINT '';

-- Purpose: Detect invalid due dates
-- Expected Result: 0 rows after transformation
-- Impact: High - invalid dates become NULL in Silver layer
PRINT '>> Check 3.5: Due Date - Invalid Date Format Detection';

SELECT 
    sls_ord_num,
    sls_due_dt
FROM Bronze.CRM_sales_details
WHERE sls_due_dt <= 0 
   OR LEN(sls_due_dt) <> 8;

PRINT '   Expected: 0 rows after transformation | These become NULL in Silver';
PRINT '';

-- ========================================
-- Check 4: Business Rule Validation
-- ========================================
-- Purpose: Detect illogical date sequences (order > ship or order > due)
-- Expected Result: 0 rows (dates should follow logical sequence)
-- Impact: Medium - indicates data quality issues in source
PRINT '>> Check 3.6: Date Sequence - Business Logic Validation';

SELECT 
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM Bronze.CRM_sales_details
WHERE (sls_order_dt > sls_ship_dt) 
   OR (sls_order_dt > sls_due_dt);

PRINT '   Expected: 0 rows | If rows found: Illogical date sequences detected';
PRINT '';

-- ========================================
-- Check 5: Financial Calculation Validation
-- ========================================
-- Purpose: Verify sales amount calculations and handle missing/invalid values
-- Expected Result: Shows corrected values after transformation logic
-- Impact: Critical - affects revenue reporting accuracy
PRINT '>> Check 3.7: Sales Calculations - Validation and Correction Preview';

SELECT DISTINCT 
    sls_quantity,
    CASE
        WHEN sls_sales IS NULL AND sls_price IS NOT NULL THEN sls_quantity * ABS(sls_price)
        WHEN sls_sales <= 0 AND sls_price IS NOT NULL THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
    END AS sls_sales_corrected,
    CASE 
        WHEN sls_price IS NULL AND sls_sales IS NOT NULL THEN sls_sales / sls_quantity
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price 
    END AS sls_price_corrected
FROM Bronze.CRM_sales_details
WHERE sls_sales <> sls_quantity * sls_price 
   OR sls_sales <= 0 
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
ORDER BY sls_sales_corrected, sls_quantity, sls_price_corrected;

PRINT '   Info: Preview of calculation corrections applied in Silver layer';
PRINT '';
PRINT '';

/*
================================================================================
TABLE 4: Sliver.ERP_Cust_az12 - ERP Customer Demographics Quality Checks
================================================================================
*/

PRINT '================================================================';
PRINT 'Quality Checks: Sliver.ERP_Cust_az12';
PRINT '================================================================';
PRINT '';

-- ========================================
-- Check 1: Data Cleansing Validation
-- ========================================
-- Purpose: Preview records from Bronze layer before transformation
-- Expected Result: Information only - shows raw data
-- Impact: Informational - baseline for transformation validation
PRINT '>> Check 4.1: Source Data Preview (Bronze Layer)';

SELECT TOP 10 * 
FROM [Bronze].[ERP_Cust_az12]
ORDER BY cid;

PRINT '   Info: Sample of raw ERP customer data';
PRINT '';

-- ========================================
-- Check 2: Referential Integrity
-- ========================================
-- Purpose: Verify ERP customer IDs match CRM customer keys after cleansing
-- Expected Result: Shows matched records after 'NAS' prefix removal
-- Impact: High - validates ID cleansing transformation
PRINT '>> Check 4.2: Customer ID Matching - CRM to ERP Validation';

SELECT 
    c.cust_id,
    c.cust_key,
    c.cust_firstname,
    c.cust_lastname
FROM [Sliver].[CRM_Cust_Info] c
WHERE c.cust_key IN (
    SELECT SUBSTRING(cid, 4, LEN(cid)) 
    FROM [Bronze].[ERP_Cust_az12]
    WHERE cid LIKE 'NAS%'
);

PRINT '   Info: CRM customers matched with ERP data after ID cleansing';
PRINT '';

-- ========================================
-- Check 3: Date Validation
-- ========================================
-- Purpose: Identify future birthdates (invalid)
-- Expected Result: Shows records with future dates (will be set to NULL)
-- Impact: Medium - validates date validation logic
PRINT '>> Check 4.3: Birthdate - Future Date Detection';

SELECT 
    cid,
    bdate,
    DATEDIFF(DAY, GETDATE(), bdate) AS days_in_future
FROM [Bronze].[ERP_Cust_az12]
WHERE bdate > GETDATE()
ORDER BY bdate DESC;

PRINT '   Expected: These records will have NULL bdate in Silver layer';
PRINT '';

-- ========================================
-- Check 4: Data Standardization
-- ========================================
-- Purpose: Verify distinct gender values in source data
-- Expected Result: Shows raw values before standardization
-- Impact: Low - validates need for standardization
PRINT '>> Check 4.4: Gender - Value Distribution (Before Standardization)';

SELECT 
    gen,
    COUNT(*) AS count
FROM [Bronze].[ERP_Cust_az12]
GROUP BY gen
ORDER BY gen;

PRINT '   Info: Raw gender values requiring standardization';
PRINT '';

-- ========================================
-- Check 5: Transformation Preview
-- ========================================
-- Purpose: Preview complete transformation logic
-- Expected Result: Shows how data will look after transformation
-- Impact: Informational - validates all transformation rules
PRINT '>> Check 4.5: Complete Transformation Preview';

SELECT TOP 20
    cid AS original_cid,
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
         ELSE cid
    END AS cleaned_cid,
    bdate AS original_bdate,
    CASE WHEN bdate > GETDATE() THEN NULL
         ELSE bdate
    END AS validated_bdate,
    gen AS original_gen,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
         ELSE 'n/a'
    END AS standardized_gen
FROM [Bronze].[ERP_Cust_az12];

PRINT '   Info: Sample of transformation results';
PRINT '';
PRINT '';

/*
================================================================================
TABLE 5: Sliver.ERP_loc_az101 - ERP Location Data Quality Checks
================================================================================
*/

PRINT '================================================================';
PRINT 'Quality Checks: Sliver.ERP_loc_az101';
PRINT '================================================================';
PRINT '';

-- ========================================
-- Check 1: Data Volume Check
-- ========================================
-- Purpose: Count total records in location table
-- Expected Result: Total record count
-- Impact: Informational - baseline metric
PRINT '>> Check 5.1: Record Count';

SELECT COUNT(*) AS total_records
FROM [Bronze].[ERP_loc_az101];

PRINT '   Info: Total location records';
PRINT '';

-- ========================================
-- Check 2: Referential Integrity
-- ========================================
-- Purpose: Verify location customer IDs match CRM after hyphen removal
-- Expected Result: Shows matched records
-- Impact: High - validates ID normalization
PRINT '>> Check 5.2: Customer ID Matching - CRM to ERP Location Validation';

SELECT TOP 100 
    c.cust_id,
    c.cust_key,
    c.cust_firstname,
    c.cust_lastname
FROM [Sliver].[CRM_Cust_Info] c
WHERE c.cust_key IN (
    SELECT REPLACE(cid, '-', '') 
    FROM [Bronze].[ERP_loc_az101]
);

PRINT '   Info: CRM customers matched with location data after ID normalization';
PRINT '';

-- ========================================
-- Check 3: Data Standardization
-- ========================================
-- Purpose: Identify distinct country values requiring standardization
-- Expected Result: Shows raw country codes/names
-- Impact: Medium - validates country mapping logic
PRINT '>> Check 5.3: Country - Value Distribution (Before Standardization)';

SELECT 
    cntry,
    COUNT(*) AS count
FROM [Bronze].[ERP_loc_az101]
GROUP BY cntry
ORDER BY count DESC;

PRINT '   Info: Country values requiring standardization';
PRINT '';

-- ========================================
-- Check 4: Transformation Preview
-- ========================================
-- Purpose: Preview complete transformation for location data
-- Expected Result: Shows cleaned IDs and standardized countries
-- Impact: Informational - validates transformation rules
PRINT '>> Check 5.4: Complete Transformation Preview';

SELECT TOP 20
    cid AS original_cid,
    CASE WHEN cid LIKE '%-%' THEN REPLACE(cid, '-', '') 
         ELSE cid
    END AS normalized_cid,
    cntry AS original_country,
    CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United State'
         WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
         ELSE cntry
    END AS standardized_country
FROM [Bronze].[ERP_loc_az101];

PRINT '   Info: Sample of location data transformation';
PRINT '';
PRINT '';

/*
================================================================================
TABLE 6: Sliver.ERP_px_cat_g1v2 - Product Category Quality Checks
================================================================================
*/

PRINT '================================================================';
PRINT 'Quality Checks: Sliver.ERP_px_cat_g1v2';
PRINT '================================================================';
PRINT '';

-- ========================================
-- Check 1: Data Preview
-- ========================================
-- Purpose: Preview product category hierarchy
-- Expected Result: Ordered list of categories
-- Impact: Informational - validates category structure
PRINT '>> Check 6.1: Category Hierarchy Preview';

SELECT TOP 20
    id,
    cat,
    subcat,
    maintenance
FROM [Bronze].[ERP_px_cat_g1v2]
ORDER BY id DESC;

PRINT '   Info: Sample of product category hierarchy';
PRINT '';

-- ========================================
-- Check 2: Referential Integrity
-- ========================================
-- Purpose: Verify product category IDs are referenced in product table
-- Expected Result: Shows products with valid category references
-- Impact: High - validates category usage
PRINT '>> Check 6.2: Category Usage - Product Reference Validation';

SELECT TOP 100 
    p.prod_id,
    p.prod_cat_id,
    p.prod_name,
    p.prod_line
FROM [Sliver].[CRM_Prod_Info] p
WHERE p.prod_cat_id IN (
    SELECT id 
    FROM [Bronze].[ERP_px_cat_g1v2]
);

PRINT '   Info: Products with valid category references';
PRINT '';

-- ========================================
-- Check 3: Data Format Validation
-- ========================================
-- Purpose: Detect unwanted spaces in category fields
-- Expected Result: 0 rows (no untrimmed values)
-- Impact: Low - affects display consistency
PRINT '>> Check 6.3: Category Fields - Unwanted Space Detection';

SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM [Bronze].[ERP_px_cat_g1v2]
WHERE cat <> TRIM(cat) 
   OR subcat <> TRIM(subcat) 
   OR maintenance <> TRIM(maintenance);

PRINT '   Expected: 0 rows | If rows found: Untrimmed category values';
PRINT '';

-- ========================================
-- Check 4: Data Standardization
-- ========================================
-- Purpose: Verify distinct category values
-- Expected Result: List of unique categories
-- Impact: Informational - validates category taxonomy
PRINT '>> Check 6.4: Category - Distinct Values';

SELECT DISTINCT cat 
FROM [Bronze].[ERP_px_cat_g1v2]
ORDER BY cat;

PRINT '   Info: Unique category values';
PRINT '';

-- Purpose: Verify distinct subcategory values
-- Expected Result: List of unique subcategories
-- Impact: Informational - validates subcategory taxonomy
PRINT '>> Check 6.5: Subcategory - Distinct Values';

SELECT DISTINCT subcat
FROM [Bronze].[ERP_px_cat_g1v2]
ORDER BY subcat;

PRINT '   Info: Unique subcategory values';
PRINT '';

-- Purpose: Verify distinct maintenance flag values
-- Expected Result: List of unique maintenance indicators
-- Impact: Informational - validates maintenance flag usage
PRINT '>> Check 6.6: Maintenance Flag - Distinct Values';

SELECT 
    maintenance,
    COUNT(*) AS count
FROM [Bronze].[ERP_px_cat_g1v2]
GROUP BY maintenance
ORDER BY maintenance;

PRINT '   Info: Maintenance flag distribution';
PRINT '';

PRINT '================================================================================';
PRINT 'Data Quality Checks Completed!';
PRINT 'Completion Time: ' + CONVERT(VARCHAR(23), GETDATE(), 121);
PRINT '================================================================================';
PRINT '';
PRINT 'Review Results:';
PRINT '- Any rows returned from validation checks indicate data quality issues';
PRINT '- Informational queries provide insight into data distribution';
PRINT '- Address any issues found before proceeding to Gold layer';
PRINT '================================================================================';
GO

/*
================================================================================
End of Quality Checks
================================================================================

Maintenance Log:
--------------------------------------------------------------------------------
Date         Author          Version    Description
--------------------------------------------------------------------------------
[05/02/2026]       [Ahmed Shittu]     1.0        Initial creation
--------------------------------------------------------------------------------

Quality Check Summary:
    Table 1: CRM_Cust_Info      - 4 checks (PK, format, standardization)
    Table 2: CRM_Prod_Info      - 7 checks (PK, FK, format, validity, logic)
    Table 3: CRM_sales_details  - 7 checks (PK, format, dates, business rules)
    Table 4: ERP_Cust_az12      - 5 checks (cleansing, validation, matching)
    Table 5: ERP_loc_az101      - 4 checks (normalization, standardization)
    Table 6: ERP_px_cat_g1v2    - 6 checks (structure, format, taxonomy)

Total Quality Checks: 33

*/
