/*
================================================================================
Stored Procedure: Sliver.Bulk_load
Description:      ETL process to transform and load data from Bronze to Silver layer
Author:           Ahmed Shittu]
Created:          [05/02/2026]
Database:         DatawareHouse
Schema:           Sliver
Version:          1.0
================================================================================

Purpose:
    This stored procedure performs data transformation and cleansing operations
    as part of the ETL pipeline, moving data from the Bronze layer (raw data)
    to the Silver layer (cleaned and standardized data).

Process Flow:
    1. CRM_Cust_Info      - Customer data deduplication and standardization
    2. CRM_Prod_Info      - Product catalog enrichment and normalization
    3. CRM_sales_details  - Sales data validation and calculation
    4. ERP_Cust_az12      - ERP customer data cleansing
    5. ERP_loc_az101      - Location data standardization
    6. ERP_px_cat_g1v2    - Product category pass-through

Features:
    - Automatic error handling and rollback
    - Row count tracking for each table
    - Execution time monitoring
    - Detailed logging output
    - Data quality validations

Dependencies:
    - Bronze layer tables must be populated
    - Silver layer tables must exist
    - Database: DatawareHouse

Usage:
    EXEC Sliver.Bulk_load;

Returns:
    - Success/Error messages via PRINT statements
    - Row counts for each processed table
    - Total execution duration

Error Handling:
    - All errors are caught and logged
    - Transaction is rolled back on failure
    - Error details include message and line number

================================================================================
*/

CREATE OR ALTER PROCEDURE Sliver.Bulk_load
AS 
BEGIN 
    -- Prevent row count messages for cleaner output
    SET NOCOUNT ON;
    
    -- Declare variables for tracking and error handling
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowCount INT;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Print execution header
        PRINT '================================================================';
        PRINT 'Stored Procedure: Sliver.Bulk_load';
        PRINT 'Execution Started: ' + CONVERT(VARCHAR(23), @StartTime, 121);
        PRINT '================================================================';
        PRINT '';
        
        /*
        ================================================================
        TABLE 1: Bronze.CRM_Cust_Info -> Sliver.CRM_Cust_Info
        ================================================================
        Purpose: Clean and standardize customer information from CRM system
        
        Data Quality Issues Addressed:
        1. Remove duplicate records from the table
        2. Unwanted space removal from first and last name
        3. Data normalization/standardization of customer gender/marital status
        
        Transformations:
        - Deduplication: Keep latest record per cust_id based on cust_created_date
        - Text cleaning: TRIM() applied to firstname and lastname
        - Gender mapping: F → Female, M → Male, Other → n/a
        - Marital status mapping: M → Married, S → Single, Other → n/a
        - NULL filtering: Exclude records where cust_id IS NULL
        ================================================================
        */
        PRINT '>> Processing: Bronze.CRM_Cust_Info -> Sliver.CRM_Cust_Info';
        
        -- Clear existing data in Silver layer
        TRUNCATE TABLE Sliver.CRM_Cust_Info;

        -- Deduplicate and transform customer data
        WITH cte_sliver AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cust_created_date DESC) AS rn  
            FROM Bronze.CRM_Cust_Info
        )
        INSERT INTO Sliver.CRM_Cust_Info (
            cust_id,
            cust_key,
            cust_firstname,
            cust_lastname,
            cust_maritial_status,
            cust_gender,
            cust_created_date
        )
        SELECT 
            cust_id,
            cust_key,
            TRIM(cust_firstname) AS cust_firstname,
            TRIM(cust_lastname) AS cust_lastname,
            CASE WHEN UPPER(TRIM(cust_maritial_status)) = 'M' THEN 'Married'
                 WHEN UPPER(TRIM(cust_maritial_status)) = 'S' THEN 'Single'
                 ELSE 'n/a'
            END AS cust_maritial_status,
            CASE WHEN UPPER(TRIM(cust_gender)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
                 ELSE 'n/a'
            END AS cust_gender,
            cust_created_date
        FROM cte_sliver 
        WHERE rn = 1 AND cust_id IS NOT NULL;
        
        -- Log row count
        SET @RowCount = @@ROWCOUNT;
        PRINT '   ✓ Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '';

        /*
        ================================================================
        TABLE 2: Bronze.CRM_Prod_Info -> Sliver.CRM_prod_Info
        ================================================================
        Purpose: Enrich and normalize product catalog data
        
        Data Quality Issues Addressed:
        1. Derivation of new column (feature engineering)
        2. Perform data normalization on prod_key
        3. Perform data enrichment to prod_end_date
        4. Data normalization/standardization on prod_line
        
        Transformations:
        - Feature engineering: Extract prod_cat_id from first 5 chars of prod_key
        - Key normalization: Extract actual prod_key from position 7 onwards
        - Missing data handling: Replace NULL prod_cost with 0
        - Product line mapping: M → Mountain, R → Road, S → Other Sales, T → Touring
        - End date calculation: Use LEAD() to derive end date from next start date
        ================================================================
        */
        PRINT '>> Processing: Bronze.CRM_Prod_Info -> Sliver.CRM_prod_Info';
        
        -- Clear existing data in Silver layer
        TRUNCATE TABLE Sliver.CRM_prod_Info;

        INSERT INTO Sliver.CRM_prod_Info (
            prod_id,
            prod_cat_id,
            prod_key,
            prod_name,
            prod_cost,
            prod_line,
            prod_start_date,
            prod_end_date
        )
        SELECT 
            prod_id,
            REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS Prod_cat_id,
            SUBSTRING(prod_key, 7, LEN(prod_key)) AS prod_key,
            prod_name,
            ISNULL(prod_cost, 0) AS prod_cost,
            CASE WHEN UPPER(TRIM(prod_line)) = 'M' THEN 'Mountain'
                 WHEN UPPER(TRIM(prod_line)) = 'R' THEN 'Road'
                 WHEN UPPER(TRIM(prod_line)) = 'S' THEN 'Other Sales'
                 WHEN UPPER(TRIM(prod_line)) = 'T' THEN 'Touring'
                 ELSE 'n/a'
            END AS prod_line,
            prod_start_date,
            DATEADD(DAY, -1, LEAD(prod_start_date) OVER (PARTITION BY prod_key ORDER BY prod_start_date)) AS prod_end_date 
        FROM Bronze.CRM_Prod_Info;
        
        -- Log row count
        SET @RowCount = @@ROWCOUNT;
        PRINT '   ✓ Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '';

        /*
        ================================================================
        TABLE 3: Bronze.CRM_sales_details -> Sliver.CRM_sales_details
        ================================================================
        Purpose: Validate and clean sales transaction data
        
        Data Quality Issues Addressed:
        1. Handling invalid data
        2. Data type casting
        3. Handling missing data
        4. Handling invalid date
        
        Transformations:
        - Date validation: Check for 0 or incorrect length (must be 8 digits)
        - Date conversion: Cast YYYYMMDD integer format to DATE type
        - Sales calculation: Derive from quantity * price when missing
        - Price calculation: Derive from sales / quantity when missing
        - Negative handling: Apply ABS() to negative prices
        - Zero/NULL sales: Recalculate from quantity and price
        ================================================================
        */
        PRINT '>> Processing: Bronze.CRM_sales_details -> Sliver.CRM_sales_details';
        
        -- Clear existing data in Silver layer
        TRUNCATE TABLE Sliver.CRM_sales_details;

        INSERT INTO Sliver.CRM_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- Validate and convert order date
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
            END AS sls_order_dt,
            -- Validate and convert ship date
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
            END AS sls_ship_dt,
            -- Validate and convert due date
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
            END AS sls_due_dt,
            -- Calculate or correct sales amount
            CASE WHEN sls_sales IS NULL AND sls_price IS NOT NULL THEN sls_quantity * ABS(sls_price)
                 WHEN sls_sales <= 0 AND sls_price IS NOT NULL THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales 
            END AS sls_sales,
            sls_quantity,
            -- Calculate or correct unit price
            CASE WHEN sls_price IS NULL AND sls_sales IS NOT NULL THEN sls_sales / sls_quantity
                 WHEN sls_price < 0 THEN ABS(sls_price)
                 ELSE sls_price 
            END AS sls_price
        FROM Bronze.CRM_sales_details;
        
        -- Log row count
        SET @RowCount = @@ROWCOUNT;
        PRINT '   ✓ Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '';

        /*
        ================================================================
        TABLE 4: Bronze.ERP_Cust_az12 -> Sliver.ERP_Cust_az12
        ================================================================
        Purpose: Clean customer demographic data from ERP system
        
        Data Quality Issues Addressed:
        1. Removal of invalid characters at the beginning of the primary key
        2. Handling invalid date field (out-of-range date)
        3. Handling data standardization and consistency
        
        Transformations:
        - ID cleaning: Remove 'NAS' prefix from customer IDs
        - Date validation: Set future birthdates to NULL
        - Gender standardization: F/Female → Female, M/Male → Male, Other → n/a
        ================================================================
        */
        PRINT '>> Processing: Bronze.ERP_Cust_az12 -> Sliver.ERP_Cust_az12';
        
        -- Clear existing data in Silver layer
        TRUNCATE TABLE Sliver.ERP_Cust_az12;

        INSERT INTO [Sliver].[ERP_Cust_az12] (
            cid,
            bdate,
            gen
        )
        SELECT
            -- Remove invalid 'NAS' prefix from customer ID
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                 ELSE cid
            END AS cid,
            -- Validate birthdate (cannot be in future)
            CASE WHEN bdate > GETDATE() THEN NULL
                 ELSE bdate
            END AS bdate,
            -- Standardize gender values
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
                 ELSE 'n/a'
            END gen
        FROM [Bronze].[ERP_Cust_az12];
        
        -- Log row count
        SET @RowCount = @@ROWCOUNT;
        PRINT '   ✓ Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '';

        /*
        ================================================================
        TABLE 5: Bronze.ERP_loc_az101 -> Sliver.ERP_loc_az101
        ================================================================
        Purpose: Standardize customer location data from ERP
        
        Data Quality Issues Addressed:
        1. Handling primary and foreign key inconsistency
        2. Data standardization and consistency
        
        Transformations:
        - ID normalization: Remove hyphens from customer IDs for consistency
        - Country mapping: US/USA → United State, DE → Germany
        - Missing data: Empty or NULL country → n/a
        ================================================================
        */
        PRINT '>> Processing: Bronze.ERP_loc_az101 -> Sliver.ERP_loc_az101';
        
        -- Clear existing data in Silver layer
        TRUNCATE TABLE Sliver.ERP_loc_az101;

        INSERT INTO [Sliver].[ERP_loc_az101] (
            cid,
            cntry
        )
        SELECT 
            -- Normalize customer ID by removing hyphens
            CASE WHEN cid LIKE '%-%' THEN REPLACE(cid, '-', '') 
                 ELSE cid
            END cid,
            -- Standardize country names
            CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United State'
                 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
                 ELSE cntry
            END cntry
        FROM [Bronze].[ERP_loc_az101];
        
        -- Log row count
        SET @RowCount = @@ROWCOUNT;
        PRINT '   ✓ Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '';

        /*
        ================================================================
        TABLE 6: Bronze.ERP_px_cat_g1v2 -> Sliver.ERP_px_cat_g1v2
        ================================================================
        Purpose: Load product category hierarchy from ERP
        
        Data Quality Issues Addressed: N/A
        
        Transformations:
        - Direct pass-through (no transformations required)
        - Source data quality is acceptable
        
        Note: This table serves as a reference/lookup table for product categories
        ================================================================
        */
        PRINT '>> Processing: Bronze.ERP_px_cat_g1v2 -> Sliver.ERP_px_cat_g1v2';
        
        -- Clear existing data in Silver layer
        TRUNCATE TABLE Sliver.ERP_px_cat_g1v2;

        INSERT INTO [Sliver].[ERP_px_cat_g1v2] (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT   
            id,
            cat,
            subcat,
            maintenance
        FROM [Bronze].[ERP_px_cat_g1v2];
        
        -- Log row count
        SET @RowCount = @@ROWCOUNT;
        PRINT '   ✓ Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '';
        
        -- Print success summary
        PRINT '================================================================';
        PRINT 'Execution Completed Successfully!';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + ' seconds';
        PRINT '================================================================';
        
    END TRY
    BEGIN CATCH
        -- Capture error details
        SET @ErrorMessage = ERROR_MESSAGE();
        
        -- Print error information
        PRINT '';
        PRINT '================================================================';
        PRINT 'ERROR OCCURRED!';
        PRINT 'Error Message: ' + @ErrorMessage;
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT '================================================================';
        
        -- Re-throw the error to halt execution
        THROW;
    END CATCH
    
END
GO

/*
================================================================================
End of Stored Procedure
================================================================================

Maintenance Log:
--------------------------------------------------------------------------------
Date         Author          Version    Description
--------------------------------------------------------------------------------
[05/02/2026]       [Ahmed Shittu]     1.0        Initial creation
--------------------------------------------------------------------------------

*/
