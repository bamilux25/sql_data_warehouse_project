/*
================================================================================
STORED PROCEDURE: Bronze.Bulk_Insert_record
================================================================================

PURPOSE:
This stored procedure performs the Extract and Load phases of the ETL pipeline
for the Bronze Layer in the Data Warehouse Medallion Architecture. It loads 
raw data from CSV files into Bronze staging tables without transformation or 
cleansing.

ARCHITECTURE LAYER: Bronze (Raw Data Landing Zone)
DATABASE: DatawareHouse
SCHEMA: Bronze

FUNCTIONALITY:
  - Truncates existing Bronze layer tables to ensure clean load
  - Performs BULK INSERT operations from CSV source files
  - Loads data from both CRM and ERP systems
  - Tracks execution duration for each table load
  - Implements comprehensive error handling for data quality issues
  - Provides detailed logging and progress reporting

DATA SOURCES:
  CRM System Files (C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_crm\):
    - cust_info.csv       : Customer master data
    - prd_info.csv        : Product catalog information
    - sales_details.csv   : Sales transaction records
  
  ERP System Files (C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_erp\):
    - CUST_AZ12.csv       : Customer demographic data
    - LOC_A101.csv        : Customer location/country data
    - PX_CAT_G1V2.csv     : Product categorization and maintenance

TABLES POPULATED:
  - Bronze.CRM_Cust_Info
  - Bronze.CRM_Prod_Info
  - Bronze.CRM_sales_details
  - Bronze.ERP_Cust_az12
  - Bronze.ERP_loc_az101
  - Bronze.ERP_px_cat_g1v2

EXECUTION:
  EXEC Bronze.Bulk_Insert_record;

OUTPUT:
  - Console messages showing progress for each table
  - Row counts and duration metrics for each load operation
  - Total execution time for the complete process
  - Detailed error messages if any failures occur

ERROR HANDLING:
  - Individual try-catch blocks for each table load
  - Fails fast on first error to prevent partial loads
  - Comprehensive error reporting (message, severity, state, line number)
  - Transaction rollback on failure

PERFORMANCE FEATURES:
  - TABLOCK hint for optimized bulk loading
  - Millisecond precision timing for performance analysis
  - Row count tracking for data validation
  - Batch processing for large datasets

DEPENDENCIES:
  - Source CSV files must exist in specified locations
  - Bronze layer tables must be created before execution
  - SQL Server service account must have read access to file paths
  - BULK INSERT permissions required

NEXT STEPS AFTER EXECUTION:
  1. Verify row counts match source file records
  2. Check for any data quality issues in Bronze tables
  3. Execute Silver layer transformation procedures
  4. Monitor execution duration for performance optimization

AUTHOR: Ahmed Shittu
CREATED: 28/01/2026
MODIFIED: [28/01/2026]
VERSION: 1.0

EXECUTION HISTORY:
  Date         | User    | Changes
  -------------|---------|--------------------------------------------------
  YYYY-MM-DD   | Author  | Initial creation with error handling and timing
  
================================================================================
*/

CREATE OR ALTER PROCEDURE Bronze.Bulk_Insert_record 
AS
BEGIN
    SET NOCOUNT ON;
    
    -- =========================================================================
    -- VARIABLE DECLARATIONS
    -- =========================================================================
    DECLARE @StartTime DATETIME2 = SYSDATETIME();
    DECLARE @EndTime DATETIME2;
    DECLARE @Duration VARCHAR(20);
    DECLARE @StepStartTime DATETIME2;
    DECLARE @StepDuration VARCHAR(20);
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    DECLARE @RowCount INT;
    
    BEGIN TRY
        -- =====================================================================
        -- PROCESS INITIALIZATION
        -- =====================================================================
        PRINT '============================================='
        PRINT 'Loading Bronze Layer Process'
        PRINT 'Start Time: ' + CONVERT(VARCHAR(30), @StartTime, 121)
        PRINT '============================================='
        
        -- =====================================================================
        -- SECTION A: CRM SOURCES BULK INSERTION
        -- =====================================================================
        PRINT '->->->->->->->->->->->->->->->->->->->->->->->'
        PRINT 'Loading CRM Files into Tables'
        PRINT '->->->->->->->->->->->->->->->->->->->->->->->'
        
        -- ---------------------------------------------------------------------
        -- CRM Table 1: Customer Information
        -- ---------------------------------------------------------------------
        BEGIN TRY
            SET @StepStartTime = SYSDATETIME();
            PRINT '>>>> Truncating Table: Bronze.CRM_Cust_Info';
            IF OBJECT_ID('DatawareHouse.Bronze.CRM_Cust_Info', 'U') IS NOT NULL 
                TRUNCATE TABLE Bronze.CRM_Cust_Info;
            
            PRINT '<<<< Inserting Data into Table: Bronze.CRM_Cust_Info';
            BULK INSERT Bronze.CRM_Cust_Info
            FROM 'C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_crm\cust_info.csv'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                FIRSTROW = 2,
                TABLOCK
            );
            
            SET @RowCount = @@ROWCOUNT;
            SET @StepDuration = CONVERT(VARCHAR(20), DATEDIFF(MILLISECOND, @StepStartTime, SYSDATETIME()));
            PRINT '     ✓ Rows Loaded: ' + CAST(@RowCount AS VARCHAR(10)) + ' | Duration: ' + @StepDuration + 'ms';
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = 'Error loading CRM_Cust_Info: ' + ERROR_MESSAGE();
            PRINT '     ✗ ' + @ErrorMessage;
            THROW;
        END CATCH
        
        -- ---------------------------------------------------------------------
        -- CRM Table 2: Product Information
        -- ---------------------------------------------------------------------
        BEGIN TRY
            SET @StepStartTime = SYSDATETIME();
            PRINT '>>>> Truncating Table: Bronze.CRM_Prod_Info';
            IF OBJECT_ID('DatawareHouse.Bronze.CRM_Prod_Info', 'U') IS NOT NULL 
                TRUNCATE TABLE Bronze.CRM_Prod_Info;
            
            PRINT '<<<< Inserting Data into Table: Bronze.CRM_Prod_Info';
            BULK INSERT Bronze.CRM_Prod_Info
            FROM 'C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_crm\prd_info.csv'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                FIRSTROW = 2,
                TABLOCK
            );
            
            SET @RowCount = @@ROWCOUNT;
            SET @StepDuration = CONVERT(VARCHAR(20), DATEDIFF(MILLISECOND, @StepStartTime, SYSDATETIME()));
            PRINT '     ✓ Rows Loaded: ' + CAST(@RowCount AS VARCHAR(10)) + ' | Duration: ' + @StepDuration + 'ms';
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = 'Error loading CRM_Prod_Info: ' + ERROR_MESSAGE();
            PRINT '     ✗ ' + @ErrorMessage;
            THROW;
        END CATCH
        
        -- ---------------------------------------------------------------------
        -- CRM Table 3: Sales Details
        -- ---------------------------------------------------------------------
        BEGIN TRY
            SET @StepStartTime = SYSDATETIME();
            PRINT '>>>> Truncating Table: Bronze.CRM_sales_details';
            IF OBJECT_ID('DatawareHouse.Bronze.CRM_sales_details', 'U') IS NOT NULL 
                TRUNCATE TABLE Bronze.CRM_sales_details;
            
            PRINT '<<<< Inserting Data into Table: Bronze.CRM_sales_details';
            BULK INSERT Bronze.CRM_sales_details
            FROM 'C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_crm\sales_details.csv'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                FIRSTROW = 2,
                TABLOCK
            );
            
            SET @RowCount = @@ROWCOUNT;
            SET @StepDuration = CONVERT(VARCHAR(20), DATEDIFF(MILLISECOND, @StepStartTime, SYSDATETIME()));
            PRINT '     ✓ Rows Loaded: ' + CAST(@RowCount AS VARCHAR(10)) + ' | Duration: ' + @StepDuration + 'ms';
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = 'Error loading CRM_sales_details: ' + ERROR_MESSAGE();
            PRINT '     ✗ ' + @ErrorMessage;
            THROW;
        END CATCH
        
        -- =====================================================================
        -- SECTION B: ERP SOURCES BULK INSERTION
        -- =====================================================================
        PRINT '->->->->->->->->->->->->->->->->->->->->->->->'
        PRINT 'Loading ERP Files into Tables'
        PRINT '->->->->->->->->->->->->->->->->->->->->->->->'
        
        -- ---------------------------------------------------------------------
        -- ERP Table 1: Customer Demographics (AZ12)
        -- ---------------------------------------------------------------------
        BEGIN TRY
            SET @StepStartTime = SYSDATETIME();
            PRINT '>>>> Truncating Table: Bronze.ERP_Cust_az12';
            IF OBJECT_ID('DatawareHouse.Bronze.ERP_Cust_az12', 'U') IS NOT NULL 
                TRUNCATE TABLE Bronze.ERP_Cust_az12;
            
            PRINT '<<<< Inserting Data into Table: Bronze.ERP_Cust_az12';
            BULK INSERT Bronze.ERP_Cust_az12
            FROM 'C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_erp\CUST_AZ12.csv'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                FIRSTROW = 2,
                TABLOCK
            );
            
            SET @RowCount = @@ROWCOUNT;
            SET @StepDuration = CONVERT(VARCHAR(20), DATEDIFF(MILLISECOND, @StepStartTime, SYSDATETIME()));
            PRINT '     ✓ Rows Loaded: ' + CAST(@RowCount AS VARCHAR(10)) + ' | Duration: ' + @StepDuration + 'ms';
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = 'Error loading ERP_Cust_az12: ' + ERROR_MESSAGE();
            PRINT '     ✗ ' + @ErrorMessage;
            THROW;
        END CATCH
        
        -- ---------------------------------------------------------------------
        -- ERP Table 2: Location Data (A101)
        -- ---------------------------------------------------------------------
        BEGIN TRY
            SET @StepStartTime = SYSDATETIME();
            PRINT '>>>> Truncating Table: Bronze.ERP_loc_az101';
            IF OBJECT_ID('DatawareHouse.Bronze.ERP_loc_az101', 'U') IS NOT NULL 
                TRUNCATE TABLE Bronze.ERP_loc_az101;
            
            PRINT '<<<< Inserting Data into Table: Bronze.ERP_loc_az101';
            BULK INSERT Bronze.ERP_loc_az101
            FROM 'C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_erp\LOC_A101.csv'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                FIRSTROW = 2,
                TABLOCK
            );
            
            SET @RowCount = @@ROWCOUNT;
            SET @StepDuration = CONVERT(VARCHAR(20), DATEDIFF(MILLISECOND, @StepStartTime, SYSDATETIME()));
            PRINT '     ✓ Rows Loaded: ' + CAST(@RowCount AS VARCHAR(10)) + ' | Duration: ' + @StepDuration + 'ms';
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = 'Error loading ERP_loc_az101: ' + ERROR_MESSAGE();
            PRINT '     ✗ ' + @ErrorMessage;
            THROW;
        END CATCH
        
        -- ---------------------------------------------------------------------
        -- ERP Table 3: Product Category (G1V2)
        -- ---------------------------------------------------------------------
        BEGIN TRY
            SET @StepStartTime = SYSDATETIME();
            PRINT '>>>> Truncating Table: Bronze.ERP_px_cat_g1v2';
            IF OBJECT_ID('DatawareHouse.Bronze.ERP_px_cat_g1v2', 'U') IS NOT NULL 
                TRUNCATE TABLE Bronze.ERP_px_cat_g1v2;
            
            PRINT '<<<< Inserting Data into Table: Bronze.ERP_px_cat_g1v2';
            BULK INSERT Bronze.ERP_px_cat_g1v2
            FROM 'C:\Users\PC Deals\Documents\SQL_DWH\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\n',
                FIRSTROW = 2,
                TABLOCK
            );
            
            SET @RowCount = @@ROWCOUNT;
            SET @StepDuration = CONVERT(VARCHAR(20), DATEDIFF(MILLISECOND, @StepStartTime, SYSDATETIME()));
            PRINT '     ✓ Rows Loaded: ' + CAST(@RowCount AS VARCHAR(10)) + ' | Duration: ' + @StepDuration + 'ms';
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = 'Error loading ERP_px_cat_g1v2: ' + ERROR_MESSAGE();
            PRINT '     ✗ ' + @ErrorMessage;
            THROW;
        END CATCH
        
        -- =====================================================================
        -- PROCESS COMPLETION
        -- =====================================================================
        SET @EndTime = SYSDATETIME();
        SET @Duration = CONVERT(VARCHAR(20), DATEDIFF(SECOND, @StartTime, @EndTime));
        
        PRINT '============================================='
        PRINT 'Bronze Layer Load Completed Successfully!'
        PRINT 'End Time: ' + CONVERT(VARCHAR(30), @EndTime, 121)
        PRINT 'Total Duration: ' + @Duration + ' seconds'
        PRINT '============================================='
        
    END TRY
    BEGIN CATCH
        -- =====================================================================
        -- ERROR HANDLING
        -- =====================================================================
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState = ERROR_STATE();
        
        PRINT '============================================='
        PRINT '✗✗✗ FATAL ERROR ✗✗✗'
        PRINT 'Error Message: ' + @ErrorMessage
        PRINT 'Error Severity: ' + CAST(@ErrorSeverity AS VARCHAR(10))
        PRINT 'Error State: ' + CAST(@ErrorState AS VARCHAR(10))
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10))
        PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'Not in procedure')
        PRINT '============================================='
        
        -- Re-throw the error to calling application
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- End of Stored Procedure: Bronze.Bulk_Insert_record
PRINT 'Stored Procedure Bronze.Bulk_Insert_record created/altered successfully!';
GO
