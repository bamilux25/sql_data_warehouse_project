/***************************************************************************************************
*
*   Script:      01 - Create Data Warehouse Database and Medallion Schemas.sql
*   Author:      Shittu (Data Engineer)
*   Created:     2025 / 2026
*   Project:     Modern Data Warehouse – CRM + ERP Integration
*
*   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
*   │  PURPOSE                                                                                    │
*   └─────────────────────────────────────────────────────────────────────────────────────────────┘
*   This script:
*     • Drops the existing 'DatawareHouse' database (if it exists)
*     • Creates a fresh 'DatawareHouse' database
*     • Creates the three core schemas following the **modern Medallion Architecture**:
*       - Bronze  → raw / landing data (as close as possible to source format)
*       - Silver  → cleaned, validated, lightly transformed, conformed data
*       - Gold    → business-ready, aggregated, enriched, analytics-optimized tables/views
*
*   This is typically the **very first script** in the project setup / initialization pipeline.
*
*   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
*   │  ⚠️  IMPORTANT WARNING & SAFETY INFORMATION                                                 │
*   └─────────────────────────────────────────────────────────────────────────────────────────────┘
*   ★ THIS SCRIPT **PURPOSELY DESTROYS** THE EXISTING DATABASE NAMED 'DatawareHouse'
*   ★ All data, tables, views, procedures, functions, indexes, etc. WILL BE **PERMANENTLY DELETED**
*
*   → Only run this script:
*     • In **development** or **test** environments
*     • When you explicitly want to reset the entire data warehouse to a clean state
*     • When you have **no valuable data** in the current DatawareHouse database
*
*   → Recommended safeguards before running:
*     1. Confirm you are NOT connected to a production server
*     2. Verify the database name ('DatawareHouse') matches your intention
*     3. Take a **full backup** if there's even a small chance data might be needed later
*     4. Consider renaming this script or adding an extra confirmation prompt in CI/CD pipelines
*
*   → Safer alternatives for production/ongoing environments:
*     • Use schema-only reset scripts (drop objects inside schemas, not the whole DB)
*     • Use database snapshots or restore from backup instead of drop & recreate
*
***************************************************************************************************/

-- ────────────────────────────────────────────────────────────────────────────────────────────────
--  Execution context: should run under a sysadmin or dbcreator role
-- ────────────────────────────────────────────────────────────────────────────────────────────────

USE master;
GO

-- Drop and recreate database if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DatawareHouse')
BEGIN
    ALTER DATABASE DatawareHouse 
        SET SINGLE_USER 
        WITH ROLLBACK IMMEDIATE;
    
    DROP DATABASE DatawareHouse;
PRINT '=========================================';    
PRINT 'Existing DatawareHouse database dropped.';
PRINT  '========================================';
END
ELSE
BEGIN
    PRINT '========================================='; 
    PRINT 'No existing DatawareHouse database found.';
    PRINT '========================================='; 
END
GO

-- Create fresh database
CREATE DATABASE DatawareHouse;
GO
PRINT '========================================='; 
PRINT 'DatawareHouse database created successfully.';
PRINT '========================================='; 
GO

-- Switch context
USE DatawareHouse;
GO

-- Create Medallion Architecture schemas
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO

PRINT '========================================='; 
PRINT 'Bronze, Silver, and Gold schemas created.';
PRINT '========================================='; 

GO

-- Optional: quick verification
SELECT 
    name AS SchemaName, 
    schema_id 
FROM sys.schemas 
WHERE name IN ('Bronze','Silver','Gold');
GO
