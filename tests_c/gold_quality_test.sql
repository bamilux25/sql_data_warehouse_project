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
