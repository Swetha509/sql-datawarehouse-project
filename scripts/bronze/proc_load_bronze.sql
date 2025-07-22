/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE datawarehouse.bronze.load_bronze()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER 
AS
BEGIN
        --Loading bronze layer
        --loading crm tables 
        -- crm_cust_info
        --truncating table: bronze.crm_cust_info 
        TRUNCATE TABLE datawarehouse.bronze.crm_cust_info;
        --inserting data into:bronze.crm_cust_info
        COPY INTO datawarehouse.bronze.crm_cust_info
        FROM @datawarehouse.external_stages.aws_s3_csv
        FILES = ('cust_info.csv');
        

        -- crm_prd_info
         --truncating table: bronze.crm_prd_info 
        TRUNCATE TABLE datawarehouse.bronze.crm_prd_info;
         --inserting data into:bronze.crm_prd_info
        COPY INTO datawarehouse.bronze.crm_prd_info
        FROM @datawarehouse.external_stages.aws_s3_csv
        FILES = ('prd_info.csv');
       
        -- crm_sales_details
         --truncating table: bronze.crm_sales_details
        TRUNCATE TABLE datawarehouse.bronze.crm_sales_details;
         --inserting data into:bronze.crm_sales_details
        COPY INTO datawarehouse.bronze.crm_sales_details
        FROM @datawarehouse.external_stages.aws_s3_csv
        FILES = ('sales_details.csv');

        --loading erp tables 
        -- erp_loc_a101
         --truncating table: bronze.erp_loc_a101 
        TRUNCATE TABLE datawarehouse.bronze.erp_loc_a101;
         --inserting data into:bronze.erp_loc_a101
        COPY INTO datawarehouse.bronze.erp_loc_a101
        FROM @datawarehouse.external_stages.aws_s3_csv
        FILES = ('LOC_A101.csv');
        
        -- erp_cust_az12
         --truncating table: bronze.erp_cust_az12 
        TRUNCATE TABLE datawarehouse.bronze.erp_cust_az12;
         --inserting data into:bronze.erp_cust_az12
        COPY INTO datawarehouse.bronze.erp_cust_az12
        FROM @datawarehouse.external_stages.aws_s3_csv
        FILES = ('CUST_AZ12.csv');
       
        -- erp_px_cat_g1v2
         --truncating table: bronze.erp_px_cat_g1v2 
        TRUNCATE TABLE datawarehouse.bronze.erp_px_cat_g1v2;
         --inserting data into:bronze.erp_px_cat_g1v2
        COPY INTO datawarehouse.bronze.erp_px_cat_g1v2
        FROM @datawarehouse.external_stages.aws_s3_csv
        FILES = ('PX_CAT_G1V2.csv');
        
     RETURN 'Bronze layer loaded successfully.';
END;



call datawarehouse.bronze.load_bronze();

select * from datawarehouse.bronze.crm_cust_info; 
select * from datawarehouse.bronze.crm_prd_info; 
select * from datawarehouse.bronze.crm_sales_details; 
select * from datawarehouse.bronze.erp_loc_a101; 
select * from datawarehouse.bronze.erp_cust_az12; 
select * from datawarehouse.bronze.erp_px_cat_g1v2; 

