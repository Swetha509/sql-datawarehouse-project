/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call Silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE datawarehouse.silver.load_silver()
RETURNS STRING
LANGUAGE SQL
AS
begin 
--truncating table: silver.crm_cust_info
truncate table silver.crm_cust_info;
--inserting data into silver.crm_cust_info
INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		

-- Truncate the target table
truncate table silver.crm_prd_info;
-- Insert transformed data into the target table
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category ID
    SUBSTR(prd_key, 7) AS prd_key,                        -- Extract product key
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line, -- Map product line codes to descriptive values
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(DATEADD(DAY, -1, 
         LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
    ) AS DATE) AS prd_end_dt -- Calculate end date as one day before next start date
FROM bronze.crm_prd_info;


-- Truncate the target table
TRUNCATE TABLE silver.crm_sales_details;
-- Insert transformed data into target table   
INSERT INTO silver.crm_sales_details (
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

    -- Handle order date
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(TO_VARCHAR(sls_order_dt)) != 8 THEN NULL
        ELSE TO_DATE(TO_VARCHAR(sls_order_dt), 'YYYYMMDD')
    END AS sls_order_dt,

    -- Handle ship date
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(TO_VARCHAR(sls_ship_dt)) != 8 THEN NULL
        ELSE TO_DATE(TO_VARCHAR(sls_ship_dt), 'YYYYMMDD')
    END AS sls_ship_dt,

    -- Handle due date
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(TO_VARCHAR(sls_due_dt)) != 8 THEN NULL
        ELSE TO_DATE(TO_VARCHAR(sls_due_dt), 'YYYYMMDD')
    END AS sls_due_dt,

    -- Recalculate sales if invalid
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    -- Derive price if invalid
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

-- Truncate target table
TRUNCATE TABLE silver.erp_loc_a101;
-- Insert transformed data into target table
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', '') AS cid,  -- Remove dashes
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry -- Normalize and handle missing/blank country codes
FROM bronze.erp_loc_a101;


-- Truncate the target table
TRUNCATE TABLE silver.erp_cust_az12;
-- Insert transformed data into target table
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    -- Remove 'NAS' prefix if present
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4)
        ELSE cid
    END AS cid,

    -- Set future birthdates to NULL
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate,

    -- Normalize gender values
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;

--truncate target table 
TRUNCATE TABLE silver.erp_px_cat_g1v2;
-- insert transformed data into target table
INSERT INTO silver.erp_px_cat_g1v2 (
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
 FROM bronze.erp_px_cat_g1v2;
  return 'silver data loaded successfully';
end;

call datawarehouse.silver.load_silver();

select * from datawarehouse.silver.crm_cust_info; 
select * from datawarehouse.silver.crm_prd_info; 
select * from datawarehouse.silver.crm_sales_details; 
select * from datawarehouse.silver.erp_loc_a101; 
select * from datawarehouse.silver.erp_cust_az12; 
select * from datawarehouse.silver.erp_px_cat_g1v2; 






