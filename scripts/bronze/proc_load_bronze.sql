/*
=======================================================================
Stored Procedure: load Bronze layer (source -> Bronze)
=======================================================================
Script Purpose
    This stored procedure loads data into the 'bronze' schema from external csv files.
    Its performs the following actions:
    -Truncate the btonze tables before loading data.
    -uses the 'Bulk insert' command to load data from csv files to bronze tables.

Parameters:
  None.
This store procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
========================================================================
*/


create or alter procedure bronze.load_bronze as 
	Begin
		DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @batch_start_time datetime, @batch_end_time datetime;
		Begin try
			set @batch_start_time = Getdate();
			print'=========================================';
			print 'loading bronze layer';
			print'=========================================';

			print'-----------------------------------------';
			print 'loading CRM Tables'
			print'-----------------------------------------';

		SET @START_TIME = GETDATE();
		print '>> Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting data into : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\user\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock 
		);
		SET @END_TIME = GETDATE();
		PRINT '>> lOAD DURATION:' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>>-------------';

		SET @START_TIME = GETDATE();
		print '>> Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting data into : bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\user\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock 
		);
		SET @END_TIME = GETDATE();
		PRINT '>> lOAD DURATION:' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>>-------------';

		SET @START_TIME = GETDATE();
		print '>> Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>> Inserting data into : bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\user\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock 
		);
		SET @END_TIME = GETDATE();
		PRINT '>> lOAD DURATION:' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>>-------------';

			print'-----------------------------------------';
			print 'loading erp Tables'
			print'-----------------------------------------';

		SET @START_TIME = GETDATE();
		print '>> Truncating table: bronze.erp_cus_az12';
		truncate table bronze.erp_cus_az12;

		print '>> Inserting data into : bronze.erp_cus_az12'
		bulk insert bronze.erp_cus_az12
		from 'C:\Users\user\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock 
		);
		SET @END_TIME = GETDATE();
		PRINT '>> lOAD DURATION:' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>>-------------';

		SET @START_TIME = GETDATE();
		print '>> Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting data into : bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\user\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock 
		);
		SET @END_TIME = GETDATE();
		PRINT '>> lOAD DURATION:' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>>-------------';

		SET @START_TIME = GETDATE();
		print '>> Truncating table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting data into : bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\user\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock 
		);
		SET @END_TIME = GETDATE();
		PRINT '>> lOAD DURATION:' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + 'SECONDS';
		PRINT '>>-------------';

		set @batch_end_time = GETDATE();
		PRINT '======================================'
		PRINT 'LOADING BRONZE LAYER IS COMPLETED';
		PRINT '  - TOTAL LOAD DURATION: ' + CAST (DATEDIFF(SECOND, @BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + ' SECONDS';


			END TRY
			BEGIN CATCH
				PRINT '=============================='
				PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
				PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
				PRINT 'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
				PRINT '=============================='
			END CATCH
end 

