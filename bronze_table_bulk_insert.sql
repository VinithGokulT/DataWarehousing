drop procedure bronze.load_bronze;

create or alter procedure bronze.load_bronze as
BEGIN
	DECLARE @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	BEGIN TRY
		print('====================================');
		print('Loading the bronze layer');
		print('====================================');
		set @batch_start_time = GETDATE();
		print('-----------------------------');
		print('Loading CRM tables');
		print('-----------------------------');

		set @start_time = GETDATE();
		print('Truncating table:crm_cust_info');
		truncate table bronze.crm_cust_info
		print('inserting table:crm_cust_info')
		BULK INSERT bronze.crm_cust_info
		from
		'C:\Users\vinit\Downloads\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		print ('loading duration: '+ cast( datediff(second, @start_time, @end_time) as nvarchar)+' second');

		--2
		set @start_time = GETDATE();
		print('Truncating table:crm_prd_info');
		truncate table bronze.crm_prd_info;
		print('inserting table:crm_prd_info')
		bulk insert bronze.crm_prd_info
		from 
		'C:\Users\vinit\Downloads\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with(
		firstrow= 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		print ('loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' second');
		print('-------------------');


		set @start_time = GETDATE();
		print('Truncating table:crm_sales_details');
		truncate table bronze.crm_sales_details
		print('inserting table:crm_sales_details')
		bulk insert bronze.crm_sales_details
		from 
		'C:\Users\vinit\Downloads\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with(
		firstrow= 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		print ('loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' second');
		print('-------------------');


		print('-----------------------------');
		print('Loading ERP tables');
		print('-----------------------------');

		set @start_time = GETDATE();
		print('Truncating table:erp_loc_a101');
		truncate table bronze.erp_loc_a101
		print('inserting table:erp_loc_a101')
		bulk insert bronze.erp_loc_a101
		from 
		'C:\Users\vinit\Downloads\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		with(
		firstrow= 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		print ('loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' second');
		print('-------------------');


		set @start_time = GETDATE();
		print('Truncating table:erp_cust_az12');
		truncate table bronze.erp_cust_az12
		print('inserting table:erp_cust_az12')
		bulk insert bronze.erp_cust_az12
		from 
		'C:\Users\vinit\Downloads\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		with(
		firstrow= 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		print ('loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' second');
		print('-------------------');


		set @start_time = GETDATE();
		print('Truncating table:erp_px_cat_g1v2');
		truncate table bronze.erp_px_cat_g1v2
		print('inserting table:erp_px_cat_g1v2')
		bulk insert bronze.erp_px_cat_g1v2
		from 
		'C:\Users\vinit\Downloads\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow= 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		print ('loading duration: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' second');
		print('-------------------');
		set @batch_end_time = GETDATE();
		print ('bronze_loading duration: '+ cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar)+' second');

	END TRY
	BEGIN CATCH
		print('=====================');
		print('error occured during bronze layer')
		print('error message' + error_message());
		print('error message' + cast(error_number() as nvarchar));
		print('error message' + cast(error_state() as nvarchar));
		print('=====================');
	END CATCH

END


EXEC bronze.load_bronze;