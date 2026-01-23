create or alter procedure silver.load_silver as
BEGIN
	declare @batch_start_time datetime, @batch_end_time datetime, @start_time datetime, @end_time datetime;
	BEGIN TRY
		set @batch_start_time = getdate();
		print '=============================';
		print 'Loading Silver tables';
		print '=============================';
		set @start_time = getdate();
		print 'truncate table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '=============================';
		print 'insert table: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date 
		)
		select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
			end cst_marital_status,
			case
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				when upper(trim(cst_gndr)) = 'F' then 'Female'
				else 'n/a'
			end cst_gndr,
			cst_create_date
		from 
		(
		select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
		from [DataWarehouse].[bronze].[crm_cust_info]
		where cst_id is not null
		) t
		where flag_last = 1 -- to select last created record if multiple values are present

		set @end_time = getdate()
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar )+'seconds';
		print '=============================';



		print '=============================';
		set @start_time = getdate();
		print 'truncate table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '=============================';
		print 'insert table: silver.crm_prd_info';
		insert into silver.crm_prd_info (
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
			replace(substring(prd_key,1,5), '-','_')as cat_id,
			substring(prd_key, 7, len(prd_key))as prd_key,
			prd_nm,
			isnull(prd_cost,0) as prd_cost,
			case upper(trim(prd_line))
				when 'R' then 'Road'
				when 'M' then 'Mountain'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a'
			end as prd_line,
			cast(prd_start_dt as date)as prd_start_dt,
			cast(
				lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1
					as date
					)
			as prd_end_dt
		FROM [DataWarehouse].[bronze].[crm_prd_info]

		set @end_time = getdate()
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar )+'seconds';
		print '=============================';



		print '=============================';
		set @start_time = getdate();
		print 'truncate table: silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '=============================';
		print 'insert table: silver.crm_sales_details';
		insert into silver.crm_sales_details(
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
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt =0 or len(sls_order_dt)<> 8 then NULL
				else cast(cast(sls_order_dt as varchar)as date)
			end sls_order_dt,
			case 
				when sls_ship_dt =0 or len(sls_ship_dt)<> 8 then NULL
				else cast(cast(sls_ship_dt as varchar)as date)
			end sls_ship_dt,
			case 
				when sls_due_dt =0 or len(sls_due_dt)<> 8 then NULL
				else cast(cast(sls_due_dt as varchar)as date)
			end sls_due_dt,
			case
				when sls_sales is null or sls_sales <= 0 or sls_sales <> sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price)
				else sls_sales
			end sls_sales,
			sls_quantity,
			case
				when sls_price is null or sls_price <= 0
					then sls_sales/nullif(sls_quantity,0)
				else sls_price
			end sls_price
		from [DataWarehouse].[bronze].crm_sales_details

		set @end_time = getdate()
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar )+'seconds';
		print '=============================';


		print '=============================';
		set @start_time = getdate();
		print 'truncate table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '=============================';
		print 'insert table: silver.erp_loc_a101';

		insert into silver.erp_loc_a101(
			cid,
			cntry
		)
		select 
			replace(cid, '-','') cid,
			case
				when trim(cntry)='US' or trim(cntry)='USA' then 'United States'
				when trim(cntry)='DE' then 'Germany'
				when trim(cntry)='' or trim(cntry) is null then 'n/a'
				else trim(cntry)
			end cntry
		from [DataWarehouse].[bronze].erp_loc_a101

		set @end_time = getdate()
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar )+'seconds';
		print '=============================';

		
		
		print '=============================';
		set @start_time = getdate();
		print 'truncate table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '=============================';
		print 'insert table: silver.erp_loc_a101';
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select 
			case 
				when cid like 'NAS%' then substring(cid, 4, len(cid))
				else cid
			end cid,
			case 
				when bdate > getdate() then NULL
				else bdate
			end bdate,
			case
				when upper(trim(gen)) in ('F','FEMALE') then 'Female'
				when upper(trim(gen)) in ('M','MALE') then 'Male'
				else 'n/a'
			end gen

		from [DataWarehouse].[bronze].erp_cust_az12

		set @end_time = getdate()
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar )+'seconds';
		print '=============================';


		print '=============================';
		set @start_time = getdate();
		print 'truncate table: silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '=============================';
		print 'insert table: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select
			id,
			cat,
			subcat,
			maintenance
		from
		[DataWarehouse].[bronze].erp_px_cat_g1v2

		set @end_time = getdate()
		print 'loading time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar )+'seconds';
		print '=============================';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END
