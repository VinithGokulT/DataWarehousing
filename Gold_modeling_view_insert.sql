if OBJECT_ID('gold.dim_customer','v') is not null
	drop view gold.dim_customer;
go
create view gold.dim_customer as 
	select 
		row_number() over(order by cst_id)	as customer_key,
		ci.cst_id								as customer_id,
		ci.cst_key								as customer_number,
		ci.cst_firstname						as first_name,
		ci.cst_lastname							as last_name,
		ca.bdate								as birthdate,
		ci.cst_marital_status					as marital_status,
		la.cntry								as country,
		case
			when ci.cst_gndr !='n/a' then ci.cst_gndr
			else coalesce (ca.gen,'n/a')
		end										as gender,
		ci.cst_create_date						as creat_date
	from silver.crm_cust_info ci
	left 
	join silver.erp_cust_az12 ca on ci.cst_key=ca.cid
	left 
	join silver.erp_loc_a101 la on ci.cst_key=la.cid
go


if object_id('gold.dim_product','v') is not null
	drop view gold.dim_product
GO
create view gold.dim_product as
	select 
		cp.prd_id			product_id,
		cp.prd_key			product_key,
		cp.prd_nm			product_name,
		cp.cat_id			category_id,
		ep.cat				category,
		ep.subcat			subcategory,
		cp.prd_line			product_line,
		cp.prd_cost			product_cost,
		ep.maintenance		maintenance,
		cp.prd_start_dt		start_date

	from silver.crm_prd_info cp
	join silver.erp_px_cat_g1v2 ep
		on cp.cat_id = ep.id
go

if OBJECT_ID('gold.fact_sales','v') is not null
	drop view gold.fact_sales
GO

create view gold.fact_sales as
	select 
		cs.sls_ord_num		order_number,
		dc.customer_key		customer_key,
		dp.product_key		product_key,
		cs.sls_order_dt		order_date,
		cs.sls_ship_dt		ship_date,
		cs.sls_due_dt		due_date,
		cs.sls_sales		sales,
		cs.sls_quantity		quantity,
		cs.sls_price		price

	from silver.crm_sales_details cs
	join gold.dim_product dp 
		on cs.sls_prd_key = dp.product_key
	join gold.dim_customer dc
		on cs.sls_cust_id = dc.customer_id
