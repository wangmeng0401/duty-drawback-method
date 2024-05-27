/* 
Direct ID method to claim duty drawback on exports to Canada, Chile, and Mexico. 

For more details, refer to 
https://docs.google.com/document/d/1vf0gigQKJzqplZpWuyH9i0Q-vlDd3xEM-qHMrGuwwsw/edit#heading=h.f3ebbg5c1j11
*/

---------------------------------------------------------------------------------
-------------------------------- Imports data part ------------------------------
---------------------------------------------------------------------------------

------------------ Step 1: Load the 2 data files from Gavin ---------------------

drop table if exists bi_work.mengmeng_dd_import_skulevel_2023Q2_rawraw;
create table bi_work.mengmeng_dd_import_skulevel_2023Q2_rawraw (
	FLEX_ID varchar,
	Shipment_Name varchar(2000),
	Product_Name varchar,
	SKU varchar,
	PO_Numbers varchar,
	Quantity float,
	Average_Price_per_Unit float,
	Entry_Number varchar,
	Cleared_Customs_Date date,
	HS_Code varchar,
	Weight_kg float,
	Line_num varchar
	);

drop table if exists bi_work.mengmeng_dd_import_skulevel_2023Q2_raw;
create table bi_work.mengmeng_dd_import_skulevel_2023Q2_raw (
	FLEX_ID varchar,
	Shipment_Name varchar(2000),
	Product_Name varchar,
	SKU varchar,
	PO_Numbers varchar,
	Quantity float,
	Average_Price_per_Unit float,
	Entry_Number varchar,
	Cleared_Customs_Date date,
	HS_Code varchar,
	Weight_kg float,
	Line_num varchar,
	id int identity(1,1)
	);


drop table if exists bi_work.mengmeng_dd_imports_2023Q2_rawraw;
create table bi_work.mengmeng_dd_imports_2023Q2_rawraw (
	Entry_No varchar,
	MOT int,
	Importer varchar,
	import_date date,
	Entry_Date date,
	Entry_Port varchar,
	Reference_No varchar,
	Cust_Ref_No varchar,
	PO_No varchar,
	Consignee varchar,
	Line varchar,
	Gross_Weight varchar,
	C_O varchar,
	C_E varchar,
	Part_No varchar,
	Part_Description varchar,
	Tariff_No varchar,
	SPI varchar,
	Description varchar,
	Quantity_1 float,
	Unit_1 varchar,
	Quantity_2 float,
	Unit_2 varchar,
	Entered_Value float,
	Spec_Rate float,
	Adv_Rate float,
	Duty float,
	MPF_Calculated float,
	MPF_Prorated float,
	Total_Value float,
	Total_Duty float,
	Total_MPF float,
	Total_HMF float,
	Total_Other_Fees float,
	Total_ADD float,
	Total_CVD float,
	Total_Tax float,
	NAFTA float,
	Recon_Type float,
	"056_Cotton_Fee" float,
	"501_Harbor_Maintenance_Fee" float,
	Liquidation_Date date,
	FDA_Disclaim varchar
	);

drop table if exists bi_work.mengmeng_dd_imports_2023Q2_raw;
create table bi_work.mengmeng_dd_imports_2023Q2_raw (
	Entry_No varchar,
	MOT int,
	Importer varchar,
	import_date date,
	Entry_Date date,
	Entry_Port varchar,
	Reference_No varchar,
	Cust_Ref_No varchar,
	PO_No varchar,
	Consignee varchar,
	Line varchar,
	Gross_Weight varchar,
	C_O varchar,
	C_E varchar,
	Part_No varchar,
	Part_Description varchar,
	Tariff_No varchar,
	SPI varchar,
	Description varchar,
	Quantity_1 float,
	Unit_1 varchar,
	Quantity_2 float,
	Unit_2 varchar,
	Entered_Value float,
	Spec_Rate float,
	Adv_Rate float,
	Duty float,
	MPF_Calculated float,
	MPF_Prorated float,
	Total_Value float,
	Total_Duty float,
	Total_MPF float,
	Total_HMF float,
	Total_Other_Fees float,
	Total_ADD float,
	Total_CVD float,
	Total_Tax float,
	NAFTA float,
	Recon_Type float,
	"056_Cotton_Fee" float,
	"501_Harbor_Maintenance_Fee" float,
	Liquidation_Date date,
	FDA_Disclaim varchar, 
	id int identity(1,1)
	);

select count(*) from bi_work.mengmeng_dd_imports_2023Q2_raw;
drop table bi_work.mengmeng_dd_imports_2023Q2_rawraw;

--- Step 2: Sku level raw data clean and mapped with poitem and factorypo -------

DROP TABLE IF EXISTS bi_work.mengmeng_dd_import_sku_2023Q2_map;
CREATE TABLE bi_work.mengmeng_dd_import_sku_2023Q2_map AS 
WITH SKULEVELTEMP AS (
	SELECT DISTINCT -- clean the purchase order number and the hs code
		id, 
		flex_id, 
		shipment_name, 
		po_numbers_processed as po_numbers, 
		replace(sku, chr(10), ' ') AS sku, 
		product_name, 
		quantity, 
		entry_number, 
		cleared_customs_date, 
		hs_code_reformat AS hs_code_raw, 
		LOWER(TRIM(SPLIT_PART(a.hs_code_reformat, '|', NS.n))) AS hs_code_parsed
	FROM 
		(SELECT n FROM bi_work.numbers ORDER BY 1 LIMIT 2) NS
	INNER JOIN 
		(SELECT 
			flex_id,
			shipment_name,
			product_name,
			sku,
			po_numbers,
			CASE 
				WHEN TRIM(PO_Numbers) = '' THEN UPPER(TRIM(SPLIT_PART(sku, '/', 1)))
				ELSE UPPER(TRIM(PO_Numbers))
			END AS PO_Numbers_processed,
			quantity,
			average_price_per_unit,
			entry_number,
			cleared_customs_date,
			hs_code,
			CASE 
				WHEN REGEXP_COUNT(hs_code, ';') = 1 THEN TRIM(SPLIT_PART(hs_code, ';', 1))||','||TRIM(SPLIT_PART(hs_code, ';', 2))
				WHEN REGEXP_COUNT(hs_code, ';') > 1 THEN TRIM(SPLIT_PART(hs_code, ';', 1))||','||TRIM(SPLIT_PART(hs_code, ';', 2))||' | '||TRIM(SPLIT_PART(hs_code, ';', 3))||','||TRIM(SPLIT_PART(hs_code, ';', 4))
				ELSE hs_code 
			END AS hs_code_reformat,
			weight_kg,
			line_num,
			id
		FROM 
			bi_work.dd_import_skulevel_2023Q2_raw) a ON NS.n <= REGEXP_COUNT(a.hs_code, '|') + 1
	), 

SKULEVELIMPORTS AS (
	SELECT 
		id, 
		flex_id, 
		shipment_name, 
		po_numbers, 
		sku, 
		product_name, 
		quantity, 
		entry_number, 
		cleared_customs_date, 
		hs_code_raw, 
		hs_code_parsed,
		TRIM(SPLIT_PART(REPLACE(REPLACE(t2.hs_code_parsed,'.',''),'|',','),',',1)) AS hs_code_clean, -- 10 digital hs code
		TRIM(SPLIT_PART(REPLACE(REPLACE(t2.hs_code_parsed,'.',''),'|',','),',',2)) AS hs_code2, -- 8 digital hs code
		TRIM(
			REGEXP_REPLACE(
				REPLACE(
					UPPER(CONCAT(REGEXP_REPLACE(TRIM(SPLIT_PART(t2.sku,',',3)), '[ ]', ''),  -- Extract 3rd SKU element, remove space
					REGEXP_REPLACE(TRIM(SPLIT_PART(t2.sku,',',4)), '[-]', '') -- Extract 4th SKU element, Concat 3rd and 4th with ''
					)),CHR(13),'') -- Remove New Line element
				,'[ ]','')
			) AS alliancestylecode -- Remove any space if exists. RESULT EXAMPLE: MCK16-F21CARAMELBROWN
	FROM 
		SKULEVELTEMP t2
	WHERE 
		hs_code_parsed <> ''
	), 

FPOMAP AS (
	SELECT 
		UPPER(TRIM(t6.masterfponum)) AS masterfponum, 
		COALESCE(
			NULLIF(REGEXP_REPLACE(UPPER(TRIM(t6.alliancestylecode)),'[ ]',''),''), -- Remove Space from factorypo.alliancecode
			UPPER(COALESCE(NULLIF(REGEXP_REPLACE(TRIM(t5.alliancestylecode),'[ ]',''),''), -- Remove Space from poitem.alliancecode
			CONCAT(REGEXP_REPLACE(TRIM(t5.stylenum), '[ ]', '-'), TRIM(t5.color))
			))) AS alliancestylecode,
		MAX(t5.productcode) AS productcode
	FROM 
		mars__alliance.factorypo t6 
	INNER JOIN 
		mars__revolveclothing_com___db.poitem t5 ON t6.revolvepoid = t5.id 
	WHERE 
		t6.fpostatus = 'Received' 
	GROUP BY 
		1, 2
	) -- join the no.4 poitem and no.5 factorypo tables for the next step
	
SELECT 
	t1.id, 
	t1.flex_id, 
	t1.shipment_name, 
	t1.po_numbers, 
	t1.sku, 
	t1.product_name, 
	t1.quantity, 
	t1.entry_number, 
	t1.cleared_customs_date, 
	t1.hs_code_raw, 
	t1.hs_code_parsed,
	t1.hs_code_clean, -- 10 digital hs code
	t1.hs_code2, -- 8 digital hs code
	t1.alliancestylecode,
	t2.masterfponum, 
	t2.productcode
FROM 
	SKULEVELIMPORTS t1
LEFT JOIN 
	FPOMAP t2 ON UPPER(TRIM(t1.po_numbers)) = t2.masterfponum AND t1.alliancestylecode = t2.alliancestylecode; -- join the sku level imports data with poitem and factorypo tables. 

-------- Step 3: Join all the import data to have the final imports data --------

DROP TABLE IF EXISTS bi_work.mengmeng_dd_importsfinal_2023Q2;
CREATE TABLE bi_work.mengmeng_dd_importsfinal_2023Q2 AS
WITH TEMP AS ( -- prepare the dd_import_2023Q2_raw data (the drawback_report data from Gavin) for joining to the sku level data. 
	SELECT 
		t1.entry_no,
		t1.mot,
		t1.importer,
		t1.import_date,
		t1.entry_date,
		t1.entry_port,
		t1.reference_no,
		t1.cust_ref_no,
		t1.po_no,
		t1.consignee,
		t1.line,
		t1.gross_weight,
		t1.c_o,
		t1.c_e,
		t1.part_no,
		t1.part_description,
		t1.tariff_no,
		t2.tariff_no AS tariff_no2, 
		t1.spi,
		t1.description,
		t1.quantity_1,
		t1.unit_1,
		t1.quantity_2,
		t1.unit_2,
		t1.entered_value,
		t1.spec_rate,
		t1.adv_rate,
		t2.adv_rate AS adv_rate2,
		t1.duty,
		t1.mpf_calculated,
		t1.total_value,
		t1.total_duty,
		t1.total_mpf,
		t1.total_hmf,
		t1.total_other_fees,
		t1.total_add,
		t1.total_cvd,
		t1.total_tax,
		t1.nafta,
		t1.recon_type,
		t1."056_cotton_fee",
		t1."501_harbor_maintenance_fee",
		t1.liquidation_date,
		t1.fda_disclaim,
		t1.id
	FROM 
		(SELECT 
			entry_no,
			mot,
			importer,
			import_date,
			entry_date,
			entry_port,
			reference_no,
			cust_ref_no,
			po_no,
			consignee,
			line,
			gross_weight,
			c_o,
			c_e,
			part_no,
			part_description,
			tariff_no, -- 10 digits hts code
			spi,
			description,
			quantity_1,
			unit_1,
			quantity_2,
			unit_2,
			entered_value,
			spec_rate,
			adv_rate,
			duty,
			mpf_calculated,
			total_value,
			total_duty,
			total_mpf,
			total_hmf,
			total_other_fees,
			total_add,
			total_cvd,
			total_tax,
			nafta,
			recon_type,
			"056_cotton_fee",
			"501_harbor_maintenance_fee",
			liquidation_date,
			fda_disclaim,
			id
		FROM 
			bi_work.dd_imports_2023Q2_raw t1
		WHERE 
			t1.tariff_no NOT IN ('99038803','99038809','99038815','99038841') AND t1.import_date >= '2020-04-01') t1
	LEFT JOIN 
		(SELECT 
			entry_no,
			mot,
			importer,
			import_date,
			entry_date,
			entry_port,
			reference_no,
			cust_ref_no,
			po_no,
			consignee,
			line,
			gross_weight,
			c_o,
			c_e,
			part_no,
			part_description,
			tariff_no,
			spi,
			description,
			quantity_1,
			unit_1,
			quantity_2,
			unit_2,
			entered_value,
			spec_rate,
			adv_rate,
			duty,
			mpf_calculated,
			total_value,
			total_duty,
			total_mpf,
			total_hmf,
			total_other_fees,
			total_add,
			total_cvd,
			total_tax,
			nafta,
			recon_type,
			"056_cotton_fee",
			"501_harbor_maintenance_fee",
			liquidation_date,
			fda_disclaim,
			id
		FROM 
			bi_work.mengmeng_dd_imports_2023Q2_raw t1
		WHERE 
			t1.tariff_no IN ('99038803','99038815','99038841') /* intentionally remove 9903.88.09 - looks like incorrect entry) */
			AND t1.import_date >= '2020-04-01') t2 ON t1.entry_no = t2.entry_no AND t1."line" = t2."line"
		) 

SELECT 
	t1.entry_no, 
	t1.entry_port, 
	t1.import_date, 
	t1.importer, 
	t1.total_value, 
	t1.total_duty, 
	t1.total_mpf, 
	t1.total_hmf, 
	t1.line, 
	t1.tariff_no AS hs_code, 
	t1.tariff_no2 AS hs_code2,
	t1.quantity_1, 
	t1.unit_1,
	t1.entered_value,
	t1.adv_rate, 
	t1.adv_rate2, 
	ISNULL(t1."056_cotton_fee",0) + ISNULL(t1."501_harbor_maintenance_fee",0) AS other_fees,
	t2.po_numbers, 
	t2.flex_id, 
	t2.productcode, 
	t2.alliancestylecode, 
	t1.description,
	t2.quantity AS skuquantity, 
	CASE 
		WHEN t1.unit_1 IN ('PRS') THEN 'PRS' 
		WHEN t1.unit_1 IN ('DOZ','NO') THEN 'PCS' 
		ELSE '' 
	END AS sku_uom, 
	t1.entered_value/NULLIF(ROUND(t1.quantity_1 * case when t1.unit_1 = 'DOZ' then 12 else 1 end, 0),0) AS importunitprice,
	t1.Entry_Date
FROM 
	TEMP t1 
LEFT JOIN 
	bi_work.mengmeng_dd_import_sku_2023Q2_map t2 ON t1.entry_no = t2.entry_number AND t1.tariff_no = t2.hs_code_clean
WHERE 
	t1.tariff_no2 IS NULL
UNION ALL
SELECT 
	t1.entry_no, 
	t1.entry_port, 
	t1.import_date, 
	t1.importer, 
	t1.total_value, 
	t1.total_duty, 
	t1.total_mpf, 
	t1.total_hmf, 
	t1.line, 
	t1.tariff_no AS hs_code, 
	t1.tariff_no2 AS hs_code2,
	t1.quantity_1, 
	t1.unit_1,
	t1.entered_value,
	t1.adv_rate, 
	t1.adv_rate2, 
	ISNULL(t1."056_cotton_fee",0) + ISNULL(t1."501_harbor_maintenance_fee",0) AS other_fees,
	t2.po_numbers, 
	t2.flex_id, 
	t2.productcode, 
	t2.alliancestylecode, 
	t1.description,
	t2.quantity AS skuquantity, 
	CASE 
		WHEN t1.unit_1 IN ('PRS') THEN 'PRS' 
		WHEN t1.unit_1 IN ('DOZ','NO') THEN 'PCS' 
		ELSE '' 
	END AS sku_uom, 
	t1.entered_value/NULLIF(ROUND(t1.quantity_1 * case when t1.unit_1 = 'DOZ' then 12 else 1 end, 0), 0) AS importunitprice,
	t1.Entry_Date
FROM 
	TEMP t1 
LEFT JOIN 
	bi_work.mengmeng_dd_import_sku_2023Q2_map t2 ON t1.entry_no = t2.entry_number AND t1.tariff_no = t2.hs_code_clean AND t1.tariff_no2 = t2.hs_code2
WHERE 
	t1.tariff_no2 IS NOT NULL;

--------- Step 4: Exclude the ineligibles to consolidate the import data -------

CREATE TABLE bi_work.mengmeng_dd_import_outputfile_ALL (like bi_work.dd_import_outputfile_ALL);
DELETE FROM bi_work.mengmeng_dd_import_outputfile_ALL WHERE Entry_Date >= '2023-04-01' and Entry_Date < '2023-07-01';
INSERT INTO bi_work.mengmeng_dd_import_outputfile_ALL
SELECT 
	* 
FROM 
	bi_work.mengmeng_dd_importsfinal_2023Q2
WHERE 
	Entry_Date >= '2023-04-01' and Entry_Date < '2023-07-01'
	AND entry_no NOT IN (SELECT entry_number FROM bi_work.dd_import_skulevel_2023Q2_raw WHERE LEFT(UPPER(TRIM(po_numbers)),5) IN ('BENNE','SOVSK','LANIA','MASON','EXFTC') OR po_numbers ILIKE '%EXFTC%' OR po_numbers ILIKE '%SOVSK%'); -- EXCLUDES ineligible imports because of id method (ie. vendors 'BENNE','SOVSK','LANIA','MASON','EXFTC' for 2019 imports and prior).

SELECT 
	* 
FROM 
	bi_work.mengmeng_dd_import_outputfile_ALL 
WHERE 
	Entry_Date >= '2023-04-01' and Entry_Date < '2023-07-01' 
ORDER BY 
	Entry_Date, entry_no, cast(line as int);

---------------------------------------------------------------------------------
-------------------------------- Exorts data part -------------------------------
---------------------------------------------------------------------------------

--------------------- Step 5: Pull all eligible exports -------------------------

DROP TABLE IF EXISTS bi_work.mengmeng_dd_eligible_exports; -- gathering serialnumber info, receivedate, cost, ponum, etc.
CREATE TABLE bi_work.mengmeng_dd_eligible_exports AS
WITH ALLSHIPMENT AS (
	SELECT 
		t4.sitename, 
		t1.invoicenum AS invoice, 
		t1.transactionid, 
		t1.shipmentid, 
		t2.trackingnumber, 
		t1.sstatus,
		t1.shippingcountry AS customercountry, 
		t7.countrycode AS customercountrycode, 
		t7.us, 
		t1.rdate::DATE AS dispatchdate, 
		DATEADD(d,1,t1.rdate)::DATE AS exportdate, 
		t2.deliverydate,
		REPLACE(t1.serialnumber, 's', '') AS serialnumber_join,
		's'||REPLACE(t1.serialnumber, 's', '') AS serialnumber,
		t1.productcode, 
		t1.ssales, 
		t2.cost, 
		t3.name AS productname,
		t3.isportfolio,  
		t5.shippingoption,
		ROW_NUMBER() OVER (PARTITION BY REPLACE(t1.serialnumber, 's', '') ORDER BY t1.rdate DESC) AS rn,
		FIRST_VALUE(t7.us) OVER (PARTITION BY REPLACE(t1.serialnumber, 's', '') ORDER BY t1.rdate rows between unbounded preceding and unbounded following) AS firstshipmentcountry_us,
		FIRST_VALUE(t1.rdate) OVER (PARTITION BY REPLACE(t1.serialnumber, 's', '') ORDER BY CASE WHEN t7.us = 1 THEN '2050-01-01' ELSE t1.rdate END rows between unbounded preceding and unbounded following) AS firstintlshipmentdate
	FROM 
		bi_report.shipmentnumber_rs t1
	INNER JOIN 
		mars__revolveclothing_com___db.shipment t2 ON t1.shipmentid = t2.shipmentid
	INNER JOIN 
		mars__revolveclothing_com___db.ORDERS t5 ON t2.transactionid = t5.transactionid
	LEFT OUTER JOIN 
		bi_report.siteflag t4 ON t1.ordertype = t4.ordertype
	LEFT JOIN 
		bi_work.fw_countryregion t7 ON t1.shippingcountry = t7.country
	LEFT JOIN 
		mars__revolveclothing_com___db.product t3 ON t1.productcode = UPPER(TRIM(t3.code))
	WHERE 
		t1.rdate >= '2000-01-01' 
		-- AND t3.isportfolio = 1
		-- t1.rdate >= DATE_TRUNC('month',DATEADD(YEAR,-5,CURRENT_DATE)) AND t1.rdate < DATE_TRUNC('quarter',DATEADD(QUARTER,-1,CURRENT_DATE))
		AND REPLACE(t1.serialnumber, 's', '') <> ''
	), 

TEMP AS ( -- joined the inventoryitem, poitem, and factorypo tables together. 
	SELECT DISTINCT 
		t2.serialnumber::varchar(30) AS serialnumber, 
		t2.cost, 
		t2.ponum, 
		t6.fponum, 
		t6.masterfponum, 
		t2.receivedate, 
		COALESCE( NULLIF(REGEXP_REPLACE(UPPER(TRIM(t6.alliancestylecode)),'[ ]',''),''), UPPER(COALESCE(NULLIF(REGEXP_REPLACE(TRIM(t5.alliancestylecode),'[ ]',''),''), CONCAT(REGEXP_REPLACE(TRIM(t5.stylenum), '[ ]', '-'), TRIM(t5.color)))) ) AS alliancestylecode
	FROM 
		mars__revolveclothing_com___db.inventoryitem t2
	LEFT OUTER JOIN 
		mars__revolveclothing_com___db.poitem t5 ON UPPER(TRIM(t2.ponum)) = UPPER(TRIM(t5.ponum)) AND UPPER(TRIM(t2.product)) = UPPER(TRIM(t5.productcode))
	LEFT OUTER JOIN 
		mars__alliance.factorypo t6 ON t5.id = t6.revolvepoid AND fpostatus = 'Received' 
		AND CASE WHEN t2.ismexicoserial = 1 THEN '-MX' ELSE '-US' END = CASE WHEN RIGHT(UPPER(TRIM(t6.masterfponum)),3) IN ('-US','-MX') THEN RIGHT(UPPER(TRIM(t6.masterfponum)),3) ELSE '-US' END
	WHERE 
		t2.receivedate >= '2014-05-01' AND t2.qtyreceived = 1 -- this may include item imported prior to this date, email Gavin regarding import date
	)  -- joined the inventoryitem, poitem, and factorypo tables together. 

SELECT DISTINCT 
	t1.sitename, 
	t1.invoice, 
	t1.transactionid, 
	t1.shipmentid, 
	t1.trackingnumber, 
	t1.sstatus,
	t1.customercountry, 
	t1.customercountrycode, 
	t1.us, 
	t1.dispatchdate, 
	t1.exportdate, 
	t1.deliverydate,
	t1.serialnumber_join,
	t1.serialnumber,
	t1.productcode, 
	t1.ssales, 
	t1.cost, 
	t1.productname,
	t1.isportfolio,  
	t1.shippingoption,
	t1.rn,
	t1.firstshipmentcountry_us,
	t1.firstintlshipmentdate, 
	ROUND(t1.ssales, 2) AS exportprice,
	t2.ponum, 
	t2.fponum, 
	t2.masterfponum, 
	t2.receivedate,
	CASE WHEN t1.isportfolio = 1 THEN t2.alliancestylecode ELSE '' END AS alliancestylecode
FROM 
	ALLSHIPMENT t1
LEFT JOIN 
	TEMP t2 ON t1.serialnumber_join = t2.serialnumber
WHERE 
	t1.rn = 1 AND t1.us = 0 AND t1.sstatus = 'shipped'
	AND t1.customercountrycode IN ('CA','CL','MX') -- all shipments to CA, CL, and MX
	AND t1.isportfolio = 1
	AND t1.exportdate >= '2023-04-01' AND t1.exportdate < '2023-07-01'
	AND (t1.firstshipmentcountry_us = 0 OR DATEDIFF(day, t2.receivedate, t1.firstintlshipmentdate) <= 365);

---------------- Step 6: Map all the imports and exports together ---------------

DROP TABLE IF EXISTS bi_work.mengmeng_dd_import_exports_mapped;
CREATE TABLE bi_work.mengmeng_dd_import_exports_mapped AS
WITH HTSCODES AS (
	SELECT DISTINCT 
		entry_no, 
		po_numbers, 
		REGEXP_REPLACE(alliancestylecode,'[ ]','') AS alliancestylecode, -- Remove space from alliancecode imported before 2022Q4
		import_date, 
		hs_code, 
		hs_code2, 
		adv_rate, 
		adv_rate2, 
		unit_1, 
		description,
		ROW_NUMBER() OVER (PARTITION BY REGEXP_REPLACE(alliancestylecode,'[ ]',''), po_numbers ORDER BY import_date DESC) AS rn
	FROM 
		bi_work.dd_importsfinal_ALL -- all import data from Gavin Sample Report tables. 
	WHERE 
		hs_code IN (SELECT hs_code FROM bi_work.dd_import_outputfile_ALL)
	), 

EXPORTSMAPPED AS (
	SELECT 
		TIMEZONE('America/Los_Angeles', TO_TIMESTAMP(GETDATE(),'YYYY-MM-DD HH24:MI:SS'))::DATE AS pull_date, 
		t1.sitename AS exporter, 
		t1.exportdate, 
		'Air' AS carrier, 
		t1.trackingnumber, 
		t1.customercountrycode AS destination, 
		t1.productcode, 
		t1.invoice, 
		t1.serialnumber, 
		'' AS tracking2, 
		1 AS qty, 
		'PCS' as unitofmeasure,
		t2.hs_code, 
		t2.hs_code2, 
		t1.productname as description, 
		t1.exportprice, 
		t2.unit_1, 
		t2.entry_no, 
		0 as substituted,
		t1.alliancestylecode, 
		t2.import_date,
		t2.description AS importdescription, 
		t1.deliverydate
	FROM 
		bi_work.mengmeng_dd_eligible_exports t1 
	INNER JOIN 
		HTSCODES t2 ON t1.masterfponum = t2.po_numbers AND t1.alliancestylecode = t2.alliancestylecode -- here join the exports and imports
	WHERE 
		t1.isportfolio = 1 
		AND t2.rn = 1
		AND t1.exportdate >= '2023-04-01' AND t1.exportdate < '2023-07-01' 
		AND t1.customercountrycode IN ('CA','CL','MX')
	)

SELECT 
	* 
FROM 
	EXPORTSMAPPED;

---------------------------------------------------------------------------------
------------------------ Final Step: pull the data ------------------------------
---------------------------------------------------------------------------------

SELECT 
	* 
FROM 
	bi_work.mengmeng_dd_import_exports_mapped;