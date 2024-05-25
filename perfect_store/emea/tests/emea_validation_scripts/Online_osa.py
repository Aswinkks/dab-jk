# Databricks notebook source
import os
import snowflake.connector
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import sum, when, col, expr, current_timestamp, lit, split, trim, coalesce
from datetime import datetime
from pytz import timezone 
ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')

credential = DefaultAzureCredential()
#historical_load = False if dbutils.widgets.get('historical_load')=='False' else True
env = dbutils.widgets.get('env')
schema_sfl = "CORE_RAW"
key_vault_url = f'https://kv-sobg-{env}-001.vault.azure.net/'
database_sfl = f"{env}_COMX_SOBG"
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
 
snowflake_secret = secret_client.get_secret("comx-sobg-snowlfake-sa")
user_sfl = snowflake_secret.properties.tags['username']
role_sfl = snowflake_secret.properties.tags['role']
warehouse_sfl = snowflake_secret.properties.tags['warehouse']
host_sfl = snowflake_secret.properties.tags['host']
password_sfl = snowflake_secret.value
conn = snowflake.connector.connect(
    user=user_sfl,
    password=password_sfl,
    account=host_sfl[:23],
    warehouse=warehouse_sfl,
    database=database_sfl,
    schema=schema_sfl,
    role= role_sfl
    )

# COMMAND ----------


query = f"""select distinct GMC_BRAND_b1 as gmc_brand
from
  {env}_COMX_SOBG.CORE_raw.L0_gmc_mapping
where
  country = 'United Kingdom'
  and source_system = 'Profitero' and country='United Kingdom' order by 1;"""
c = conn.cursor().execute(query)
mapping_sheet = []
for gmc_brand in c:
    mapping_sheet.append(gmc_brand[0])
print(c.rowcount)


# COMMAND ----------

q1 = f"""
select
  distinct GMC_BRAND_NAME as GMC_BRAND_NAME
from
  {env}_COMX_SOBG.CORE_INTEGRATION.L1_online_osa where KV_FLAG='KV' and source_country='United Kingdom' order by 1;"""
c1 = conn.cursor().execute(q1)
l1_table = []
for (GMC_BRAND_NAME) in c1:
    l1_table.append(GMC_BRAND_NAME[0])
print(c.rowcount-c1.rowcount)
#print(l1_table)


for element in mapping_sheet:
    if element not in l1_table:
        print(element)
#There is no data coming from source for these brands
# select distinct source_category,source_brand,source,kv_flag from
#   dev_COMX_SOBG.CORE_INTEGRATION.L1_online_osa where source_country='United Kingdom'
#   and source_brand in ('Bebe','Bebe Zartpflege','Cicabiafine','Oraldene','Le Petit Marseillais','Lubriderm','Nett','Nizoral','O.B.','Penaten') and kv_flag='KV';

# COMMAND ----------

q = """select * from  (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah """
cu = conn.cursor().execute(q)
c1 =cu.rowcount
c1

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
inner join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id """)
cj1 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id """)
cj1_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
inner join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code""")
cj2 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code""")
cj2_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
inner join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code""")
cj3 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code""")
cj3_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
inner join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code""")
cj4 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code""")
cj4_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
inner join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code""")
cj5 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code""")
cj5_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code""")
cj6 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code""")
cj6_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
inner join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code""")
cj7 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code""")
cj7_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 inner join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name""")
cj8 =cu.rowcount

cu = conn.cursor().execute("""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name""")
cj8_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
inner join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country""")
cj9 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country""")
cj9_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
inner join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country""")
cj10 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country""")
cj10_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
inner join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country""")
cj11 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country""")
cj11_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country
 inner join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY""")
cj12 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY""")
cj12_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
inner join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on retailers.name = gcc.SOURCE_RETAILER_Name and pah.db_country = gcc.country """)
cj13 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on retailers.name = gcc.SOURCE_RETAILER_Name and pah.db_country = gcc.country """)
cj13_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on retailers.name = gcc.SOURCE_RETAILER_Name and pah.db_country = gcc.country 
inner join (select distinct region as region,
							GCC_CUSTOMER_INC_CODE, 
							GCC_CUSTOMER_INC_NAME,
							gcc_banner_code,
							GCC_BANNER_NAME,
							GCC_CHANNEL_L1_CODE,
							GCC_CHANNEL_L2_CODE,
							GCC_CHANNEL_L1_NAME,
							GCC_CHANNEL_L2_NAME,AG_CODE,
							case when country_iso2_code='UK' then 'United Kingdom'
								when country_iso2_code='DE' then 'Germany' end as db_country1  
                          from {database_sfl}.core_raw.L0_global_gcc where region='EMEA') gcch on gcc.gcc_banner_code = gcch.gcc_banner_code and pdc.db_country = gcch.db_country1 and gcc.GCC_CHANNEL_L2_NAME = gcch.GCC_CHANNEL_L2_NAME;""")
cj14 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc, db_country_code) as rn1 from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PRICE_AVAILABILITY_GLOBAL_DAILY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1) pah 
left join PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY customer_products on pah.customer_product_id = customer_products.id 
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) brands on customer_products.brand_id = brands.id and pah.db_country_code=brands.db_country_code
left join (select * from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) products on products.id = pah.product_id and pah.retailer_id = products.retailer_id and pah.db_country_code = products.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) retailers on pah.retailer_id = retailers.id and pah.db_country_code = retailers.db_country_code
left join (select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) catg_prd on customer_products.id = catg_prd.customer_product_id and pah.db_country_code=catg_prd.db_country_code
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) pdc on pdc.id = catg_prd.category_id and pah.db_country_code=pdc.db_country_code
left join (select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4 on pdc4.id=pdcp4.category_id) msl on customer_products.id = msl.customer_product_id and pah.db_country_code=msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pah.db_country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and mapping_quality is not NULL) gmc_mapping on brands.brand = GMC_mapping.source_brand_name and pdc.source_category = gmc_mapping.source_category_name and  pah.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) ) gmc_mapping1 on brands.brand = gmc_mapping1.source_brand_name1 and pah.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL )  gmc_mapping2 on brands.brand = gmc_mapping2.source_brand_name2 and pdc.source_category = gmc_mapping2.source_category_name2 and pdc.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pah.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on retailers.name = gcc.SOURCE_RETAILER_Name and pah.db_country = gcc.country 
left join (select distinct region as region,
							GCC_CUSTOMER_INC_CODE, 
							GCC_CUSTOMER_INC_NAME,
							gcc_banner_code,
							GCC_BANNER_NAME,
							GCC_CHANNEL_L1_CODE,
							GCC_CHANNEL_L2_CODE,
							GCC_CHANNEL_L1_NAME,
							GCC_CHANNEL_L2_NAME,AG_CODE,
							case when country_iso2_code='UK' then 'United Kingdom'
								when country_iso2_code='DE' then 'Germany' end as db_country1  
                          from {database_sfl}.core_raw.L0_global_gcc where region='EMEA') gcch on gcc.gcc_banner_code = gcch.gcc_banner_code and pdc.db_country = gcch.db_country1 and gcc.GCC_CHANNEL_L2_NAME = gcch.GCC_CHANNEL_L2_NAME;""")
cj14_1 =cu.rowcount

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,StructField,IntegerType,LongType,DateType
schema = StructType([ \
    StructField("test_time",StringType(),True), \
    StructField("table_name",StringType(),True), \
    StructField("current_count",StringType(),True), \
    StructField("inner_join_count",StringType(),True),\
    StructField("left_join_count",IntegerType(),True), \
    StructField("inner_join_ratio",StringType(),True),\
    StructField("left_join_ratio",StringType(),True)
  ])
data2 = [(ind_time,"PROFITERO_FACT_PRICE_AVAILABILITY",c1,None,None,None,None),
    (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
    (ind_time,"DIM_brands_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
    (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
    (ind_time,"DIM_retailers_EMEA",cj3_1,cj4,cj4_1,round(cj4/cj3_1,2),round(cj4_1/cj3_1,2)),
    (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj4_1,cj5,cj5_1,round(cj5/cj4_1,2),round(cj5_1/cj4_1,2)),
    (ind_time,"DIM_CATEGORIES_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
    (ind_time,"msl_flag_emea",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
    (ind_time,"AGH_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
    (ind_time,"GMC_EMEA_mapping",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
    (ind_time,"GMC_EMEA_mapping1",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
    (ind_time,"GMC_EMEA_mapping2",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
    (ind_time,"gmch_global_emea",cj11_1,cj12,cj12_1,round(cj12/cj11_1,5),round(cj12_1/cj11_1,2)),
    (ind_time,"GCC_EMEA_mapping",cj12_1,cj13,cj13_1,round(cj13/cj12_1,2),round(cj13_1/cj12_1,2)),
    (ind_time,"gcch_global_emea",cj13_1,cj14,cj14_1,round(cj14/cj13_1,2),round(cj14_1/cj13_1,2))]
df14 = spark.createDataFrame(data2,schema)
df14.show(truncate=False)
conn.close()
#GMC_EMEA_mapping1  GMC_EMEA_mapping2

# COMMAND ----------


# pah_query = f"""select * from (select *,row_number() over(partition by customer_product_id,DB_COUNTRY order by date desc,        db_country_code) as rn1 from {env}_COMX_SOBG.CORE_RAW.L0_PROFITERO_FACT_PRICE_AVAILABILITY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false')a where a.rn1=1"""
# pah_df = spark.read.format("snowflake") \
#     .option("host",host_sfl) \
#     .option("user", user_sfl) \
#     .option('role', role_sfl) \
#     .option("password", password_sfl) \
#     .option("database", database_sfl) \
#     .option("sfWarehouse", warehouse_sfl) \
#     .option("query", pah_query)\
#     .option("partitionColumn", "customer_product_id")\
#     .option("lowerBound", "1020817823")\
#     .option("upperBound", "1668232588")\
#     .option("numPartitions", "100")\
#     .load()
# c1 = pah_df.count()


# COMMAND ----------

# customer_products_df = spark.sql("select * from global_temp.DIM_CUSTOMER_PRODUCTS_EMEA")
# products_df = spark.sql("select * from global_temp.PROFITERO_DIM_PRODUCTS_EMEA")
# retailers_df = spark.sql("select * from global_temp.DIM_retailers_EMEA")
# catg_prd_df = spark.sql("select * from global_temp.DIM_CATEGORY_PRODUCT_EMEA")
# pdc_df = spark.sql("select * from global_temp.DIM_CATEGORIES_EMEA")
# brands_df = spark.sql("select * from global_temp.DIM_brands_EMEA")
# agh_df = spark.sql("select * from global_temp.AGH_EMEA")
# gmc_df = spark.sql("select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_name from global_temp.GMC_EMEA_mapping where source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL")
# gmch_df = spark.sql("select * from global_temp.gmch_global_emea")
# gcc_df = spark.sql("select * from global_temp.GCC_EMEA_mapping where source_system='Profitero'")
# gcch_df = spark.sql("select * from global_temp.gcch_global_emea")
# msl_df = spark.sql("select * from global_temp.msl_flag_emea")
# #clt_df = spark.sql("select * from global_temp.categories_lookup_table")
# gmc_mapping_df1 = spark.sql("select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country  from global_temp.GMC_EMEA_mapping where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL)  ")
# gmc_mapping_df2 = spark.sql("""select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2  from global_temp.GMC_EMEA_mapping where source_system='Profitero'
# and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL""")

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'inner')
# cj1=df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')
# cj1_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'inner')
# cj2=df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')
# cj2_1=df_int1_1.count()


# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'inner')
# cj3=df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')
# cj3_1=df_int1_1.count()


# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'inner')
# cj4 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')
# cj4_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'inner')
# cj5 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')
# cj5_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'inner')

# cj6 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')

# cj6_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'inner')
# cj7 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')
# cj7_1 = df_int1_1.count()

# COMMAND ----------


# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(clt_df,((pah_df['customer_product_id']==clt_df['id']) ),'inner')
# cj8 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(clt_df,((pah_df['customer_product_id']==clt_df['id']) ),'left')
# cj8_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'inner')
# cj8 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')
# cj8_1 = df_int1_1.count()

# COMMAND ----------

# .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
#                   .join(gmch_global_df,((trim(gmc_mapping_df['source_category_name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'inner')
# cj9 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')
# cj9_1 = df_int1_1.count()

# COMMAND ----------

# .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
#                   .join(gmch_global_df,((trim(gmc_mapping_df['source_category_name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'inner')

# cj10 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')

# cj10_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'inner')

# cj11 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')

# cj11_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'inner')
# cj12 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')
# cj12_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(gmc_mapping_df1,((trim(brands_df['brand']) == trim(gmc_mapping_df1['source_brand_name1']))  & (pah_df['db_country']==gmc_mapping_df1['country'])),'inner')
# cj13 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(gmc_mapping_df1,((trim(brands_df['brand']) == trim(gmc_mapping_df1['source_brand_name1']))  & (pah_df['db_country']==gmc_mapping_df1['country'])),'left')
# cj13_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(gmc_mapping_df1,((trim(brands_df['brand']) == trim(gmc_mapping_df1['source_brand_name1']))  & (pah_df['db_country']==gmc_mapping_df1['country'])),'left')\
#                 .join(gmc_mapping_df2,((trim(brands_df['brand']) == trim(gmc_mapping_df2['source_brand_name2'])) & (trim(pah_df['db_country'])==trim(gmc_mapping_df2['country'])) & (pdc_df['source_category'] == gmc_mapping_df2['source_category_name2']) &  (pdc_df['source_sub_category'] == gmc_mapping_df2['SOURCE_SUBCATEGORY_NAME2'])),'inner')
# cj14 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                  .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (pdc_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(gmc_mapping_df1,((trim(brands_df['brand']) == trim(gmc_mapping_df1['source_brand_name1']))  & (pah_df['db_country']==gmc_mapping_df1['country'])),'left')\
#                 .join(gmc_mapping_df2,((trim(brands_df['brand']) == trim(gmc_mapping_df2['source_brand_name2'])) & (trim(pah_df['db_country'])==trim(gmc_mapping_df2['country'])) & (pdc_df['source_category'] == gmc_mapping_df2['source_category_name2']) &  (pdc_df['source_sub_category'] == gmc_mapping_df2['SOURCE_SUBCATEGORY_NAME2'])),'left')
# cj14_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(clt_df,((pah_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(clt_df,((pah_df['customer_product_id']==clt_df['id']) ),'left')
# cj13 = df_int1.count()

# df_int1_1 = pah_df.join(customer_products_df,pah_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pah_df['db_country_code']==brands_df['db_country_code'])),'left')\
#                 .join(products_df,((products_df['id']==pah_df['product_id']) & (pah_df['retailer_id']==products_df['retailer_id']) &  (pah_df['db_country_code']==products_df['db_country_code'])),'left')\
#                 .join(retailers_df,((pah_df['retailer_id']==retailers_df['id']) & (pah_df['db_country_code']==retailers_df['db_country_code'])),'left')\
#                 .join(catg_prd_df,((customer_products_df['id']==catg_prd_df['customer_product_id']) & (customer_products_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(pdc_df,((pdc_df['id']==catg_prd_df['category_id']) & (pdc_df['db_country_code']==catg_prd_df['db_country_code'])),'left')\
#                 .join(msl_df,((customer_products_df['id']==msl_df['customer_product_id']) & (pah_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 .join(clt_df,((pah_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(agh_df,(pdc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pdc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pah_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(clt_df,((pah_df['customer_product_id']==clt_df['id']) ),'inner')
# cj13_1 = df_int1_1.count()

# COMMAND ----------



# COMMAND ----------


