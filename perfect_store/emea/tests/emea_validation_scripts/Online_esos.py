# Databricks notebook source
import os
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import (
    sum,
    when,
    col,
    expr,
    current_timestamp,
    lit,
    split,
    trim,
    coalesce,
)
import snowflake.connector

credential = DefaultAzureCredential()
# historical_load = False if dbutils.widgets.get('historical_load')=='False' else True
schema_sfl = "core_integration"
env = dbutils.widgets.get("env")
key_vault_url = f"https://kv-sobg-{env}-001.vault.azure.net/"
database_sfl = f"{env}_COMX_SOBG"
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

snowflake_secret = secret_client.get_secret("comx-sobg-snowlfake-sa")
user_sfl = snowflake_secret.properties.tags["username"]
role_sfl = snowflake_secret.properties.tags["role"]
warehouse_sfl = snowflake_secret.properties.tags["warehouse"]
host_sfl = snowflake_secret.properties.tags["host"]
password_sfl = snowflake_secret.value
conn = snowflake.connector.connect(
    user=user_sfl,
    password=password_sfl,
    account=host_sfl[:23],
    warehouse=warehouse_sfl,
    database=database_sfl,
    schema=schema_sfl,
    role=role_sfl,
)
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
  {env}_COMX_SOBG.CORE_INTEGRATION.L1_online_esos where KV_FLAG='KV' and source_country='United Kingdom' order by 1;"""
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
#   dev_COMX_SOBG.CORE_INTEGRATION.L1_online_esos where source_country='United Kingdom'
#   and source_brand in ('Bebe','Cicabiafine','Oraldene','Le Petit Marseillais','Lubriderm','Nett','Nizoral','O.B.','Penaten','Stugeron') and kv_flag='KV';

# COMMAND ----------

from datetime import datetime
from pytz import timezone 
ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
print(ind_time)

# COMMAND ----------

q = """select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp"""
cu = conn.cursor().execute(q)
c1 =cu.rowcount
c1

# COMMAND ----------

# DBTITLE 1,rankings --> cj1
cu = conn.cursor().execute("""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
inner join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
""")
cj1 =cu.rowcount

cu = conn.cursor().execute("""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
 """)
cj1_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,ranking products--> cj2
cu = conn.cursor().execute("""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
inner join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id""")
cj2 =cu.rowcount

cu = conn.cursor().execute("""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id""")
cj2_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,products--> cj3
cu = conn.cursor().execute("""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
inner join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id""")
cj3 =cu.rowcount

cu = conn.cursor().execute("""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id""")
cj3_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,cpl--> cj4
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id""")
cj4 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
left join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id""")
cj4_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,customer_products--> cj5
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
inner join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id""")
cj5 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id""")
cj5_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,category_products--> cj6
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
inner join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id""")
cj6 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id""")
cj6_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,categories--> cj7
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code""")
cj7 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
left join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code""")
cj7_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,brands --> cj8
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
inner join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country""")
cj8 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country""")
cj8_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,retailers--> cj9
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
inner join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country""")
cj9 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country""")
cj9_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,msl --> cj10
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
inner join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code""")
cj10 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code""")
cj10_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,ag--> cj11
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 inner join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 inner join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name""")
cj11 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name""")
cj11_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,gmc_mapping--> cj12
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 inner join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country """)
cj12 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country """)
cj12_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,gmc_mapping1 --> cj13
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
inner join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country""")
cj13 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country""")
cj13_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,gmc_mapping2--> cj14
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
inner join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country""")
cj14 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country""")
cj14_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,gmch --> cj15
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country
 inner join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY""")
cj15 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country""")
cj15_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,gcc_mapping --> cj16
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
inner join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on pdr.name = gcc.SOURCE_RETAILER_Name and pdc1.db_country = gcc.country """)
cj16 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on pdr.name = gcc.SOURCE_RETAILER_Name and pdc1.db_country = gcc.country """)
cj16_1 =cu.rowcount

# COMMAND ----------

# DBTITLE 1,gcch --> cj17
cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on pdr.name = gcc.SOURCE_RETAILER_Name and pdc1.db_country = gcc.country 
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
                          from {database_sfl}.core_raw.L0_global_gcc where region='EMEA') gcch on gcc.gcc_banner_code = gcch.gcc_banner_code and pdc1.db_country = gcch.db_country1 and gcc.GCC_CHANNEL_L2_NAME = gcch.GCC_CHANNEL_L2_NAME """)
cj17 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select Ranking_ID,Ranking_Product_ID,Date,Page_Placement,Actual_rank,Organic_rank,Sponsored_rank,Sponsored,db_country,updated_at,DB_COUNTRY_CODE from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id,page_placement,actual_rank,db_country order by db_country_code) as rn from PROD_CONSUMER360_NONPII.digitalshelf_raw.T_RPT_PROFITERO_FACT_PLACEMENT_GLOBAL_DAILY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1) pfp
left join 
(select id,name,type from (select *,row_number()over(partition by id order by DB_COUNTRY) as rn1 from 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKINGS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn1=1) pdrk on pfp.ranking_id = pdrk.id
left join (select id,retailer_id from (select *,row_number() over(partition by id order by DB_COUNTRY) as rn2 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RANKING_PRODUCTS_GLOBAL_WEEKLY   where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false')a where rn2=1) pdrp on pfp.ranking_product_id = pdrp.id
left join 
(select id,ranking_product_id from (select *,row_number()over(partition by id,DB_COUNTRY order by DB_COUNTRY_code) as rn3 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_PRODUCTS_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany')  and is_deleted = 'false') a1 where rn3=1) pdp 
 on pfp.ranking_product_id=pdp.ranking_product_id
inner join 
(select distinct * from {database_sfl}.CORE_RAW.L0_Customer_Product_lookup_table) cpl on pdp.id = cpl.product_id
left join 
PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CUSTOMER_PRODUCTS_GLOBAL_WEEKLY pdcp on pdcp.id = cpl.customer_product_id
left join
(select * from (select *,row_number() over (partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) pdcp1 on pdcp1.customer_product_id = cpl.customer_product_id
inner join (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1 and length(TRIM(SPLIT_PART(full_name, '***',2)))>0 ) pdc1 on pdcp1.category_id = pdc1.id and pfp.db_country_code = pdc1.db_country_code
left join (select * from (select *,row_number()over(partition by id,db_country order by DB_COUNTRY_code ) as rn5 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_STG_PROFITERO_DIM_BRANDS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and owner != '') a where rn5=1) pdb on pdcp.brand_id = pdb.id and pdc1.db_country = pdb.db_country
left join (select * from (select *,row_number() over(partition by id,db_country order by DB_COUNTRY_code ) as rn6 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_RETAILERS_GLOBAL_WEEKLY  where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a where rn6=1) pdr on pdrp.retailer_id = pdr.id and pdc1.db_country = pdr.db_country
left join 
(select distinct customer_product_id, 'Y' as MSL_FLAG,db_country_code from (select * from (select *,row_number() over(partition by id,DB_COUNTRY order by db_country_code) as rn from (select *,TRIM(SPLIT_PART(full_name, '***',2)) AS source_category,
TRIM(SPLIT_PART(full_name, '***',3)) AS source_sub_category from (select *,row_number() over(partition by id, db_country order by db_country_code) as rn10 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORIES_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn10=1) a1 where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' and full_name ilike '%spike%') a where a.rn=1 ) pdc4
 left join
 (select * from (select customer_product_id, category_id,row_number() over(partition by category_id,customer_product_id,DB_COUNTRY order by db_country_code) as rn1 from (select * from (select *,row_number()over(partition by customer_product_id order by updated_at desc) as rn4 from PROD_CONSUMER360_NONPII.DIGITALSHELF_RAW.T_RPT_PROFITERO_DIM_CATEGORY_PRODUCT_GLOBAL_WEEKLY where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') a1 where rn4=1) a where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false') where rn1=1) pdcp4
 on pdc4.id=pdcp4.category_id) msl on pdcp.id = msl.customer_product_id and pdcp.db_country_code = msl.db_country_code
 left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on pfp.DB_COUNTRY = agh.ag_long_name
 left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL and  mapping_quality is not NULL) gmc_mapping on 
 pdb.brand = GMC_mapping.source_brand_name and pdc1.source_category = gmc_mapping.source_category_name and pfp.db_country = gmc_mapping.country
left join (select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME1
  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a2 where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL) )  gmc_mapping1 on pdb.brand = gmc_mapping1.source_brand_name1 and pfp.db_country = gmc_mapping1.country
left join (select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2,GMC_SUBCATEGORY_NAME2  from (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name,GMC_BRAND_B1 as GMC_BRAND_name, SOURCE_SUBCATEGORY_NAME,GMC_SUBCATEGORY_NAME as GMC_SUBCATEGORY_NAME2
 from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA' and mapping_quality is not NULL) a3 where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL ) gmc_mapping2 on pdb.brand = gmc_mapping2.source_brand_name2 and pdc1.source_category = gmc_mapping2.source_category_name2 and pdc1.source_sub_category = gmc_mapping2.SOURCE_SUBCATEGORY_NAME2 and  pfp.db_country = gmc_mapping2.country
 left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,c5_subcategory_code,C5_SUBCATEGORY  from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and coalesce(gmc_mapping2.GMC_BRAND_name2,gmc_mapping.GMC_BRAND_name,gmc_mapping1.GMC_BRAND_name1) = gmch.b1_brand and coalesce(gmc_mapping2.GMC_SUBCATEGORY_NAME2,gmc_mapping1.GMC_SUBCATEGORY_NAME1) = gmch.C5_SUBCATEGORY
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Profitero' ) gcc on pdr.name = gcc.SOURCE_RETAILER_Name and pdc1.db_country = gcc.country 
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
                          from {database_sfl}.core_raw.L0_global_gcc where region='EMEA') gcch on gcc.gcc_banner_code = gcch.gcc_banner_code and pdc1.db_country = gcch.db_country1 and gcc.GCC_CHANNEL_L2_NAME = gcch.GCC_CHANNEL_L2_NAME """)
cj17_1 =cu.rowcount

# COMMAND ----------

data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
    (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
    (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
    (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
    (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
    (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
    (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
    (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
    (ind_time,"DIM_brands_EMEA",cj7,cj8,cj8_1,round(cj8/cj7,5),round(cj8_1/cj7,2)),
    (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
    (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
    (ind_time,"AGH",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
    (ind_time,"GMC_mapping",cj11_1,cj12,cj12_1,round(cj12/cj11_1,2),round(cj12_1/cj11_1,2)),
    (ind_time,"GMC_mapping1",cj12_1,cj13,cj13_1,round(cj13/cj12_1,2),round(cj13_1/cj12_1,2)),
    (ind_time,"GMC_mapping2",cj13_1,cj14,cj14_1,round(cj14/cj13_1,2),round(cj14_1/cj13_1,2)),
    (ind_time,"GCCH_mapping",cj14_1,cj15,cj15_1,round(cj15/cj14_1,2),round(cj15_1/cj14_1,2)),
    (ind_time,"GCC_mapping",cj15_1,cj16,cj16_1,round(cj16/cj15_1,2),round(cj16_1/cj15_1,2)),
    (ind_time,"GCCH_mapping",cj16_1,cj17,cj17_1,round(cj17/cj16_1,2),round(cj17_1/cj16_1,2))]
df14 = spark.createDataFrame(data2,schema)
df14.show(truncate=False)
conn.close()
#GMC_mapping1,GMC_mapping2

# COMMAND ----------

# from pyspark.sql.functions import col
# pfpc_query  = f"select * from {env}_COMX_SOBG.CORE_RAW.L0_PROFITERO_FACT_PRODUCTS_CONTENT where db_country in ('United Kingdom','Germany');"
# pfpc_df = spark.read.format("snowflake") \
#     .option("host",host_sfl) \
#     .option("user", user_sfl) \
#     .option('role', role_sfl) \
#     .option("password", password_sfl) \
#     .option("database", database_sfl) \
#     .option("sfWarehouse", warehouse_sfl) \
#     .option("schema",schema_sfl)\
#     .option("query",pfpc_query)\
#     .load()
# c1=pfpc_df.count()

# COMMAND ----------

# customer_products_df = spark.sql("select * from global_temp.DIM_CUSTOMER_PRODUCTS_EMEA")
# brands_df = spark.sql("select * from global_temp.DIM_brands_EMEA")
# retailers_df = spark.sql("select * from global_temp.DIM_retailers_EMEA")
# catg_prd_df = spark.sql("select * from global_temp.DIM_CATEGORY_PRODUCT_EMEA")
# pdc_df = spark.sql("select * from global_temp.DIM_CATEGORIES_EMEA")
# #products_df = spark.sql("select * from global_temp.PROFITERO_DIM_PRODUCTS_EMEA")
# agh_df = spark.sql("select * from global_temp.AGH_EMEA")
# gmc_df = spark.sql("select * from global_temp.GMC_EMEA_mapping where source_system='Profitero'")
# gmch_df = spark.sql("select * from global_temp.gmch_global_emea")
# gcc_df = spark.sql("select * from global_temp.GCC_EMEA_mapping where source_system='Profitero'")
# gcch_df = spark.sql("select * from global_temp.gcch_global_emea")
# msl_df = spark.sql("select * from global_temp.msl_flag_emea")
# clt_df = spark.sql("select * from global_temp.categories_lookup_table")

# COMMAND ----------

# from pyspark.sql.types import StructType,StringType,StructField,IntegerType,LongType,DateType
# schema = StructType([ \
#     StructField("test_time",StringType(),True), \
#     StructField("table_name",StringType(),True), \
#     StructField("current_count",StringType(),True), \
#     StructField("left_join_count",StringType(),True),\
#     StructField("inner_join_count",IntegerType(),True), \
#     StructField("left_join_ratio",StringType(),True),\
#     StructField("inner_join_ratio",StringType(),True)
#   ])


# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'inner')
# cj1=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')
# cj1_1=df_int1_1.count()



# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'inner')
# cj2=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')
# cj2_1=df_int1_1.count()


# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'inner')
                
# cj3=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')
# cj3_1=df_int1_1.count()


# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'inner')
# cj4=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')
# cj4_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'inner')
# cj5=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')
# cj5_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'inner')

# cj6=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')

# cj6_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'inner')

# cj7=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')

# cj7_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'inner')

# cj8=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')

# cj8_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'inner')

# cj9=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')

# cj9_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pfpc_df['db_country']==gcc_df['country'])),'inner')
# cj10=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pfpc_df['db_country']==gcc_df['country'])),'left')
# cj10_1=df_int1_1.count()


# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pfpc_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'inner')
# cj11=df_int1.count()

# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pfpc_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')
# cj11_1=df_int1_1.count()

# COMMAND ----------

# df_int1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pfpc_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(msl_df,((pfpc_df['customer_product_id']==msl_df['customer_product_id']) & (pfpc_df['db_country_code']==msl_df['db_country_code'])),'inner')
# cj12=df_int1.count()


# df_int1_1 = pfpc_df.join(customer_products_df,pfpc_df['customer_product_id'] == customer_products_df['id'],'left')\
#                 .join(brands_df,((customer_products_df['brand_id']==brands_df['id']) & (pfpc_df['db_country']==brands_df['db_country'])),'left')\
#                 .join(retailers_df,((pfpc_df['retailer_id']==retailers_df['id']) &\
#                                (pfpc_df['db_country']==retailers_df['db_country'])),'left')\
#                 .join(catg_prd_df,(pfpc_df['customer_product_id']==catg_prd_df['customer_product_id']),'left')\
#                 .join(pdc_df,((catg_prd_df['category_id']==pdc_df['id']) & (catg_prd_df['db_country']==pdc_df['db_country'])),'left')\
#                 .join(agh_df,(pfpc_df['DB_COUNTRY']==agh_df['ag_long_name']) ,'left')\
#                 .join(clt_df,((pfpc_df['customer_product_id']==clt_df['id']) ),'left')\
#                 .join(gmc_df,((brands_df['brand']==gmc_df['source_brand_name']) & (pfpc_df['db_country']==gmc_df['country']) & (clt_df['source_category'] == gmc_df['source_category_name'])),'left')\
#                 .join(gmch_df,((gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY']) & (brands_df['brand'] == gmch_df['GMC_BRAND_NAME'] ) ) ,'left')\
#                 .join(gcc_df,((retailers_df['name']==gcc_df['SOURCE_RETAILER_Name']) & (pfpc_df['db_country']==gcc_df['country'])),'left')\
#                 .join(gcch_df,((gcc_df['GCC_CHANNEL_L2_NAME']==gcch_df['GCC_CHANNEL_L2_NAME']) & (retailers_df['db_country']==gcch_df['db_country1']) & (gcc_df['gcc_banner_code']==gcch_df['gcc_banner_code'])),'left')\
#                 .join(msl_df,((pfpc_df['customer_product_id']==msl_df['customer_product_id']) & (pfpc_df['db_country_code']==msl_df['db_country_code'])),'left')
# cj12_1=df_int1_1.count()

# COMMAND ----------


