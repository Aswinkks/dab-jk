# Databricks notebook source
# MAGIC %md
# MAGIC Connection details

# COMMAND ----------

import os
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import sum, when, col, expr, current_timestamp, lit, trim
import snowflake.connector

credential = DefaultAzureCredential()
# historical_load = False if dbutils.widgets.get('historical_load')=='False' else True
env = dbutils.widgets.get("env")
schema_sfl = "CORE_INTEGRATION"
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

# COMMAND ----------


query = f"""select distinct GMC_BRAND_b1 as gmc_brand
from
  {env}_COMX_SOBG.CORE_raw.L0_gmc_mapping
where
  country = 'United Kingdom'
  and source_system = 'Perfect Store Dashboard' and country='United Kingdom' order by 1;"""
c = conn.cursor().execute(query)
mapping_table = []
for (gmc_brand) in c:
    mapping_table.append(gmc_brand[0])
print(c.rowcount)

# COMMAND ----------

q1 = f"""
select
  distinct GMC_BRAND_NAME as GMC_BRAND_NAME
from
  {env}_COMX_SOBG.CORE_INTEGRATION.L1_OFFLINE_SOS_OSA_HE where KV_FLAG='KV' and source_country='United Kingdom' order by 1;"""
c1 = conn.cursor().execute(q1)
l1_table = []
for (GMC_BRAND_NAME) in c1:
    l1_table.append(GMC_BRAND_NAME[0])
print(c1.rowcount)
for i in mapping_table:
    if i not in l1_table:
        print(i)
#('Actifed / Actigrip',)--missing
#('Hexoral / Oraldene',) --missing
#('Sinutab',) ---missing
#these 3 source brands are not present in tsku source table 
# select * from
#   dev_COMX_SOBG.CORE_INTEGRATION.L1_OFFLINE_SOS_OSA_HE where source_country='United Kingdom'
#   and source_brand in ('Oraldine','Actifed','Sinutab Ii')

# COMMAND ----------

q = f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi """
cu = conn.cursor().execute(q)
c1 =cu.rowcount
c1

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
inner join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name """)
cj1 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name """)
cj1_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
inner join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME """)
cj2 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME """)
cj2_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME
inner join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and gmc_mapping.GMC_BRAND_name = gmch.b1_brand """)
cj3 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME
left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and gmc_mapping.GMC_BRAND_name = gmch.b1_brand """)
cj3_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME
left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and gmc_mapping.GMC_BRAND_name = gmch.b1_brand
inner join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Perfect Store Dashboard' ) gcc on kpi.retailer = gcc.SOURCE_RETAILER_Name and kpi.country = gcc.country   """)
cj4 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME
left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and gmc_mapping.GMC_BRAND_name = gmch.b1_brand
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Perfect Store Dashboard' ) gcc on kpi.retailer = gcc.SOURCE_RETAILER_Name and kpi.country = gcc.country  """)
cj4_1 =cu.rowcount

# COMMAND ----------

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME
left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and gmc_mapping.GMC_BRAND_name = gmch.b1_brand
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Perfect Store Dashboard' ) gcc on kpi.retailer = gcc.SOURCE_RETAILER_Name and kpi.country = gcc.country 
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
                          from {database_sfl}.core_raw.L0_global_gcc where region='EMEA') gcch on gcc.gcc_banner_code = gcch.gcc_banner_code and kpi.country = gcch.db_country1 and gcc.GCC_CHANNEL_L2_NAME = gcch.GCC_CHANNEL_L2_NAME;""")
cj5 =cu.rowcount

cu = conn.cursor().execute(f"""select * from (select * from {database_sfl}.CORE_RAW.L0_V_RPT_PSD_KPIS where country in ('United Kingdom','Germany')) kpi
left join PROD_CUSTOMER360_GLOBALNA.GLOBALMASTER_ACCESS.VW_DIM_ACCOUNTABLE_GEOGRAPHY_HIERARCHY agh on kpi.country = agh.ag_long_name
left join (select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_B1 as GMC_BRAND_name,source_SUBCATEGORY_NAME,GMC_Subcategory_Code,GMC_Category_Code,GMC_SUBCATEGORY_NAME from {database_sfl}.CORE_RAW.L0_GMC_MAPPING where  region='EMEA'  and source_system='Perfect Store Dashboard' and SOURCE_CATEGORY_NAME is not NULL and mapping_quality is not NULL) gmc_mapping on kpi.brand = GMC_mapping.source_brand_name and kpi.category = gmc_mapping.source_category_name and   kpi.country = gmc_mapping.country and kpi.subcategory = gmc_mapping.source_SUBCATEGORY_NAME
left join (select distinct  C1_BUSINESS_SEGMENT_CODE,
       C1_BUSINESS_SEGMENT,
        C2_BUSINESS_SUBSEGMENT_CODE, 
        C2_BUSINESS_SUBSEGMENT, 
        C3_NEED_STATE_CODE,
        C3_NEED_STATE,C4_CATEGORY,C4_CATEGORY_CODE,b1_brand,b1_brand_code from {database_sfl}.core_raw.L0_global_gmc ) gmch on gmc_mapping.GMC_Category_Name = gmch.C4_CATEGORY and gmc_mapping.GMC_BRAND_name = gmch.b1_brand
left join (select distinct gcc_banner_code,GCC_CHANNEL_L2_NAME,SOURCE_RETAILER_Name,COUNTRY,source_system,GCC_channel_L2_code from {database_sfl}.CORE_RAW.L0_GCC_MAPPING where region='EMEA' and source_system='Perfect Store Dashboard' ) gcc on kpi.retailer = gcc.SOURCE_RETAILER_Name and kpi.country = gcc.country 
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
                          from {database_sfl}.core_raw.L0_global_gcc where region='EMEA') gcch on gcc.gcc_banner_code = gcch.gcc_banner_code and kpi.country = gcch.db_country1 and gcc.GCC_CHANNEL_L2_NAME = gcch.GCC_CHANNEL_L2_NAME;""")
cj5_1 =cu.rowcount

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,StructField,IntegerType,LongType,DateType
schema = StructType([ \
    StructField("test_time",StringType(),True), \
    StructField("table_name",StringType(),True), \
    StructField("current_count",StringType(),True), \
    StructField("inner_join_count",IntegerType(),True), \
    StructField("left_join_count",StringType(),True),\
    StructField("inner_join_ratio",StringType(),True),\
    StructField("left_join_ratio",StringType(),True)
  ])

emptyRDD = spark.sparkContext.emptyRDD()
df1 = spark.createDataFrame(emptyRDD,schema)

print(df1.isEmpty())
#df = spark.createDataFrame(data=data2,schema=schema)
from datetime import datetime
from pytz import timezone 
ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
print(ind_time)


data2 = [(ind_time,"L0_V_RPT_PSD_KPIS",c1,None,None,None,None),
    (ind_time,"AGH",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
    (ind_time,"GMC_mapping",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
    (ind_time,"GMCH_mapping",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
    (ind_time,"GCC_mapping",cj3_1,cj4,cj4_1,round(cj4/cj3_1,2),round(cj4_1/cj3_1,2)),
    (ind_time,"GCCH_mapping",cj4_1,cj5,cj5_1,round(cj5/cj4_1,2),round(cj5_1/cj4_1,2))]
df14 = spark.createDataFrame(data2,schema)
df14.show(truncate=False)
conn.close()

# COMMAND ----------

#print(user_sfl,' ',password_sfl)

# COMMAND ----------

# import json

# path_to_json = f"/Workspace/Shared/PS_redshift/cred.json"
# with open(path_to_json, "r") as handler:
#     info = json.load(handler)

# COMMAND ----------

# MAGIC %md
# MAGIC Limit to 10 rows

# COMMAND ----------

# #get snowflake details
# role_sfl=info['role_sfl']
# database_sfl= info['database_sfl']
# schema_sfl= info['schema_sfl']
# host_sfl=info['host_sfl']
# warehouse_sfl=info['warehouse_sfl']

# COMMAND ----------

# MAGIC %md
# MAGIC Filter Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profitero

# COMMAND ----------

# MAGIC %md
# MAGIC Write to Snowflake using spark 

# COMMAND ----------

# MAGIC %md
# MAGIC Check the data in snowflake 

# COMMAND ----------

# #all pyspark imports
# from pyspark.sql.functions import col,upper,row_number,when,expr,current_date
# from pyspark.sql.window import Window

# COMMAND ----------

# #reading v_rpt_psd_kpi table
# kpi_table_name = 'L0_V_RPT_PSD_KPIS'
# kpi_df = spark.read.format("snowflake") \
#     .option("host",host_sfl) \
#     .option("user", user_sfl) \
#     .option('role', role_sfl) \
#     .option("password", password_sfl) \
#     .option("database", database_sfl) \
#     .option("sfWarehouse", warehouse_sfl) \
#     .option("schema",schema_sfl)\
#     .option("dbtable",f'{kpi_table_name}')\
#     .load().filter("country in ('United Kingdom','Germany')")
# c1 = kpi_df.count()
# c1

# COMMAND ----------


# agh_df = spark.sql("select * from global_temp.AGH_EMEA")
# gmc_df = spark.sql("select * from global_temp.GMC_EMEA_mapping where source_system='Perfect Store Dashboard'")
# gmch_df = spark.sql("select * from global_temp.gmch_global_emea")
# gcc_df = spark.sql("select * from global_temp.GCC_EMEA_mapping where source_system='Perfect Store Dashboard'")
# gcch_df = spark.sql("select * from global_temp.gcch_global_emea")




# COMMAND ----------



# COMMAND ----------


# df_int1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'inner')
# cj1 = df_int1.count()
# df_int1_1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')
# cj1_1 = df_int1_1.count()
# print(cj1,cj1_1)

# COMMAND ----------

# df_int1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'inner')
# cj2 = df_int1.count()
# df_int1_1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')
# cj2_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = df_int1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')\
#                 .join(gmch_df,((kpi_df['brand']==gmch_df['gmc_brand_name']) &\
#                                (gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY'])),'inner')
# cj3 = df_int1.count()
# df_int1_1 = df_int1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')\
#                 .join(gmch_df,((kpi_df['brand']==gmch_df['gmc_brand_name']) &\
#                                (gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY'])),'left')
# cj3_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')\
#                 .join(gmch_df,((kpi_df['brand']==gmch_df['gmc_brand_name']) &\
#                                (gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY'])),'left')\
#                 .join(gcc_df,((kpi_df['retailer']==trim(gcc_df['SOURCE_RETAILER_Name'])) & (kpi_df['country']==trim(gcc_df['country']))),'inner')
# cj4 = df_int1.count()
# df_int1_1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')\
#                 .join(gmch_df,((kpi_df['brand']==gmch_df['gmc_brand_name']) &\
#                                (gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY'])),'left')\
#                 .join(gcc_df,((kpi_df['retailer']==trim(gcc_df['SOURCE_RETAILER_Name'])) & (kpi_df['country']==trim(gcc_df['country']))),'left')
# cj4_1 = df_int1_1.count()

# COMMAND ----------

# df_int1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')\
#                 .join(gmch_df,((kpi_df['brand']==gmch_df['gmc_brand_name']) &\
#                                (gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY'])),'left')\
#                 .join(gcc_df,((kpi_df['retailer']==trim(gcc_df['SOURCE_RETAILER_Name'])) & (kpi_df['country']==trim(gcc_df['country']))),'left')\
#                 .join(gcch_df,((trim(gcc_df['GCC_CHANNEL_L2_NAME'])==gcch_df['GCC_CHANNEL_L2_NAME']) 
#                                & (kpi_df['country']==gcch_df['db_country1']) 
#                                & (trim(gcc_df['gcc_banner_code'])==gcch_df['gcc_banner_code'])),'inner')
# cj5 = df_int1.count()
# df_int1_1 = kpi_df.join(agh_df,kpi_df['country'] == agh_df['AG_LONG_NAME'],'left')\
#                 .join(gmc_df,((trim(kpi_df['brand'])==trim(gmc_df['source_brand_name'])) & (kpi_df['country']==gmc_df['country']) & (trim(upper(kpi_df['category'])) == trim(upper(gmc_df['source_category_name'])))),'left')\
#                 .join(gmch_df,((kpi_df['brand']==gmch_df['gmc_brand_name']) &\
#                                (gmc_df['GMC_Category_Name']==gmch_df['C4_CATEGORY'])),'left')\
#                 .join(gcc_df,((kpi_df['retailer']==trim(gcc_df['SOURCE_RETAILER_Name'])) & (kpi_df['country']==trim(gcc_df['country']))),'left')\
#                 .join(gcch_df,((trim(gcc_df['GCC_CHANNEL_L2_NAME'])==gcch_df['GCC_CHANNEL_L2_NAME']) 
#                                & (kpi_df['country']==gcch_df['db_country1']) 
#                                & (trim(gcc_df['gcc_banner_code'])==gcch_df['gcc_banner_code'])),'left')
# cj5_1 = df_int1_1.count()

# COMMAND ----------


# data2 = [(ind_time,"L0_V_RPT_PSD_KPIS",c1,None,None,None,None),
#     (ind_time,"AGH",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"GMC_mapping",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"GMCH_mapping",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"GCC_mapping",cj3_1,cj4,cj4_1,round(cj4/cj3_1,2),round(cj4_1/cj3_1,2)),
#     (ind_time,"GCCH_mapping",cj4_1,cj5,cj5_1,round(cj5/cj4_1,2),round(cj5_1/cj4_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------


