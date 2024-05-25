# Databricks notebook source
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import sum, when, col, expr, current_timestamp, lit
 
credential = DefaultAzureCredential()

env = dbutils.widgets.get('env')
#schema_sfl = "CORE_RAW"
key_vault_url = f'https://kv-sobg-{env}-001.vault.azure.net/'
#database_sfl = f"{env}_COMX_SOBG"
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)


snowflake_secret = secret_client.get_secret("comx-sobg-snowlfake-sa")
user_sfl = snowflake_secret.properties.tags['username']
password_sfl = snowflake_secret.value
role_sfl = snowflake_secret.properties.tags['role']
warehouse_sfl = snowflake_secret.properties.tags['warehouse']
host_sfl = snowflake_secret.properties.tags['host']
password_sfl = snowflake_secret.value
database_sfl = f"{env}_COMX_SOBG"
schema_sfl = "CORE_RAW"
#profitero_table = dbutils.widgets.get("table_name")--> get the above two from parameters

# COMMAND ----------


#snowflake query

#tbl = "DEV_COMX_SOBG.CORE_RAW.L0_PROFITERO_FACT_PLACEMENT"
#pfp_query = f"""select * from {tbl} where db_country in ('United Kingdom','Germany')  and is_deleted = 'false';"""
pfp_query = f"select * from (select *,row_number() over(partition by DATE,ranking_id ,ranking_product_id order by page_placement,actual_rank,db_country_code) as rn from {env}_COMX_SOBG.CORE_RAW.L0_PROFITERO_FACT_PLACEMENT where DB_COUNTRY in ('United Kingdom','Germany') and is_deleted = 'false' ) a where rn=1"
pfp_df = spark.read.format("snowflake") \
    .option("host",host_sfl) \
    .option("user", user_sfl) \
    .option('role', role_sfl) \
    .option("password", password_sfl) \
    .option("database", database_sfl) \
    .option("sfWarehouse", warehouse_sfl) \
    .option("query", pfp_query)\
    .option("partitionColumn", "ranking_product_id")\
    .option("lowerBound", "2372169")\
    .option("upperBound", "1549534555")\
    .option("numPartitions", "200")\
    .load()
# pfp_df.count()


# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "209715200b")

# COMMAND ----------

pdcp_df = spark.sql("select * from global_temp.DIM_CUSTOMER_PRODUCTS_EMEA")
pdb_df = spark.sql("select * from global_temp.DIM_brands_EMEA")
pdr_df = spark.sql("select * from global_temp.DIM_retailers_EMEA")
pdcp1_df = spark.sql("select * from global_temp.DIM_CATEGORY_PRODUCT_EMEA")
pdc1_df = spark.sql("select * from global_temp.DIM_CATEGORIES_EMEA")
pdp_df = spark.sql("select * from global_temp.PROFITERO_DIM_PRODUCTS_EMEA")
agh_df = spark.sql("select * from global_temp.AGH_EMEA")
gmc_mapping_df = spark.sql("select distinct source_brand_name,source_category_name,country,source_system,GMC_BRAND_CODE,GMC_Category_Name, GMC_BRAND_name from global_temp.GMC_EMEA_mapping where source_system='Profitero' and SOURCE_CATEGORY_NAME is not NULL and SOURCE_SUBCATEGORY_NAME is NULL")
pdrp_df = spark.sql("select * from global_temp.PROFITERO_DIM_RANKING_PRODUCTS_EMEA")
gmch_global_df = spark.sql("select * from global_temp.gmch_global_emea")
gcc_mapping_df = spark.sql("select * from global_temp.GCC_EMEA_mapping where source_system='Profitero'")
gcch_global_df = spark.sql("select * from global_temp.gcch_global_emea")
pdrk_df = spark.sql("select * from global_temp.DIM_RANKINGS_EMEA")
msl_df = spark.sql("select * from global_temp.msl_flag_emea")
clt_df = spark.sql("select * from global_temp.categories_lookup_table")
gmc_mapping_df1 = spark.sql("select distinct GMC_BRAND_name as GMC_BRAND_name1,source_brand_name as source_brand_name1,gmc_brand_code as gmc_brand_code1,country  from global_temp.GMC_EMEA_mapping where source_system='Profitero'  and  (SOURCE_CATEGORY_NAME is  NULL)   ")
gmc_mapping_df2 = spark.sql("""select distinct source_brand_name as source_brand_name2,source_category_name as source_category_name2,country,source_system,GMC_BRAND_CODE as GMC_BRAND_CODE2,GMC_Category_Name as GMC_Category_Name2, GMC_BRAND_name as GMC_BRAND_name2, SOURCE_SUBCATEGORY_NAME as SOURCE_SUBCATEGORY_NAME2  from global_temp.GMC_EMEA_mapping where source_system='Profitero'
and SOURCE_CATEGORY_NAME is not NULL and source_SUBCATEGORY_NAME is not NULL""")

# COMMAND ----------


#snowflake query

tbl = "DEV_COMX_SOBG.CORE_RAW.L0_Customer_Product_lookup_table"

cpl_df = spark.read.format("snowflake") \
    .option("host",host_sfl) \
    .option("user", user_sfl) \
    .option('role', role_sfl) \
    .option("password", password_sfl) \
    .option("database", database_sfl) \
    .option("sfWarehouse", warehouse_sfl) \
    .option("dbtable", tbl)\
    .load()
cpl_df.createOrReplaceGlobalTempView("Customer_Product_lookup_table_EMEA")


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

# data2 = [("TSKU","","Smith","36636","M",3000),
#     ("Michael","Rose","","40288","M",4000),
#     ("Robert","","Williams","42114","M",4000),
#     ("Maria","Anne","Jones","39192","F",4000),
#     ("Jen","Mary","Brown","","F",-1)
#   ]
emptyRDD = spark.sparkContext.emptyRDD()
df13 = spark.createDataFrame(emptyRDD,schema)

#print(df13.isEmpty())
#df = spark.createDataFrame(data=data2,schema=schema)



# COMMAND ----------

from datetime import datetime
from pytz import timezone 
ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
print(ind_time)

# COMMAND ----------

c1 = pfp_df.count()
c1


# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'inner')

cj1 = df_int1.count()

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')
cj1_1 = df_int1_1.count()

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1)]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                 .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'inner')
cj2 = df_int1.count()                 


df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                 .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')
cj2_1 = df_int1_1.count()                 


# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(c1/cj1,2),round(c1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj1_1/cj2,2),round(cj1_1/cj2_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                 .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                    .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'inner')
cj3 = df_int1.count()                 

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                 .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                    .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')
cj3_1 = df_int1_1.count()                 
  

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(c1/cj1,2),round(c1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj1_1/cj2,2),round(cj1_1/cj2_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj2_1/cj3,2),round(cj2_1/cj3_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')
cj4 = df_int1.count()                 

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'left')
cj4_1 = df_int1_1.count()                 
 
# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(c1/cj1,2),round(c1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj1_1/cj2,2),round(cj1_1/cj2_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj2_1/cj3,2),round(cj2_1/cj3_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj3_1/cj4,2),None)]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False) 

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'inner')
cj5 = df_int1.count()  

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')
cj5_1 = df_int1_1.count()                 
# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)  

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'inner')
cj6 = df_int1.count()   

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')
cj6_1 = df_int1_1.count()                 

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)  

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'inner')
cj7 = df_int1.count()                 

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')
cj7_1 = df_int1_1.count()                 
   
# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)  

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
                .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'inner')
cj8 = df_int1.count()   

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
                .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')
cj8_1 = df_int1_1.count()                 
 
# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
#     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)  

# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
                .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')\
                .join(pdr_df,((pdrp_df['retailer_id']==pdr_df['id']) & (pdc1_df['db_country']==pdr_df['db_country'])),'inner')
cj9 = df_int1.count()   

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
                .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')\
                .join(pdr_df,((pdrp_df['retailer_id']==pdr_df['id']) & (pdc1_df['db_country']==pdr_df['db_country'])),'left')
cj9_1 = df_int1_1.count()   

data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
    (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
    (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
    (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
    (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
    (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
    (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
    (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
    (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
    (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2))]
df14 = spark.createDataFrame(data2,schema)
df14.show(truncate=False)   


# COMMAND ----------

df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
                .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')\
                .join(pdr_df,((pdrp_df['retailer_id']==pdr_df['id']) & (pdc1_df['db_country']==pdr_df['db_country'])),'left')\
                .join(msl_df,((pdcp_df['id']==msl_df['customer_product_id']) & (pdcp_df['db_country_code']==msl_df['db_country_code'])),'left')
cj10 = df_int1.count()    

df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
                .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
                .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
                .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
                .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
                .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
                .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
                .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')\
                .join(pdr_df,((pdrp_df['retailer_id']==pdr_df['id']) & (pdc1_df['db_country']==pdr_df['db_country'])),'left')\
                .join(msl_df,((pdcp_df['id']==msl_df['customer_product_id']) & (pdcp_df['db_country_code']==msl_df['db_country_code'])),'left')
cj10_1 = df_int1_1.count()    

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
#     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
#     (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
#     (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False) 

# COMMAND ----------

# df_int1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
#                 .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
#                 .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
#                 .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
#                 .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
#                 .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
#                 .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
#                 .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')\
#                 .join(pdr_df,((pdrp_df['retailer_id']==pdr_df['id']) & (pdc1_df['db_country']==pdr_df['db_country'])),'left')\
#                 .join(msl_df,((pdcp_df['id']==msl_df['customer_product_id']) & (pdcp_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 #.join(clt_df,((pdcp_df['id']==clt_df['id']) ),'left')
# cj11 = df_int1.count()                 

# df_int1_1 = pfp_df.join(pdrk_df,pfp_df['ranking_id'] == pdrk_df['id'],'left')\
#                 .join(pdrp_df,pfp_df['ranking_product_id']==pdrp_df['id'],'left')\
#                 .join(pdp_df,pfp_df['ranking_product_id']==pdp_df['ranking_product_id'],'left')\
#                 .join(cpl_df,pdp_df['id']==cpl_df['product_id'],'inner')\
#                 .join(pdcp_df,pdcp_df['id']==cpl_df['customer_product_id'],'left')\
#                 .join(pdcp1_df,cpl_df['customer_product_id']==pdcp1_df['customer_product_id'],'left')\
#                 .join(pdc1_df,((pdcp1_df['category_id']==pdc1_df['id']) & (pdc1_df['db_country']==pdcp1_df['db_country'])) ,'left')\
#                 .join(pdb_df,((pdcp_df['brand_id']==pdb_df['id']) & (pdc1_df['db_country']==pdb_df['db_country'])),'left')\
#                 .join(pdr_df,((pdrp_df['retailer_id']==pdr_df['id']) & (pdc1_df['db_country']==pdr_df['db_country'])),'left')\
#                 .join(msl_df,((pdcp_df['id']==msl_df['customer_product_id']) & (pdcp_df['db_country_code']==msl_df['db_country_code'])),'left')\
#                 #.join(clt_df,((pdcp_df['id']==clt_df['id']) ),'left')
# cj11_1 = df_int1_1.count()                 


# # data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
# #     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
# #     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
# #     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
# #     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
# #     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
# #     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
# #     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
# #     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
# #     (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
# #     (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
# #     (ind_time,"CLT",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2))]
# # df14 = spark.createDataFrame(data2,schema)
# # df14.show(truncate=False) 

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, expr, current_timestamp, lit, trim
df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'inner')
cj11 = df_int2.count()                 

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')
cj11_1 = df_int2_1.count()                 

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
#     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
#     (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
#     (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
#     (ind_time,"CLT",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
#     (ind_time,"AGH",cj11_1,cj12,cj12_1,round(cj12/cj11_1,2),round(cj12_1/cj11_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

# .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
#                   .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\

# COMMAND ----------

df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df['country'])) & (pdc1_df['source_category'] == gmc_mapping_df['source_category_name'])),'inner')
cj12 = df_int2.count()   

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df['country'])) & (pdc1_df['source_category'] == gmc_mapping_df['source_category_name'])),'left')
cj12_1 = df_int2_1.count()   

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
#     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
#     (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
#     (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
#     (ind_time,"CLT",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
#     (ind_time,"AGH",cj11_1,cj12,cj12_1,round(cj12/cj11_1,2),round(cj12_1/cj11_1,2)),
#     (ind_time,"GCC_mapping",cj12_1,cj13,cj13_1,round(cj13/cj12_1,2),round(cj13_1/cj12_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'inner')
cj13 = df_int2.count()    

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                 .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')
cj13_1 = df_int2_1.count()    

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
#     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
#     (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
#     (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
#     (ind_time,"CLT",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
#     (ind_time,"AGH",cj11_1,cj12,cj12_1,round(cj12/cj11_1,2),round(cj12_1/cj11_1,2)),
#     (ind_time,"GMC_mapping",cj12_1,cj13,cj13_1,round(cj13/cj12_1,2),round(cj13_1/cj12_1,2)),
#     (ind_time,"GMCH_mapping",cj13_1,cj14,cj14_1,round(cj14/cj13_1,2),round(cj14_1/cj13_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                 .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'inner')
cj14 = df_int2.count()  

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                 .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')
cj14_1 = df_int2_1.count()  

# data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
#     (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
#     (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
#     (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
#     (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
#     (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
#     (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
#     (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
#     (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
#     (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
#     (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
#     (ind_time,"CLT",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
#     (ind_time,"AGH",cj11_1,cj12,cj12_1,round(cj12/cj11_1,2),round(cj12_1/cj11_1,2)),
#     (ind_time,"GMC_mapping",cj12_1,cj13,cj13_1,round(cj13/cj12_1,2),round(cj13_1/cj12_1,2)),
#     (ind_time,"GMCH_mapping",cj13_1,cj14,cj14_1,round(cj14/cj13_1,2),round(cj14_1/cj13_1,2)),
#     (ind_time,"GCC_mapping",cj14_1,cj15,cj15_1,round(cj15/cj14_1,2),round(cj15_1/cj14_1,2))]
# df14 = spark.createDataFrame(data2,schema)
# df14.show(truncate=False)

# COMMAND ----------

df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')\
                 .join(gcch_global_df,((trim(gcc_mapping_df['gcc_banner_code']) == trim(gcch_global_df['gcc_banner_code'])) & (trim(pdc1_df['db_country'])==trim(gcch_global_df['db_country1'])) & (trim(gcc_mapping_df['GCC_CHANNEL_L2_NAME'])==trim(gcch_global_df['GCC_CHANNEL_L2_NAME']))),'inner')
cj15 = df_int2.count() 

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                 .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')\
                 .join(gcch_global_df,((trim(gcc_mapping_df['gcc_banner_code']) == trim(gcch_global_df['gcc_banner_code'])) & (trim(pdc1_df['db_country'])==trim(gcch_global_df['db_country1'])) & (trim(gcc_mapping_df['GCC_CHANNEL_L2_NAME'])==trim(gcch_global_df['GCC_CHANNEL_L2_NAME']))),'left')
cj15_1 = df_int2_1.count()                 



# COMMAND ----------

df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')\
                 .join(gcch_global_df,((trim(gcc_mapping_df['gcc_banner_code']) == trim(gcch_global_df['gcc_banner_code'])) & (trim(pdc1_df['db_country'])==trim(gcch_global_df['db_country1'])) & (trim(gcc_mapping_df['GCC_CHANNEL_L2_NAME'])==trim(gcch_global_df['GCC_CHANNEL_L2_NAME']))),'left')\
                .join(gmc_mapping_df1,((trim(pdb_df['brand']) == trim(gmc_mapping_df1['source_brand_name1'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df2['country']))),'inner')
cj16 = df_int2.count() 

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                 .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')\
                 .join(gcch_global_df,((trim(gcc_mapping_df['gcc_banner_code']) == trim(gcch_global_df['gcc_banner_code'])) & (trim(pdc1_df['db_country'])==trim(gcch_global_df['db_country1'])) & (trim(gcc_mapping_df['GCC_CHANNEL_L2_NAME'])==trim(gcch_global_df['GCC_CHANNEL_L2_NAME']))),'left')\
                .join(gmc_mapping_df1,((trim(pdb_df['brand']) == trim(gmc_mapping_df1['source_brand_name1'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df2['country']))),'left')
cj16_1 = df_int2_1.count()                 



# COMMAND ----------

df_int2 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')\
                 .join(gcch_global_df,((trim(gcc_mapping_df['gcc_banner_code']) == trim(gcch_global_df['gcc_banner_code'])) & (trim(pdc1_df['db_country'])==trim(gcch_global_df['db_country1'])) & (trim(gcc_mapping_df['GCC_CHANNEL_L2_NAME'])==trim(gcch_global_df['GCC_CHANNEL_L2_NAME']))),'left')\
                .join(gmc_mapping_df1,(trim(pdb_df['brand']) == trim(gmc_mapping_df1['source_brand_name1'])),'left')\
                .join(gmc_mapping_df2,((trim(pdb_df['brand']) == trim(gmc_mapping_df2['source_brand_name2'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df2['country'])) & (pdc1_df['source_category'] == gmc_mapping_df2['source_category_name2']) &  (pdc1_df['source_sub_category'] == gmc_mapping_df2['SOURCE_SUBCATEGORY_NAME2'])),'inner')
cj17 = df_int2.count() 

df_int2_1 = df_int1.join(agh_df,pfp_df['DB_COUNTRY'] == agh_df['ag_long_name'],'left')\
                 .join(gmc_mapping_df,((trim(pdb_df['brand']) == trim(gmc_mapping_df['source_brand_name'])) & (trim(pdc1_df['db_country'])==trim(gmc_mapping_df['country'])) & (df_int1['source_category'] == gmc_mapping_df['source_category_name'])),'left')\
                 .join(gmch_global_df,((trim(gmc_mapping_df['GMC_Category_Name']) == trim(gmch_global_df['C4_CATEGORY'])) & (df_int1['brand'] == gmch_global_df['GMC_BRAND_NAME'] ) ),'left')\
                .join(gcc_mapping_df,((trim(pdr_df['name']) == trim(gcc_mapping_df['SOURCE_RETAILER_Name'])) & (trim(pdc1_df['db_country'])==trim(gcc_mapping_df['country']))),'left')\
                 .join(gcch_global_df,((trim(gcc_mapping_df['gcc_banner_code']) == trim(gcch_global_df['gcc_banner_code'])) & (trim(pdc1_df['db_country'])==trim(gcch_global_df['db_country1'])) & (trim(gcc_mapping_df['GCC_CHANNEL_L2_NAME'])==trim(gcch_global_df['GCC_CHANNEL_L2_NAME']))),'left')\
                .join(gmc_mapping_df1,(trim(pdb_df['brand']) == trim(gmc_mapping_df1['source_brand_name1'])),'left')\
                .join(gmc_mapping_df2,((trim(pdb_df['brand']) == trim(gmc_mapping_df2['source_brand_name2'])) & (trim(pfp_df['db_country'])==trim(gmc_mapping_df2['country'])) & (pdc1_df['source_category'] == gmc_mapping_df2['source_category_name2']) &  (pdc1_df['source_sub_category'] == gmc_mapping_df2['SOURCE_SUBCATEGORY_NAME2'])),'left')
cj17_1 = df_int2_1.count()                 



# COMMAND ----------

data2 = [(ind_time,"L0_PROFITERO_FACT_PLACEMENT",c1,None,None,None,None),
    (ind_time,"DIM_RANKINGS_EMEA",c1,cj1,cj1_1,round(cj1/c1,2),round(cj1_1/c1,2)),
    (ind_time,"PROFITERO_DIM_RANKING_PRODUCTS_EMEA",cj1_1,cj2,cj2_1,round(cj2/cj1_1,2),round(cj2_1/cj1_1,2)),
    (ind_time,"PROFITERO_DIM_PRODUCTS_EMEA",cj2_1,cj3,cj3_1,round(cj3/cj2_1,2),round(cj3_1/cj2_1,2)),
    (ind_time,"Customer_Product_lookup_table_EMEA",cj3_1,cj4,None,round(cj4/cj3_1,2),None),
    (ind_time,"DIM_CUSTOMER_PRODUCTS_EMEA",cj4,cj5,cj5_1,round(cj5/cj4,2),round(cj5_1/cj4,2)),
    (ind_time,"DIM_CATEGORY_PRODUCT_EMEA",cj5_1,cj6,cj6_1,round(cj6/cj5_1,2),round(cj6_1/cj5_1,2)),
    (ind_time,"DIM_CATEGORIES_EMEA",cj6_1,cj7,cj7_1,round(cj7/cj6_1,2),round(cj7_1/cj6_1,2)),
    (ind_time,"DIM_brands_EMEA",cj7_1,cj8,cj8_1,round(cj8/cj7_1,2),round(cj8_1/cj7_1,2)),
    (ind_time,"DIM_retailers_EMEA",cj8_1,cj9,cj9_1,round(cj9/cj8_1,2),round(cj9_1/cj8_1,2)),
    (ind_time,"MSL",cj9_1,cj10,cj10_1,round(cj10/cj9_1,2),round(cj10_1/cj9_1,2)),
    (ind_time,"AGH",cj10_1,cj11,cj11_1,round(cj11/cj10_1,2),round(cj11_1/cj10_1,2)),
    (ind_time,"GMC_mapping",cj11_1,cj12,cj12_1,round(cj12/cj11_1,2),round(cj12_1/cj11_1,2)),
    (ind_time,"GMCH_mapping",cj12_1,cj13,cj13_1,round(cj13/cj12_1,2),round(cj13_1/cj12_1,2)),
    (ind_time,"GCC_mapping",cj13_1,cj14,cj14_1,round(cj14/cj13_1,2),round(cj14_1/cj13_1,2)),
    (ind_time,"GCCH_mapping",cj14_1,cj15,cj15_1,round(cj15/cj14_1,2),round(cj15_1/cj14_1,2)),
    (ind_time,"GMC_mapping1",cj15_1,cj16,cj16_1,round(cj16/cj15_1,2),round(cj16_1/cj15_1,2)),
    (ind_time,"GMC_mapping2",cj16_1,cj17,cj17_1,round(cj17/cj16_1,2),round(cj17_1/cj16_1,2))]
df14 = spark.createDataFrame(data2,schema)
df14.show(truncate=False)

# COMMAND ----------


