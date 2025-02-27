# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
     

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "ebf27a5f-29d7-4f68-976e-3c8fb14da51a",
"fs.azure.account.oauth2.client.secret": 'nzI8Q~hm7lFEs9oZc1DjsrhF6kG6P4ClPiD1dbmC',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/eadd1f8d-a7fd-445f-97a7-af05fa82863a/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympics16.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"
# MAGIC      

# COMMAND ----------

spark

# COMMAND ----------

Athlete = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/08124ff3-6bd5-4317-b22a-32c3127589bd")
Coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/619a406c-c3f4-4b30-a18d-f7334f02302a")
EntriesGen = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/entriesgen")
Medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/medals")
Teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw_data/teams")


# COMMAND ----------

Athlete.show()

# COMMAND ----------

Coaches.show()

# COMMAND ----------

EntriesGen.show()   

# COMMAND ----------

Medals.show()

# COMMAND ----------

Teams.show()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/tokyoolymic/raw_data/"))


# COMMAND ----------

Athlete.printSchema()   

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

EntriesGen.printSchema()    

# COMMAND ----------

Medals.printSchema()

# COMMAND ----------

Teams.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = Medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold").show()

# COMMAND ----------

# Read the text file assuming it's tab-separated
df = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load("/mnt/tokyoolymic/raw_data/08124ff3-6bd5-4317-b22a-32c3127589bd")

# Show the first few rows to check if the data is loaded correctly
df.show()


# COMMAND ----------

df.show()

# COMMAND ----------

Athlete.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/08124ff3-6bd5-4317-b22a-32c3127589bd")
Coaches.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/619a406c-c3f4-4b30-a18d-f7334f02302a")
EntriesGen.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/entriesgen")
Medals.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/medals")
Teams.write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/teams")     