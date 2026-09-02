# Databricks notebook source
# MAGIC %md
# MAGIC ## Setting up the Volume

# COMMAND ----------

# DBTITLE 1,Cell 1

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.disease_burden")

base_path = "/Volumes/workspace/default/disease_burden"

indicators = [
    "maternal_mortality",
    "under5_mortality",
    "malaria_mortality",
    "tb_mortality",
    "hiv_mortality",
]

for ind in indicators:
    dbutils.fs.mkdirs(f"{base_path}/raw/who_gho/{ind}")

for layer in ["bronze", "silver", "gold", "exports"]:
    dbutils.fs.mkdirs(f"{base_path}/{layer}")

display(dbutils.fs.ls(base_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the schema

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.disease_burden_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.disease_burden_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.disease_burden_gold;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Injesting the raw data for Hiv_Mortality into a json file

# COMMAND ----------

import requests
import json
from datetime import datetime

BASE_URL = "https://ghoapi.azureedge.net/api"

indicator_codes = {
    "hiv_mortality": "WHS2_138"
}

response = requests.get(f"{BASE_URL}/{indicator_codes['hiv_mortality']}")

response.raise_for_status()

data = response.json()

json_string = json.dumps(data, indent=4)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"/Volumes/workspace/default/disease_burden/raw/who_gho/hiv_mortality/hiv_mortality_{timestamp}.json"

dbutils.fs.put(
    file_path,
    json_string,
    overwrite=True
)

print(f"Successfully saved file to:\n{file_path}")

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/disease_burden/raw/who_gho/hiv_mortality/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading raw Hiv_Mortality data into the bronze layer

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql import functions as F

file_path = "/Volumes/workspace/default/disease_burden/raw/who_gho/hiv_mortality/hiv_mortality_20260730_113740.json"

raw_df = spark.read.option("multiLine", "true").json(file_path)
raw_df.printSchema()

exploded_df = raw_df.select(F.explode("value").alias("record")).select("record.*")

bronze_df = (
    exploded_df
    .withColumn("_injested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(file_path))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.disease_burden_bronze.bronze_who_hiv_mortality"
)

print("Row count written:", bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Injesting the raw data for maternal_Mortality into a json file

# COMMAND ----------

# DBTITLE 1,Cell 7
import requests
import json
from datetime import datetime

BASE_URL = "https://ghoapi.azureedge.net/api"

indicator_codes = {
    "maternal_mortality": "MDG_0000000026"
}

response = requests.get(f"{BASE_URL}/{indicator_codes['maternal_mortality']}")

response.raise_for_status()

data = response.json()

json_string = json.dumps(data, indent=4)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"/Volumes/workspace/default/disease_burden/raw/who_gho/maternal_mortality/maternal_mortality_{timestamp}.json"

dbutils.fs.put(
    file_path,
    json_string,
    overwrite=True
)

print(f"Successfully saved file to:\n{file_path}")

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql import functions as F

files = dbutils.fs.ls("/Volumes/workspace/default/disease_burden/raw/who_gho/maternal_mortality/")
if not files:
    raise FileNotFoundError("No maternal mortality data files found. Please run the data collection cell first.")
file_path = sorted([f.path for f in files if f.name.endswith('.json')])[-1]

raw_df = spark.read.option("multiLine", "true").json(file_path)
raw_df.printSchema()

exploded_df = raw_df.select(F.explode("value").alias("record")).select("record.*")

bronze_df = (
    exploded_df
    .withColumn("_injested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(file_path))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.disease_burden_bronze.bronze_who_maternal_mortality"
)

print("Row count written:", bronze_df.count())

# COMMAND ----------

import requests
import json
from datetime import datetime

BASE_URL = "https://ghoapi.azureedge.net/api"

indicator_codes = {
    "under5_mortality": "MDG_0000000007"
}

response = requests.get(f"{BASE_URL}/{indicator_codes['under5_mortality']}")

response.raise_for_status()

data = response.json()

json_string = json.dumps(data, indent=4)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"/Volumes/workspace/default/disease_burden/raw/who_gho/under5_mortality/under5_mortality_{timestamp}.json"

dbutils.fs.put(
    file_path,
    json_string,
    overwrite=True
)

print(f"Successfully saved file to:\n{file_path}")

# COMMAND ----------

from pyspark.sql import functions as F


files = dbutils.fs.ls("/Volumes/workspace/default/disease_burden/raw/who_gho/under5_mortality/")
if not files:
    raise FileNotFoundError("No under5_mortality data files found. Please run the data collection cell first.")
file_path = sorted([f.path for f in files if f.name.endswith('.json')])[-1]

raw_df = spark.read.option("multiLine", "true").json(file_path)
raw_df.printSchema()

exploded_df = raw_df.select(F.explode("value").alias("record")).select("record.*")

bronze_df = (
    exploded_df
    .withColumn("_injested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(file_path))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.disease_burden_bronze.bronze_who_under5_mortality"
)

print("Row count written:", bronze_df.count())

# COMMAND ----------

import requests
import json
from datetime import datetime

BASE_URL = "https://ghoapi.azureedge.net/api"

indicator_codes = {
    "malaria_mortality": "MALARIA_EST_MORTALITY"
}

response = requests.get(f"{BASE_URL}/{indicator_codes['malaria_mortality']}")

response.raise_for_status()

data = response.json()

json_string = json.dumps(data, indent=4)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"/Volumes/workspace/default/disease_burden/raw/who_gho/malaria_mortality/malaria_mortality_{timestamp}.json"

dbutils.fs.put(
    file_path,
    json_string,
    overwrite=True
)

print(f"Successfully saved file to:\n{file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading the raw Malaria_mortality data into the bronze layer

# COMMAND ----------

from pyspark.sql import functions as F


files = dbutils.fs.ls("/Volumes/workspace/default/disease_burden/raw/who_gho/malaria_mortality/")
if not files:
    raise FileNotFoundError("No malaria_mortality data files found. Please run the data collection cell first.")
file_path = sorted([f.path for f in files if f.name.endswith('.json')])[-1]

raw_df = spark.read.option("multiLine", "true").json(file_path)
raw_df.printSchema()

exploded_df = raw_df.select(F.explode("value").alias("record")).select("record.*")

bronze_df = (
    exploded_df
    .withColumn("_injested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(file_path))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.disease_burden_bronze.bronze_who_malaria_mortality"
)

print("Row count written:", bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Injesting the raw data for TB_Mortality into a json file

# COMMAND ----------

import requests
import json
from datetime import datetime

BASE_URL = "https://ghoapi.azureedge.net/api"

indicator_codes = {
    "tb_mortality": "TB_e_mort_exc_tbhiv_num"
}

response = requests.get(f"{BASE_URL}/{indicator_codes['tb_mortality']}")

response.raise_for_status()

data = response.json()

json_string = json.dumps(data, indent=4)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"/Volumes/workspace/default/disease_burden/raw/who_gho/tb_mortality/tb_mortality_{timestamp}.json"

dbutils.fs.put(
    file_path,
    json_string,
    overwrite=True
)

print(f"Successfully saved file to:\n{file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading raw TB_Mortality data into the bronze layer

# COMMAND ----------

from pyspark.sql import functions as F


files = dbutils.fs.ls("/Volumes/workspace/default/disease_burden/raw/who_gho/tb_mortality/")
if not files:
    raise FileNotFoundError("No tb_mortality data files found. Please run the data collection cell first.")
file_path = sorted([f.path for f in files if f.name.endswith('.json')])[-1]

raw_df = spark.read.option("multiLine", "true").json(file_path)
raw_df.printSchema()

exploded_df = raw_df.select(F.explode("value").alias("record")).select("record.*")

bronze_df = (
    exploded_df
    .withColumn("_injested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(file_path))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.disease_burden_bronze.bronze_who_tb_mortality"
)

print("Row count written:", bronze_df.count())

# COMMAND ----------

# DBTITLE 1,Cell 22
# MAGIC %sql
# MAGIC select *
# MAGIC from workspace.disease_burden_bronze.bronze_who_hiv_mortality
# MAGIC limit 20