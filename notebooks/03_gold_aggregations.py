# Databricks notebook source
from pyspark.sql import Row

AFRICAN_COUNTRY_NAMES = {
    "DZA": "Algeria", "AGO": "Angola", "BEN": "Benin", "BWA": "Botswana",
    "BFA": "Burkina Faso", "BDI": "Burundi", "CPV": "Cabo Verde", "CMR": "Cameroon",
    "CAF": "Central African Republic", "TCD": "Chad", "COM": "Comoros",
    "COG": "Congo", "COD": "DR Congo", "CIV": "Cote d'Ivoire", "DJI": "Djibouti",
    "EGY": "Egypt", "GNQ": "Equatorial Guinea", "ERI": "Eritrea",
    "SWZ": "Eswatini", "ETH": "Ethiopia", "GAB": "Gabon", "GMB": "Gambia",
    "GHA": "Ghana", "GIN": "Guinea", "GNB": "Guinea-Bissau", "KEN": "Kenya",
    "LSO": "Lesotho", "LBR": "Liberia", "LBY": "Libya", "MDG": "Madagascar",
    "MWI": "Malawi", "MLI": "Mali", "MRT": "Mauritania", "MUS": "Mauritius",
    "MAR": "Morocco", "MOZ": "Mozambique", "NAM": "Namibia", "NER": "Niger",
    "NGA": "Nigeria", "RWA": "Rwanda", "STP": "Sao Tome and Principe",
    "SEN": "Senegal", "SYC": "Seychelles", "SLE": "Sierra Leone", "SOM": "Somalia",
    "ZAF": "South Africa", "SSD": "South Sudan", "SDN": "Sudan",
    "TZA": "Tanzania", "TGO": "Togo", "TUN": "Tunisia", "UGA": "Uganda",
    "ZMB": "Zambia", "ZWE": "Zimbabwe"
}

gold_country_dim = spark.createDataFrame(
    [Row(country_code = code, country_name = name)
     for code, name in AFRICAN_COUNTRY_NAMES.items()]
)

gold_country_dim.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('workspace.disease_burden_gold.gold_country_dim')

# COMMAND ----------

gold_country_dim.show()

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.disease_burden_gold")

# COMMAND ----------

spark.table('workspace.disease_burden_gold.gold_country_dim').count()

# COMMAND ----------

silver_df = 'workspace.disease_burden_silver.silver_disease_burden'

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import round as spark_round, col

silver_df = spark.table('workspace.disease_burden_silver.silver_disease_burden')

gold_disease_burden_by_country = silver_df.withColumn(
    'value', spark_round(col('value'), 2)
)

gold_disease_burden_by_country.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('country_code', 'year') \
    .saveAsTable('workspace.disease_burden_gold.gold_disease_burden_by_country')

# COMMAND ----------

spark.table('workspace.disease_burden_gold.gold_disease_burden_by_country').show(5)

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql import Window
from pyspark.sql.functions import lag, round as spark_round, try_divide, col

window_spec = Window.partitionBy("country_code", "indicator").orderBy("year")

gold_yoy_trends = gold_disease_burden_by_country.withColumn(
    "previous_year_value", lag("value", 1).over(window_spec)
).withColumn(
    "yoy_change", spark_round(col("value") - col("previous_year_value"), 2)
).withColumn(
    "yoy_pct_change", spark_round(
        try_divide((col("value") - col("previous_year_value")) * 100, col("previous_year_value")), 2
    )
)

gold_yoy_trends.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("country_code", "year") \
    .saveAsTable("workspace.disease_burden_gold.gold_yoy_trends")

# COMMAND ----------

gold_yoy_trends.show()

# COMMAND ----------

tables_to_export = {
    "gold_disease_burden_by_country": "workspace.disease_burden_gold.gold_disease_burden_by_country",
    "gold_yoy_trends": "workspace.disease_burden_gold.gold_yoy_trends",
    "gold_country_dim": "workspace.disease_burden_gold.gold_country_dim",
}

export_base_path = "/Volumes/workspace/default/disease_burden/exports"

for export_name, table_name in tables_to_export.items():
    df = spark.table(table_name)
    df.coalesce(1).write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(f"{export_base_path}/{export_name}")

# COMMAND ----------

dbutils.fs.ls(f"{export_base_path}/gold_disease_burden_by_country")

# COMMAND ----------

display(gold_disease_burden_by_country)

# COMMAND ----------

display(gold_yoy_trends)

# COMMAND ----------

display(gold_country_dim)