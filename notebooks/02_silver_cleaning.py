# Databricks notebook source
df1 = spark.read.table("workspace.disease_burden_bronze.bronze_who_hiv_mortality")
df2 = spark.read.table("workspace.disease_burden_bronze.bronze_who_malaria_mortality")
df3 = spark.read.table("workspace.disease_burden_bronze.bronze_who_maternal_mortality")
df4 = spark.read.table("workspace.disease_burden_bronze.bronze_who_tb_mortality")
df5 = spark.read.table("workspace.disease_burden_bronze.bronze_who_under5_mortality")

# COMMAND ----------

INDICATOR_MAP = {
    "hiv_mortality": "workspace.disease_burden_bronze.bronze_who_hiv_mortality",
    "malaria_mortality": "workspace.disease_burden_bronze.bronze_who_malaria_mortality",
    "maternal_mortality": "workspace.disease_burden_bronze.bronze_who_maternal_mortality",
    "tb_mortality": "workspace.disease_burden_bronze.bronze_who_tb_mortality",
    "under5_mortality": "workspace.disease_burden_bronze.bronze_who_under5_mortality",
}

# COMMAND ----------

from pyspark.sql.functions import col, lit

silver_frames = []

for indicator_name, table_name in INDICATOR_MAP.items():
    df = spark.read.table(table_name)

    df = df.filter(col("SpatialDimType") == "COUNTRY")

    if indicator_name == "under5_mortality":
        df = df.where(col("Dim1") == "SEX_BTSX")
        df = df.where(col('Dim3') == 'WEALTHQUINTILE_TOTL')

    df = df.filter(col("NumericValue").isNotNull())

    df = df.select(
        col("SpatialDim").alias("country_code"),
        col("TimeDim").alias("year"),
        lit(indicator_name).alias("indicator"),
        col("NumericValue").alias("value")
    )

    silver_frames.append(df)

# COMMAND ----------

raw = spark.read.table('workspace.disease_burden_bronze.bronze_who_hiv_mortality')
print("Total rows in bronze:" , raw.count())

country_only = raw.where('SpatialDimType = "COUNTRY"')
print('after country:' , country_only.count())

not_censored = country_only.where(col('NumericValue').isNotNull())
print('after dropping censored rows:', not_censored.count())

africa_only = not_censored.where(col('SpatialDim').isin(AFRICAN_ISO3_CODES))
print('after africa filter:',  africa_only.count())

# COMMAND ----------

from functools import reduce
silver_df = reduce(lambda df1, df2: df1.unionByName(df2), silver_frames)

# COMMAND ----------

silver_df.show(10)
silver_df.count()

# COMMAND ----------

AFRICAN_ISO3_CODES = [
    "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
    "COM", "COG", "COD", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
    "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
    "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
    "STP", "SEN", "SYC", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO",
    "TUN", "UGA", "ZMB", "ZWE"
]

# COMMAND ----------

before_count = silver_df.count()
silver_df = silver_df.filter(col("country_code").isin(AFRICAN_ISO3_CODES))
after_count = silver_df.count()
print(before_count, after_count)

# COMMAND ----------

duplicate_check = (
    silver_df
    .groupBy("country_code", "year", "indicator")
    .count()
    .filter(col("count") > 1)
)

duplicate_check.show()
print(duplicate_check.count())

# COMMAND ----------

from pyspark.sql.functions import col

spark.read.table("workspace.disease_burden_bronze.bronze_who_hiv_mortality") \
    .filter((col("SpatialDim") == "CIV") & (col("TimeDim") == 2001)) \
    .show(truncate=False)


# COMMAND ----------

spark.read.table('workspace.disease_burden_bronze.bronze_who_hiv_mortality') \
    .filter(col('IndicatorCode') != 'WHS2_138') \
    .groupBy('IndicatorCode') \
    .count() \
    .show()


# COMMAND ----------

total_rows = spark.read.table('workspace.disease_burden_bronze.bronze_who_hiv_mortality').count()
print(total_rows)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

silver_df.groupBy('indicator').count().show()

# COMMAND ----------

spark.read.table("workspace.disease_burden_bronze.bronze_who_under5_mortality") \
    .filter(col("SpatialDimType") == "COUNTRY") \
    .filter(col("Dim1") == "BTSX") \
    .count()

# COMMAND ----------

from pyspark.sql.functions import col
spark.read.table("workspace.disease_burden_bronze.bronze_who_under5_mortality") \
    .where(col('SpatialDimType')== 'COUNTRY') \
    .count()


# COMMAND ----------

spark.read.table('workspace.disease_burden_bronze.bronze_who_under5_mortality') \
    .select(col('Dim1')) \
    .distinct() \
    .show()

# COMMAND ----------

silver_df.count()
silver_df.groupBy('indicator').count().show()

# COMMAND ----------

silver_df = silver_df.where(col("country_code").isin(AFRICAN_ISO3_CODES))

# COMMAND ----------

silver_df.count()
silver_df.groupBy('indicator').count().show()

# COMMAND ----------

silver_df.where(col("indicator") == "under5_mortality").select("year").distinct().count()

# COMMAND ----------

spark.read.table('workspace.disease_burden_bronze.bronze_who_under5_mortality') \
    .select('Dim2', 'Dim3') \
    .distinct() \
    .show()

# COMMAND ----------

silver_df.count()
silver_df.groupBy('indicator').count().show()

# COMMAND ----------

silver_df.count()

# COMMAND ----------

silver_df.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('country_code', 'year') \
    .saveAsTable('workspace.disease_burden_silver.silver_disease_burden')

# COMMAND ----------

silver_df_v2 = silver_df.where(col('year') >= 2010)

silver_df_v2.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('country_code', 'year') \
    .saveAsTable('workspace.disease_burden_silver.silver_disease_burden')

# COMMAND ----------

silver_df.show()

# COMMAND ----------



# COMMAND ----------

spark.sql('DESCRIBE HISTORY workspace.disease_burden_silver.silver_disease_burden').show(truncate=False)

# COMMAND ----------

old_version_df = spark.read \
    .format('delta') \
    .option('versionAsOf', 0) \
    .table('workspace.disease_burden_silver.silver_disease_burden')

old_version_df.count()

# COMMAND ----------

spark.sql('''
          RESTORE TABLE workspace.disease_burden_silver.silver_disease_burden
          TO VERSION AS OF 0''')

# COMMAND ----------

spark.table('workspace.disease_burden_silver.silver_disease_burden').count()

# COMMAND ----------

