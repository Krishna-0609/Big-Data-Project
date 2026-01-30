from pyspark.sql.functions import avg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("BigMart Data Cleaning") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.csv(
    "hdfs:///bigmart/raw/bigmart.csv",
    header=True,
    inferSchema=True
)

clean_df = df.fillna({"Item_Weight": 0}) \
    .withColumn(
        "Item_Visibility",
        when(col("Item_Visibility") == 0, None)
        .otherwise(col("Item_Visibility"))
    )


transformed_df = clean_df.agg(avg("Item_MRP").alias("Avg_Item_MRP"))
transformed_df.show()
