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

df.show()

# Cleaning
clean_df = df.fillna({"Item_Weight": 0}) \
    .withColumn(
        "Item_Visibility",
        when(col("Item_Visibility") == 0, None)
        .otherwise(col("Item_Visibility"))
    )


print("✅ Total records after cleaning:", clean_df.count())

clean_df.show()

spark.stop()
