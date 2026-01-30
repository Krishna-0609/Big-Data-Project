from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("BigMart Ingestion").enableHiveSupport().getOrCreate()

df=spark.read.csv("hdfs:///bigmart/raw/bigmart.csv",header=True,inferSchema=True)
print("Original Records:👍", df.count())
df.show()
large_df=df
for _ in range(7):
    large_df=large_df.union(df)

print("Total Records after scaling:👍",large_df.count())
large_df.show()

large_df.write.mode("overwrite").parquet("hdfs:///bigmart/staged/bigmart_large")
