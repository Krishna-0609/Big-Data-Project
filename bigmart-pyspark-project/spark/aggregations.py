from pyspark.sql.window import Window
from pyspark.sql.functions import rank
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



windowSpec=Window.partitionBy("Outlet_Type")\
.orderBy(col("Item_Outlet_Sales").desc())

ranked_df=clean_df.withColumn("sales_rank",rank().over(windowSpec))


sales_by_store=clean_df.groupby("Outlet_Type").agg({"Item_Outlet_Sales":"sum"})
print("----------------------------------------------------------------------------")
ranked_df.show()
print("----------------------------------------------------------------------------")
sales_by_store.show()