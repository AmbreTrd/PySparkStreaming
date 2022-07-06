from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""
Spark Session :
"""
sparkSession = SparkSession\
    .builder\
    .appName("streaming")\
    .master("local[*]")\
    .getOrCreate()

"""
Données statiques :
"""
supermarket = sparkSession\
    .read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("data/supermarket_sales.csv")

supermarket.printSchema()

supermarket_schema = supermarket.schema

"""
Données streaming :
"""
supermarket_stream = sparkSession\
    .readStream\
    .schema(supermarket_schema)\
    .format("csv")\
    .option("maxFilesPerTrigger", "1")\
    .option("header", "true")\
    .load("data/supermarket_sales.csv")

print("Spark is streaming : ", supermarket_stream.isStreaming)


"""
Aggrégations :
"""
# writeStream.start()

# supermarket_stream.select(count("Invoice ID")).show()
average_rating = supermarket_stream.groupby("Invoice ID")\
    .agg((avg("Rating").alias("Average rating")))\
    .sort(desc("Average rating"))

query = average_rating\
    .writeStream\
    .format("console")\
    .outputMode("complete")\
    .start()

query.stop()


total_price_calculated = supermarket_stream.selectExpr(
    "Invoice ID", "(Unit price * Quantity) as (Total cost)")\
    .groupBy(col("Invoice ID"))\
    .sum("Total cost")

total_price_calculated.writeStream\
    .format("memory")\
    .queryName("customer_table")\
    .outputMode("complete")\
    .start

"""
query = supermarket_stream\
    .writeStream\
    .outputMode("complete")\
    .option("checkpointLocation", "/some/location/")\
    .queryName("test_stream")\
    .format("memory")\
    .start()

"""



"""
Requêtes :
"""






