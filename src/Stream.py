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

sparkSession.sparkContext.setLogLevel("ERROR")
sparkSession.conf.set("spark.sql.shuffle.partitions", "5")

"""
Données statiques :
"""
supermarket = sparkSession\
    .read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("../data/supermarket_sales.csv")

supermarket.printSchema()

supermarket_schema = supermarket.schema

supermarket.createOrReplaceTempView("supermarket_table")

"""
Données streaming :
"""
supermarket_stream = sparkSession\
    .readStream\
    .schema(supermarket_schema)\
    .format("csv")\
    .option("maxFilesPerTrigger", "1")\
    .option("header", "true")\
    .load("../data/supermarket_sales.csv")

print("Spark is streaming : ", supermarket_stream.isStreaming)


"""
Aggrégations :
"""
# writeStream.start()

# supermarket_stream.select(count("Invoice ID")).show()
print("QUERY average_rating start")
average_rating = supermarket_stream.groupby("Invoice_ID")\
    .agg((avg("Rating").alias("Average_rating")))\
    .sort(desc("Average_rating"))

query = average_rating\
    .writeStream\
    .format("console")\
    .outputMode("complete")\
    .start()

# TODO : afficher le résultat de query : average_rating
print("QUERY average_rating end")


total_price_calculated = supermarket_stream.selectExpr(
    "Invoice_ID", "(Unit_price * Quantity) as (Total_cost)")\
    .groupBy(col("Invoice_ID"))\
    .sum("Total_cost")

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






