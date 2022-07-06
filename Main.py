from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Projet Spark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


schema1 = StructType([StructField('Invoice_ID', StringType(), True),
                      StructField('Branch', StringType(), True),
                      StructField('City', StringType(), True),
                      StructField('Customer_type', StringType(), True),
                      StructField('Gender', StringType(), True),
                      StructField('Product_line', StringType(), True),
                      StructField('Unit_price', DoubleType(), True),
                      StructField('Quantity', DoubleType(), True),
                      StructField('Tax', DoubleType(), True),
                      StructField('Total', DoubleType(), True),
                      StructField('Date', StringType(), True),
                      StructField('Time', StringType(), True),
                      StructField('Payment', StringType(), True),
                      StructField('cogs', DoubleType(), True),
                      StructField('gross_margin_percentage', DoubleType(), True),
                      StructField('gross_income', DoubleType(), True),
                      StructField('Rating', DoubleType(), True),
                      ])

sales = spark.read.format('csv') \
    .schema(schema1) \
    .option('header', True) \
    .option("delimiter", ",") \
    .load(r'supermarket_sales.csv')


sales = sales.withColumn("month_day",
                         concat_ws("-", split(sales["Date"], '/').getItem(0), split(sales["Date"], '/').getItem(1)))

liste_dates = sales.select("month_day").distinct().collect()
for i in liste_dates:
    df_day = sales.filter(sales.month_day == i["month_day"])
    ppath = 'sales_dates//' + i["month_day"] + '.csv'
    df_day.show()

    df_day.repartition(1).write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save('sales_dates//' + i["month_day"] + '.csv')

schema2 = StructType([StructField('Invoice_ID', StringType(), True),
                      StructField('Branch', StringType(), True),
                      StructField('City', StringType(), True),
                      StructField('Customer_type', StringType(), True),
                      StructField('Gender', StringType(), True),
                      StructField('Product_line', StringType(), True),
                      StructField('Unit_price', DoubleType(), True),
                      StructField('Quantity', DoubleType(), True),
                      StructField('Tax', DoubleType(), True),
                      StructField('Total', DoubleType(), True),
                      StructField('Date', StringType(), True),
                      StructField('Time', StringType(), True),
                      StructField('Payment', StringType(), True),
                      StructField('cogs', DoubleType(), True),
                      StructField('gross_margin_percentage', DoubleType(), True),
                      StructField('gross_income', DoubleType(), True),
                      StructField('Rating', DoubleType(), True),
                      StructField('month_day', StringType(), True)
                      ])

sales_streaming = spark.readStream.format('csv') \
    .schema(schema2) \
    .option("recursiveFileLookup", "true") \
    .option("header", "true") \
    .csv("sales_dates/*.csv")

print("spark is streaming ", sales_streaming.isStreaming)

