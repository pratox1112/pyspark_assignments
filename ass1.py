from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_timestamp
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import date_format
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()
def createdataframe():
    schema = StructType([
        StructField("Product Name", StringType(), True),
        StructField("Issue Date", LongType(), True),
        StructField("Price", IntegerType(), True),
        StructField("Brand", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Product Number", StringType())
    ])

    data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", "0001"),
            ("Refrigirator", 1648770999000, 35000, " LG",None, "0002"),
            ("Air Cooler", 1648770948000, 45000, " Voltas",None, "0003")]
    df11 = spark.createDataFrame(data, schema)
    df11.show()
    return df11

def unixtime(df11):
    df_1 = df11.withColumn('Issue Date', from_unixtime(col('Issue Date') / 1000, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZZ'))
    df_1.show()
    return df_1

def todatetype(df_1):
    df_2 = df_1.withColumn('Issue Date', date_format(col('Issue Date'),"yyyy-MM-dd"))
    df_2.show()
    return df_2

def removespace(df_2,df11):
    df_3 = df_2.withColumn("Brand", trim(df11["Brand"]))
    df_3.show()
    return df_3

def replacenull(df_3):
    df_4 = df_3.fillna('')
    df_4.show()
    return df_4

def createdataframe1():
    schema = StructType([
        StructField("SourceId", IntegerType(), True),
        StructField("TransactionNumber", IntegerType(), True),
        StructField("Language", StringType(), True),
        StructField("ModelNumber", IntegerType(), True),
        StructField("StartTime", StringType(), True),
        StructField("ProductNumber", StringType())
    ])

    data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", "0001"),
            (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", "0002"),
            (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", "0003")]

    df22 = spark.createDataFrame(data, schema)
    df22.show()
    return df22

def camelcasetosnakecase(df22):
    new_columns = []
    def change_case(str):
        res = [str[0].lower()]
        for c in str[1:]:
            if c in ('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
                res.append('_')
                res.append(c.lower())
            else:
                res.append(c)
        return ''.join(res)
    for col in df22.columns:
        new_columns.append(change_case(col))
    for i in range(len(df22.columns)):
        df22 = df22.withColumnRenamed(df22.columns[i], new_columns[i])
    df22.show()
    return df22

def starttimems(df22):
    df2 = df22.withColumn("start_time_ms", unix_timestamp(to_timestamp("start_time", "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) * 1000)
    df2.show()
    return df2

def merge(df2,df_4):
    df2 = df2.withColumnRenamed("product_number", "Product Number")
    final_df = df_4.join(df2, on="Product Number", how="inner")
    final_df.show()
    return final_df

def filter(final_df):
    final1_df = final_df.filter("language = 'EN'")
    final1_df.show()
    return final1_df