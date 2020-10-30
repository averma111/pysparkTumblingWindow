from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType
from lib.util import get_spark_app_config, read_kafka_stream, write_output_console
from pyspark.sql import SparkSession
from lib.logger import Log4j
from pyspark.sql import functions as F

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting  pyspark kafka application")

    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", IntegerType()),
        StructField("BrokerCode", StringType())
    ])

    kafka_df = read_kafka_stream(spark)

    value_df = kafka_df.select(F.from_json(F.col("value").cast("string"), stock_schema).alias("value"))

    # value_df.printSchema()

    # Dataframe transformation

    trade_df = value_df.select("value.*") \
        .withColumn("CreatedTime", F.to_timestamp(F.col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Buy", F.expr("CASE WHEN Type =='BUY' then Amount else 0 end")) \
        .withColumn("Sell", F.expr("CASE WHEN Type =='SELL' then Amount else 0 end"))

    # trade_df.printSchema()

    window_agg_df = trade_df \
        .groupBy(  # col("BrokerCode"),
        F.window(F.col("CreatedTime"), "15 minute")) \
        .agg(F.sum(('Buy').cast("int")).alias("TotalBuy"),sum(('Sell').cast("int")).alias("TotalSell")
             )

  #  window_agg_df.printSchema()

    output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")

    logger.info("Waiting for Query")
window_query = write_output_console(output_df)

window_query.awaitTermination()
