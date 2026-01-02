from pyspark.sql import SparkSession, DataFrame

class KafkaIO:
    def __init__(self, spark: SparkSession, brokers: str):
        self.spark = spark
        self.brokers = brokers

    def read_stream(self, topics: str) -> DataFrame:
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.brokers) \
            .option("subscribe", topics) \
            .option("startingOffsets", "latest") \
            .load()

    def write_console(self, df: DataFrame):
        return df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

    def write_csv(self, df: DataFrame, path: str, checkpoint_path: str):
        """
        Writes the stream to a CSV folder.
        
        :param path: The output directory (e.g., ./data/output)
        :param checkpoint_path: Directory for Spark metadata (REQUIRED for file sinks)
        """
        return df.writeStream \
            .format("csv") \
            .outputMode("append") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .option("header", "true") \
            .trigger(processingTime="1 minute") \
            .start()