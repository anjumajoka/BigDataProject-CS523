package cs523.finalproject.spark.hive;

import cs523.finalproject.util.AppConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

public class StreamDataToHive {

    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder()
                .appName("Spark Kafka Integration Structured Tweet Streaming")
                .master("local[*]").getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", AppConstants.TWEETER_DATA_TOPIC)
                .load();

        Dataset<Row> lines = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        StreamingQuery query = lines.writeStream().outputMode("append").format("console").start();
        lines
                .coalesce(1)
                .writeStream()
                .format("csv")
                // can be "orc", "json", "csv", "parquet" etc.
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(10))
                .option("truncate", false)
                .option("maxRecordsPerFile", 10000)
                .option("path",
                        "hdfs://localhost:8020/user/anju/twitterTweets")
                .option("checkpointLocation",
                        "hdfs://localhost:8020/user/anju/twitterTweetsCheckpoint") // args[1].toString()
                .start().awaitTermination();


    }
}
