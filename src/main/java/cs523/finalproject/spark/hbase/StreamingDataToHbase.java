package cs523.finalproject.spark.hbase;

import cs523.finalproject.util.AppConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.*;

/**
 * This is a simple example of BulkPut with Spark Streaming
 */
public class StreamingDataToHbase {

    public static void main(String[] args) throws InterruptedException {


        SparkSession spark = SparkSession.builder().appName("Spark Kafka Integration Structured Tweet Streaming").master("local[*]").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(1));

        Map<String, Object> kafkaParams = getKafkaConfigs();
        Collection<String> topics = Arrays.asList(AppConstants.TWEETER_DATA_TOPIC);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        JavaPairDStream<String, String> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        JavaDStream<String> lines = results.map(tuple2 -> tuple2._2());

        sendToHbase(jsc, AppConstants.HBASE_TABLE_NAME, lines);
        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @NotNull
    private static Map<String, Object> getKafkaConfigs() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", AppConstants.KAKFA_BBOTSTRAP_SERVER);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", AppConstants.HBASE_CONSUMER_GROUP_ID);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    private static void sendToHbase(JavaSparkContext jsc, String tableName, JavaDStream<String> lines) {
        Configuration conf = HBaseConfiguration.create();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
        hbaseContext.streamBulkPut(lines, TableName.valueOf(tableName), new PutFunction());
    }


    public static class PutFunction implements Function<String, Put> {

        private static final long serialVersionUID = 8038080193924048202L;

        public Put call(String values) throws Exception {
            System.out.println("PUT is " + values);
            String[] val = values.split(",");
            byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
            byte[] family = Bytes.toBytes("tweet_data");
            return new Put(row)
                    .addColumn(family, Bytes.toBytes("Id"), Bytes.toBytes(val[0]))
                    .addColumn(family, Bytes.toBytes("AuthorId"), Bytes.toBytes(val[1]))
                    .addColumn(family, Bytes.toBytes("CreatedAt"), Bytes.toBytes(val[2]))
                    .addColumn(family, Bytes.toBytes("PlaceId"), Bytes.toBytes(val[3]))
                    .addColumn(family, Bytes.toBytes("Coordinates"), Bytes.toBytes(val[4]))
                    .addColumn(family, Bytes.toBytes("PossiblySensitive"), Bytes.toBytes(val[5]))
                    .addColumn(family, Bytes.toBytes("Source"), Bytes.toBytes(val[6]))
                    .addColumn(family, Bytes.toBytes("Text"), Bytes.toBytes(val[7]))
                    .addColumn(family, Bytes.toBytes("Sentiment"), Bytes.toBytes(val[8]));

        }
    }
}