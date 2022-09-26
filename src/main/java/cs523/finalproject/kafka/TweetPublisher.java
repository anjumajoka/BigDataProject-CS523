package cs523.finalproject.kafka;


import com.google.gson.reflect.TypeToken;
import com.twitter.clientlib.JSON;
import com.twitter.clientlib.model.*;
import cs523.finalproject.analyzer.SentimentAnalyzer;
import cs523.finalproject.mapper.MapToCsvLine;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka publisher to publish the messages to kafka topic.
 * Also converte
 */
public class TweetPublisher {

    public final static String TOPIC = "TwitterDataAnalytics";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private Producer<String, String> producer = new KafkaProducer<>(getKafkaProperties());


    @NotNull
    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "tweet-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }


    public void sendMessage(String message) throws Exception {

        long time = System.currentTimeMillis();
        String key = UUID.randomUUID().toString();
        try {
            final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, MapToCsvLine.convertMessageToCommaSeperated(message));
            RecordMetadata metadata = producer.send(record).get();
            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}