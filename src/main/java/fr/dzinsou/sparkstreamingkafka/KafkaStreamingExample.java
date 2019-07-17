package fr.dzinsou.sparkstreamingkafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class KafkaStreamingExample {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamingExample.class);

    public static void main(String[] args) throws IOException {
        int streamingDurationSeconds = Integer.parseInt(args[0]);
        String topicName = args[1];
        String consumerPropertiesFile = args[2];

        SparkConf conf = new SparkConf().setAppName("KafkaStreamingExample");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(streamingDurationSeconds));

        Set<String> topics = Collections.singleton(topicName);
        Map<String, String> kafkaParams = new HashMap<>();

        Properties consumerProperties = new Properties();
        try (FileReader reader = new FileReader(consumerPropertiesFile)) {
            consumerProperties.load(reader);
        }
        for (Object o : consumerProperties.keySet()) {
            String key = (String) o;
            String value = consumerProperties.getProperty(key);
            kafkaParams.put(key, value);
        }

        JavaPairInputDStream<String, String> directKafkaStream =
                KafkaUtils.createDirectStream(ssc,
                        String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> LOGGER.info(record._2));
        });

        // Start streaming context
        ssc.start();
        ssc.awaitTermination();
    }
}
