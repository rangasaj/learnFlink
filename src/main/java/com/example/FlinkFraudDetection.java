package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.dsv2.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkFraudDetection {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> kafkaConsumer = getKafkaConsumer();
        //var kafkaConsumer = null;

        DataStream<User> userStream = env.fromData(UserService.getAllUsers());
        DataStream<Activity> historicalActivityStream = env.fromData(ActivityService.getHistoricalActivities());

        DataStream<String> envStream = env.addSource(kafkaConsumer);
       // env.addSource(userStream);
        //env.addSource(historicalActivityStream);

        DataStream<Tuple2<String, Integer>> counts =
                envStream.flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);
        counts.print().setParallelism(4);

        env.execute("WordCount Example");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] words = value.toLowerCase().split("\\W+");

            // Emit the tokens
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }

    private static FlinkKafkaConsumer<String> getKafkaConsumer() {
        Properties kafkaProps = new Properties();
        //kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        //kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_test");
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id","group1");

        String topic = "activity";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                topic, new SimpleStringSchema(), kafkaProps);
/*
        var topic = "source_topic";

        var kafkaConsumer = KafkaSource.builder[String]
                .setBootstrapServers(kafkaBroker)
                .setTopics(topic)
                .setGroupId("consumer_test")
                .setProperties(kafkaProps)
                .setValueOnlyDeserializer(new SimpleStringSchema)
                .build*/
        return consumer;
    }
}