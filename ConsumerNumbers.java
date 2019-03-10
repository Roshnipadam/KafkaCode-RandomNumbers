package com.random.kafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
public class ConsumerNumbers {
    public static <val> void main(String[] args) {


        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        val spark = (val) new SparkConf().setAppName("KafkaWordCount");
        val spa = (val) SparkSession.builder().appName("try2").master("local[2]").getOrCreate();
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("random-numbers");
        kafkaConsumer.subscribe(topics);
//         final KafkaConsumer<String,String> consumer;
        try{
            while (true){

//                ConsumerRecords<K, V> records = consumer.poll(Long.MAX_VALUE);
                ConsumerRecords<String,String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord record: records) {
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }

}
