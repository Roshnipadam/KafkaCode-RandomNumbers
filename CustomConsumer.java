package com.random.kafka;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.sources.In;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import org.apache.spark.streaming.Durations;
public class CustomConsumer {

    public static void main(String[] args) {
        Pattern aa = Pattern.compile("(\\[|\\])");
        Pattern mat = Pattern.compile("-?\\d+");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        SparkConf sparkConf = new SparkConf().setAppName("aaa").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Collection<String> topicName = Arrays.asList("fun");

        JavaInputDStream<ConsumerRecord<Integer, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<Integer, String>Subscribe(topicName, kafkaParams)
                );
        //spark streaming actions
        JavaDStream<String> lines1 = stream.map(ConsumerRecord::value);

        System.out.println("Printing DSTREAM");
        lines1.print();

       lines1.foreachRDD(rdd -> {
            List<String> collect = rdd.collect();
           System.out.println("Printing List of String : " + collect);
           List<Integer> intNum = new ArrayList<>();
           for (String numbers: collect) {
               numbers = numbers.replaceAll("\\[", "").replaceAll("\\]","");
               System.out.println("Number : " + numbers);
               String[] split = numbers.split(",");
               int sum = 0;
               for (String num : split) {
                   int i = Integer.parseInt(num.trim());
                   sum = sum + i;
               }
               int avg = sum / split.length;
               System.out.println("SUM : " + sum + " Lenght : " + split.length);
               System.out.println("Average : " + avg
               );
           }

        });



    //

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }}
