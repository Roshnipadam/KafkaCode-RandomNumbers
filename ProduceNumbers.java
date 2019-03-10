package com.random.kafka;
import java.util.*;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
public class ProduceNumbers {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        Random Randnum= new Random();
        int num;
        List<Integer> listNum = new ArrayList<>();

        try{
            for(int i = 0; i < 10; i++){
//               System.out.println(i);
                num = Randnum.nextInt(98)+1;
                listNum.add(num);

                kafkaProducer.send(new ProducerRecord("random-numbers", Integer.toString(i), String.valueOf(listNum)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }

}
