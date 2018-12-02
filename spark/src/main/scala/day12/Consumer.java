package day12;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args){
        HashMap<String,Object > config = new HashMap<>();
        config.put("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092");
        config.put("key.deserializer",StringDeserializer.class.getName());
        config.put("value.deserializer",StringDeserializer.class.getName());
        config.put("group.id","g000001");
        /**
         * 从哪个位置获取数据
         * [latest,earliest,none]
         */
        config.put("auto.offset.reset","earliest");
        //是否要自动递交偏移量(offset)
        //config.put("enable.auto.commit","false");

        config.put("enable.auto.commit","true");

        config.put("","500");

        //      创建一个消费者客户端实例
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(config);
         consumer.subscribe(Arrays.asList("test"));

                while (true) {
                    //拉去数据， 会从kafka所有分区下拉取数据
                    ConsumerRecords<String, String> records = consumer.poll(2000);
                    Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                    while (iterator.hasNext()) {
                        ConsumerRecord<String, String> record = iterator.next();
                        System.out.println("record = " + record);
                    }
                }

                //释放连接
        // consumer.close();
    }
}
