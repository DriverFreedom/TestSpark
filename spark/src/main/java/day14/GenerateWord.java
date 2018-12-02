package day14;

/**
 * 创建一个生产者 生成随机的key 和 字母
 * 用于实现实时流统计词频 并 存储到redis
 */
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class GenerateWord {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");//kafka的brokers列表
        //key和value的序列化方式，因为需要网络传输所以需要序列化
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建一个生产者的客户端实例
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        while(true){
            Thread.sleep(500);
            String key = UUID.randomUUID().toString();
            int value = new Random().nextInt(26) + 97;
            char word = (char)value;
            ProducerRecord<String,String> record = new ProducerRecord("wordcount",key,String.valueOf(word));
            kafkaProducer.send(record);
            System.out.println("record = " + record);

        }

    }
}
