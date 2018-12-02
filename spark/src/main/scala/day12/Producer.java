package day12;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws  Exception{
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.setProperty("key.serializer",StringSerializer.class.getName());
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //发送数据的时候时候应答 默认1
        //props.setProperty("scks","1");
        //自定义分区 默认为
        //org.apache.kafka.clients.producer.internals.DefaultPartitioner

        // props.setProperty("partitioner.class","org.apache.kafka.clients.producer.internals.DefaultPartitioner");

        //创建生产者实例
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(props);
        int count = 0;
        while(count < 10000000){
           // int partitionNum = count%3;
            ProducerRecord record = new ProducerRecord("test",0,"","NO:"+count);
            kafkaProducer.send(record);
            count++;
            Thread.sleep(1000);
        }

        kafkaProducer.close();
    }
}
