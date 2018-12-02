package day12

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

/**
  * SparkStreaming整合kafka
  */
object SparkStreaming_kafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
    val ssc = new StreamingContext(conf,Seconds(5))


    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "day12_005",
      "auto.offset.reset" -> "earliest",
      "enable.auto.comit" -> (false:java.lang.Boolean)
    )


    //指定主题
    val topics = Array("test")

    /**
      * 指定kafka数据源
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    /* val maped: DStream[(String, String)] = stream.map(record => (record.key,record.value))
    maped.foreachRDD(rdd => {
    //计算逻辑
    rdd.foreach(println)

  })*/

    stream.foreachRDD(rdd => {
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val maped: RDD[(String, String)] = rdd.map(record => (record.key,record.value))
      //计算逻辑
      maped.foreach(println)
      //循环输出
      for(o <- offsetRange){
        println(s"${o.topic}  ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    })

    //启动程序
    ssc.start()

    //等待程序被终止
    ssc.awaitTermination()





  }

}
