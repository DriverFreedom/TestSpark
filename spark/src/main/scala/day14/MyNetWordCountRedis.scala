package day14


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.JPools


/**
  * 将kafka  streaming 和 redis整合 实现词频统计
  */
object MyNetWordCountRedis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //创建SparkStreaming 对象
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    conf.set("spark.streaming.kafka.maxRatePerPartition","5")   //从kafka拉取数据限速 (5)*(分区个数)*（采集数据时间）
    conf.set("spark.streaming.kafka.stopGracefullyOnShutdown","true")     //优雅的停止关闭
    val ssc = new StreamingContext(conf,Seconds(2))

    //定义一个消费者
    val groupId = "day14_001"
    //定义一个主题
    val topic = "wordcount"

    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //连接到kafka数据源
    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,          //位置策略（可用的Executor上均匀分配分区）
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams))

    //创建在Master节点的Driver
    stream.foreachRDD(rdd => {
      val reduced = rdd.map(x => (x.value(),1)).reduceByKey(_+_)
      reduced.foreachPartition(rdd => {
        //连接redis
        val redis = JPools.getJedis

        rdd.foreach({x => redis.hincrBy("wordcount",x._1,x._2.toLong)})

        redis.close()
      })
    })

    ssc.start()

    ssc.awaitTermination()

  }

}
