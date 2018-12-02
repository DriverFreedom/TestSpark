package day12

import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.hadoop.yarn.lib.ZKClient
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * zk管理kafka的offset
  */
class SSCDirectKafka010_ZK_Offset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("orh.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getSimpleName}")
    //设置批次为2s时间
    val ssc = new StreamingContext(conf,Seconds(2))
    val groupId = "day13_001"

    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = "testTopic"
    val topics = Array(topic)

    /**
      * 分为两种情况
      * 1、第一次启动 从earlist开始消费数据
      * 2、不是第一次启动 从上一次存储开始
      */
    val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId,topic)

    /**
      * 生成的目录结构
      */
    val offsetDir = zkGroupTopicDirs.consumerOffsetDir
    //zk字符串连接组
    val zkGroups = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    /**
      * 创建一个zkClient连接
      * 判断/customer/day13_001/offset/testTopic
      * 下面没有孩子节点 如果有说明之前维护过偏移量；如果没有，说明程序是第一次执行
      */
    val zkClient = new ZkClient(zkGroups)
    val childrenCount = zkClient.countChildren(offsetDir)

    val stream = if(childrenCount>0){   //非第一次启动
      var fromOffsets = Map[TopicPartition,Long]()
      (0 until childrenCount).foreach(partitionId =>{
        val offset = zkClient.readData[String](offsetDir+s"/${partitionId}")
        fromOffsets += (new TopicPartition(topic,partitionId) -> offset.toLong)
      })

      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String,String](fromOffsets.keys.toList,kafkaParams,fromOffsets))

    }else{  //第一次启动
      println("第一次启动")
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))
    }



  }

}
















