package day15_CMCC

import day11.SparkStreaming_SparkSql
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.json4s.jackson.JsonMethods._
import org.json4s._

object cmccTest {

  /**
    *{"bussinessRst":"0000","channelCode":"6900","chargefee":"1000","clientIp":"117.136.79.101","gateway_id":"WXPAY","interFacRst":"0000",
    * "logOutTime":"20170412030030067","orderId":"384663607178845909","payPhoneNo":"15015541313","phoneno":"15015541313","provinceCode":"200",
    * "rateoperateid":"1513","receiveNotifyTime":"20170412030030017","requestId":"20170412030007090581518228485394","retMsg":"接口调用成功",
    * "serverIp":"10.255.254.10","serverPort":"8714","serviceName":"payNotifyReq","shouldfee":"1000","srcChannel":"11","sysId":"01"}
    */
  case class log(bussinessRst:String,channelCode:String,chargefee:String,clientIp:String,gateway_id:String,interFacRst:String,logOutTime:String,orderId:String,payPhoneNo:String,
                 phoneno:String, provinceCode:String,rateoperateid:String,receiveNotifyTime:String,requestId:String,retMsg:String,serverIp:String,serverPort:String,serviceName:String,
                 shouldfee:String,srcChannel:String,sysId:String)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName("cmccLoggerStreaming")
    conf.set("spark.streaming.kafka.maxRatePerPartition","5")   //从kafka拉取数据限速 (5)*(分区个数)*（采集数据时间）
    conf.set("spark.streaming.kafka.stopGracefullyOnShutdown","true")     //优雅的停止关闭
    val ssc = new StreamingContext(conf,Seconds(2))

    val topic = "cmccTopic"
    val groupId = "day15_001"

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

    //从redis中获取Offset
    //val fromOffset = JedisOffSet(groupId)
    //连接到kafka数据源

    val  stream =
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParams))

    /**
      * else{
      *       KafkaUtils.createDirectStream(ssc,
      *         LocationStrategies.PreferConsistent,
      *         ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkaParams,fromOffset))
      * }
       */

    stream.map(record => record.value()).map(value =>{
      //隐式转换 使用json4s的默认转化器
      implicit val foremats:DefaultFormats.type = DefaultFormats
      val json = parse(value)
      json.extract[log]
    }).foreachRDD(rdd =>{
      rdd.foreachPartition(partition => {
          partition.foreach(x =>{
            println(x.clientIp+"  "+x.retMsg)
          })
      })
    })



  //https://blog.csdn.net/weixin_35040169/article/details/80057561
    ssc.start()
    ssc.awaitTermination()

  }

}
