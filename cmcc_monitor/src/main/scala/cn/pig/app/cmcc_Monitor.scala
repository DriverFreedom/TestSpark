package cn.pig.app

import java.text.SimpleDateFormat

import cn.pig.utils.{AppParameters, Jpools}
import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object cmcc_Monitor {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName("中国移动实时监控平台")
    //rdd序列化 以减少内存的占用  最基本的优化
    conf.set("spark.serialize","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.streaming.kafka.maxRatePerPartition","10000")   //从kafka拉取数据限速 (5)*(分区个数)*（采集数据时间）
    conf.set("spark.streaming.kafka.stopGracefullyOnShutdown","true")     //优雅的停止关闭(但有新的任务时，先完成这个，才去做别的)
    //创建实时流
    val ssc = new StreamingContext(conf,Seconds(2))

    //创建kafka实时流
    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](AppParameters.topic,AppParameters.kafkaParams))

    /**
      * 开始计算
      */
    stream.foreachRDD(rdd => {
    /*  val value = rdd.map(x => x.value())
      value.foreach(println)*/

      val baseRdd = rdd.map(x => x.value()).map(x => JSON.parseObject(x))
        .filter(x => x.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq")).cache()

      /**
        * 获取充值订单量
        */
      val orderNum = baseRdd.map(base =>{
        //获取日期
        val data = base.getString("requestId").substring(0,8)
        val result = base.getString("bussinessRst")
        val num = if(result.equals("0000")) 1 else 0

        (data,num)
      }).reduceByKey(_+_)

      /**
        * 保存到Redis中
        */
        orderNum.foreach(x => {
          val redis = Jpools.getJedis
          redis.incrBy(x._1,x._2)
          redis.close()
        })


      //println("充值业务量:")
      orderNum.foreach(println)

      /**
        * 获取充值金额
        */
      val moneySum = baseRdd.map(x => {
        val data = x.getString("requestId").substring(0,8)
        //如果充值成功才计算金额
        val money = if(x.getString("bussinessRst").equals("0000")) x.getString("chargefee").toDouble else 0
        (data,money)
      }).reduceByKey(_+_)
     // println("充值金额：")
      //moneySum.foreach(println)

      /**
        * 计算充值成功率
        */
        //总的订单数
      val totalOrderNum: Long = baseRdd.count()
      //充值成功的订单数
      val sucessOrderNum: RDD[(String, Long)] = baseRdd.map(x => {
        val data = x.getString("requestId").substring(0,8)
        val num: Int = if(x.getString("bussinessRst").equals("0000")) 1 else 0
        (data,num.toLong)
      }).reduceByKey(_+_)

     // print(totalOrderNum+"  ")
      sucessOrderNum.foreach(println)

      /**
        * 充值平均时长
        */


      val time = baseRdd.map(x => {
        //日期
        val requestId = x.getString("requestId")
        val data = requestId.substring(0,8)
        //开始时间
        val startTime = requestId.substring(0,17)
        //结束时间
        val endTimer = x.getString("receiveNotifyTime")
        val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
        //支付成功的时间差
        val t = if(x.getString("bussinessRst").equals("0000")) format.parse(endTimer).getTime - format.parse(startTime).getTime else 0
        (data,t)
      })
      time.foreach(println)

    })


    ssc.start()

    ssc.awaitTermination()
  }
}
