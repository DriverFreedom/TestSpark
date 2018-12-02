package cn.pig.utils

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object ApiUtils {


  /**
    * 解析文件 获取基础数据 并保存到内存中
    * @param baseRdd
    * @return
    */
  def Api_BaseDate(baseRdd: RDD[ConsumerRecord[String, String]]): RDD[(String, String, List[Double], String, String)] = {
    val baseData: RDD[(String, String, List[Double], String, String)] =
      baseRdd.map(rdd => JSON.parseObject(rdd.value()))
        .filter(x => x.getString("serviceName").equals("reChargeNotifyReq"))
        .map(rdd => {
          //事物结果
          val result = rdd.getString("bussinessRst")
          //获得充值金额
          val fee = rdd.getString("chargefee").toDouble
          //获取省份
          val provinceCode = rdd.getString("provinceCode")
          println(provinceCode)
          //获取充值得发起时间和结束时间
          val requestId = rdd.getString("requestId")
          //获取日期
          val data = requestId.substring(0, 8)
          //小时
          val hour = requestId.substring(8, 10)
          //分钟
          val minute = requestId.substring(10, 12)
          //充值结束的时间
          val receiveTime = rdd.getString("receiveNotifyTime")

          val time = CalculateTools.getDate(requestId, receiveTime)
          val SuccedResult: (Int, Double, Long) = if (result.equals("0000")) (1, fee, time) else (0, 0, 0)


          (data, hour,List[Double](1, SuccedResult._1, SuccedResult._2, SuccedResult._3), provinceCode,minute)

        }).cache()
    baseData
  }


  /**
    * 业务概况
    * 充值订单量、充值金额、充值时长
    * @param baseData
    */
  def Api_general_total(baseData: RDD[(String, String, List[Double], String,String)]) = {

    baseData.map(tp => (tp._1, tp._3)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachPartition(partition => {

      /**
        * 保存到redis中
        */
      val redis = Jpools.getJedis
      partition.foreach(tp => {

        println(tp._1)
        redis.hincrBy("A-" + tp._1, "total", tp._2(0).toLong)
        redis.hincrBy("A-" + tp._1, "success", tp._2(1).toLong)
        redis.hincrBy("A-" + tp._1, "money", tp._2(2).toLong)
        redis.hincrBy("A-" + tp._1, "time", tp._2(3).toLong)
        //redis.expire("A-" + tp._1, 60 * 60 * 48)
      })
      redis.close()
    })
  }

  /**
    * 实时充值办理业务趋势
    * 每小时的订单量和成功的订单量
    * @param baseData
    */
   def api_general_hour(baseData: RDD[(String, String, List[Double], String,String)]) = {
    baseData.map(tp => ((tp._1, tp._2), (tp._3))).reduceByKey((list1, list2) => {
      list1.zip(list2).map(tp => tp._1 + tp._2)
    }).foreachPartition(tp => {
      val redis = Jpools.getJedis
      tp.foreach(data => {
        redis.hincrBy("B-" + data._1._1, "total-" + data._1._2, data._2(0).toLong)
        redis.hincrBy("B-" + data._1._1, "success-" + data._1._2, data._2(1).toLong)
        //redis.expire("B-" + data._1._1, 60 * 60 * 48)
      })
      redis.close()
    })
  }

  /**
    * 全国各省充值业务失败量分布
    * 使用广播的方式获取省份名称
    * 日期 时间hour list（成功标记位，金额，
    */
  def api_general_province(baseData: RDD[(String, String, List[Double], String,String)],provinceName:Broadcast[Map[String, AnyRef]]) = {
    baseData.map(tp => ((tp._1,tp._4),tp._3)).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(tp =>tp._1+tp._2)
    })
      .foreachPartition(tp => {
        val redis = Jpools.getJedis
        tp.foreach(data =>{
          //redis.hincrBy("C-"+data._1._1,"total-"+provinceName.value.getOrElse(data._1._2,data._1._2),data._2(0).toLong)
          redis.hincrBy("C-"+data._1._1,provinceName.value.getOrElse(data._1._2,data._1._2)+"",data._2(1).toLong)
         // redis.expire("C-"+data._1._1,60*60*48)
        })
        redis.close()
      })
  }

  /**
  * 实时统计每分钟的充值金额和订单量
    */
  def api_realtime_minute(baseData: RDD[(String, String, List[Double], String,String)]) = {
    baseData.map(tp => ((tp._1,tp._2,tp._5),List(tp._3(1),tp._3(2)))).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(tp =>tp._1+tp._2)
    })
      .foreachPartition(tp => {
        val redis = Jpools.getJedis
        tp.foreach(data =>{
          redis.hincrBy("D-"+data._1._1,"Num-"+data._1._2+data._1._3,data._2(0).toLong)
          redis.hincrBy("D-"+data._1._1,"Money-"+data._1._2+data._1._3,data._2(1).toLong)
          //redis.expire("D-"+data._1._1,60*60*48)

        })
        redis.close()
      })

  }





}
