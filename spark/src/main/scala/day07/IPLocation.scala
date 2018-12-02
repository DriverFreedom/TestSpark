package day07

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IPLocation {
  val ipFile = "d:\\data\\spark\\ip.txt"
  val acessFile = "d:\\data\\spark\\access.log"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("IpLocation").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //1.读取IP规则资源库
    val lines = sc.textFile(ipFile)
    //2.整理Ip规则
    val ipRules = lines.map(x => {
      val splited = x.split("[|]")
      val startNum = splited(2).toLong
      val endNum = splited(3).toLong
      val province = splited(6)
      (startNum,endNum,province)
    })
    //println(ipRules.collect().toBuffer)
    //3.将Ip收集起来
    val ipDriver: Array[(Long, Long, String)] = ipRules.collect()
    //4.将IP通过广播的方式发送到executor
    //广播之后，在Driver端获取了广播变量的引用（如果没有广播完，就不往下走）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipDriver)

    //5.读取访问日志
    val access = sc.textFile(acessFile)
    //6.整理访问日志
    val provinces = access.map(x => {
      val fields = x.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)
      //通过广播获取所有ip规则，然后进行匹配
      val allIpRulesExecutor = broadcastRef.value
      //根据规则查找，二分查找
      var province = "未知"
      val index = MyUtils.binarySearch(allIpRulesExecutor,ipNum)
      if(index != -1){
        province = allIpRulesExecutor(index)._3
      }
      (province,1)
    })
    //7.按照省份进行计数
    val reduceRDD: RDD[(String, Int)] = provinces.reduceByKey(_+_)
    //8.打印结果
    //reduceRDD.foreach(println)
    //9.将数据存储到mysql中
    /**
      * reduceRDD.foreach(x => {
      *
      * val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=true","root","123456")
      * val pstm = conn.prepareStatement("insert into access_log values (?,?)")
      *       pstm.setString(1,x._1)
      *       pstm.setInt(2,x._2)
      *       pstm.execute()
      *       pstm.close()
      *       conn.close()
      * })
      */
    //MyUtils.data2MySQL(reduceRDD.collect().toIterator)
    reduceRDD.foreachPartition(MyUtils.data2MySQL(_))
    sc.stop()


  }

}
