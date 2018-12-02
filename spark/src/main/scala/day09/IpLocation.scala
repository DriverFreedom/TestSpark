package day09

/**
  * 两表联合效率较低
  */

import day07.MyUtils
import org.apache.spark.sql.{Dataset, SparkSession}

object IpLocation {
  val ipFile = "d:\\data\\spark\\ip.txt"
  val acessFile = "d:\\data\\spark\\access.log"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLIPLocation").master("local[*]").getOrCreate()
    import  spark.implicits._
    //读取ip文件
    val ipFile = spark.read.textFile("d:\\data\\spark\\ip.txt")
    //整理ip文件
    val ipRules: Dataset[(Long, Long, String)] = ipFile.map(line => {
      val splited = line.split("[|]")
      val startNum = splited(2).toLong
      val endNum = splited(3).toLong
      val province = splited(6)
      (startNum,endNum,province)
    })
    //加入元数据
    //val ipDF = ipRules.toDF("start_num","end_num","province")

    //将全部的IP规则收集到Driver端
    val ipRulesDriver = ipRules.collect()
    //广播 阻塞的方法 没有广播完，就不会向下
    val broadcastRef = spark.sparkContext.broadcast(ipRulesDriver)

    //读取web日志
    val accessLogLines = spark.read.textFile(acessFile)
    val ips = accessLogLines.map(line => {
      val Fields = line.split("[|]")
      val ip = Fields(1)
      MyUtils.ip2Long(ip)
    }).toDF("ip_num")
    //将访问日志数据注册成视图
    ips.createTempView("access_ip")

    //定义并注册自定义函数
    //自定义函数在哪里定义的?  (Driver)  业务逻辑在Executor执行
    spark.udf.register("ip_num2Province",(ip_num:Long)=>{
      //获取广播到Driver
      //根据Driver端的广播变量引用，在发送task时，会将Driver端的引用伴随着发送到Executor
      val rulesExecute: Array[(Long, Long, String)] = broadcastRef.value
      val index = MyUtils.binarySearch(rulesExecute,ip_num)
      var province = "未知"
      if(index != -1){
        province = rulesExecute(index)._3
      }
      province
    })

    val result = spark.sql("select ip_num2Province(ip_num) province,count(*) counts from access_ip group by province order by counts desc")

    result.show()

    spark.stop()

  }

}
