package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object IPDemo {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val ipFile = ("d:\\data\\spark\\ip.txt")
  val acessFile = "d:\\data\\spark\\access.log"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Ip").master("local[*]").getOrCreate()
    import spark.implicits._
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
    val ipDF = ipRules.toDF("start_num","end_num","province")
    //将ip注册成view
    ipDF.createTempView("t_ip")
    //读取访问日志文件
    val access_file = spark.read.textFile(acessFile)
    import day07.MyUtils
    val accessDF = access_file.map(line =>{
      val fields = line.split("[|]")
      val ip = fields(1)
      MyUtils.ip2Long(ip)
    }).toDF("ip")
    //将访问日志整理成视图
    accessDF.createTempView("t_access")
    //sql语句 关联两张表
    val result = spark.sql("SELECT province,count(*) counts FROM t_ip JOIN t_access ON ip>=start_num and ip<=end_num GROUP BY province ORDER BY counts DESC")
    result.show();
    spark.stop()

  }

}
