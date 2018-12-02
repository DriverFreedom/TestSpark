package TestTwo
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
object No_4 {

  val ipFile = "D:\\QQFiles\\数据\\大题二\\ip.txt"
  val orderFile = "D:\\QQFiles\\数据\\大题二\\order.log"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Order").master("local[*]").getOrCreate()

    import spark.implicits._
    //读取Ip文件
    val ip = spark.read.textFile(ipFile)
    //整理ip文件
    val ipDF = ip.map(line => {
      val splited = line.split("[|]")
      val startNum = splited(2).toLong
      val endNum = splited(3).toLong
      val province = splited(6)
      (startNum,endNum,province)

    }).toDF("start_num","end_num","province")
    //将ipDF注册成view
    ipDF.createOrReplaceTempView("t_ip")
    //读取order文件
    val order = spark.read.textFile(orderFile)
    //处理order文件
    val orderDF = order.map(line => {
      val splited = line.split(" ")
      val ip = ip2Long(splited(1))
      val price = splited(4).toInt
      (ip,price)
    }).toDF("ip","price")
    //将order文件映射为view
    orderDF.createOrReplaceTempView("t_order")

    //关联两张表 sql查询
    val result: DataFrame = spark.sql("select province,count(price) count_price from t_ip join t_order on ip >= start_num and ip <= end_num group by province order by count_price desc limit 3")

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    //prop.put("driver","com.mysql.jdbc.Driver")
    result.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test","provinceOrder",prop)
    result.show()
    spark.stop()



  }

  //ip转换函数
  def ip2Long(ip:String):Long ={
    val fragments = ip.split("[.]")
    var ipNum =0L
    for(i<- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
