package day06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]").setAppName("test1")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d:/data/spark/access_2013_05_31.log")
    val countSize: RDD[AnyVal] = lines.map(line => {
      if(ApacheAccessLog.isValidateLogLine(line)){
        val list = ApacheAccessLog.parseLogLine(line)
         list.contentSize
      }
    })
    //countSize.map()
    //过滤空值
    val a = countSize.filter(!_.toString.equals("()"))
      //转为Int类型
    val i = a.map(_.toString.toLong)

    println("最大值"+i.collect().toBuffer.toList.max)
    println("最小值"+i.collect().toBuffer.toList.min)

    val sum =  i.reduce(_+_)
    val count = i.collect().length
    println("平均值"+ sum/count)
  }

}
