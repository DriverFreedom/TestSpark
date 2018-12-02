package day06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("test4").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d:/data/spark/access_2013_05_31.log")
    val endpoint = lines.map(x => {
      if(ApacheAccessLog.isValidateLogLine(x)){
        ApacheAccessLog.parseLogLine(x).endpoint
      }
    })
    //变为元组类型
    val map = endpoint.map(x => (x,1))
    //聚合
    val reduced = map.reduceByKey(_+_)
    //过滤
    val filtered = reduced.filter(!_._1.toString.eq("()"))
    //排序
    val sorted = filtered.sortBy(-_._2)
    println("输出资源下载了最多的前3个")
    println(sorted.take(3).toBuffer)
  }

}
