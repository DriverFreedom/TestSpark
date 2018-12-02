package day06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test3 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[3]").setAppName("test3")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d:/data/spark/access_2013_05_31.log")
    val line = lines.map(x =>{
      if(ApacheAccessLog.isValidateLogLine(x)){
        val endpoint = ApacheAccessLog.parseLogLine(x).endpoint
        val ip = endpoint.toString.substring(0,endpoint.toString.lastIndexOf("/"))
        ip
      }
    })
    //变为元组
    val tuple = line.map(x => (x,1))
    //聚合
    val redeced = tuple.reduceByKey(_+_)
    //过滤
    val filtered1 = redeced.filter(!_._1.toString.eq("()"))
    val filtered = filtered1.filter(_._1.toString.length != 0)
    //计算访问次数超过100次的网站
    val result: RDD[(Any, Int)] = filtered.filter(_._2 > 1000)
    println("访问次数超过1000次的网站")
    result.foreach(println)
  }
}
