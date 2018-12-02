package day06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("test2").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //读取文件
    val lines = sc.textFile("d:/data/spark/access_2013_05_31.log")
    val line = lines.map(line =>{
      if(ApacheAccessLog.isValidateLogLine(line)){
        ApacheAccessLog.parseLogLine(line).responseCode
      }
    })
    //变为元组
    val result = line.map(x => (x,1))
    //聚合
    val reduceed = result.reduceByKey(_+_)
    //过滤
    val filtered = reduceed.filter(!_._1.toString.eq("()"))
    filtered.foreach(println)


  }

}
