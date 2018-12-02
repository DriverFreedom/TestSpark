package day02

import java.net.URL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Favteacher").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d:/data/teacher.log")
    val word: RDD[((String, String), Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/")+1)
      val url = new URL(line).getHost
      val subject = url.substring(0,url.indexOf("."))
      ((subject,teacher),1)
    })
    val reduced = word.reduceByKey(_+_)



    val sorted = reduced.sortBy(_._2,false)
    val list = sorted.take(3)
    println(list.toBuffer)

  }
}
