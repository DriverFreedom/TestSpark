package day03

import java.net.URL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 根据学科取得的最受欢迎的前2名老师的排序
  */
object FavTeacher2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val subjects = Array("javaee","bigdata","python")
    val conf = new SparkConf().setAppName("Favteacher").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d:/data/teacher.log")
    //处理数据
    val word: RDD[((String, String), Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/")+1)
      val url = new URL(line).getHost
      val subject = url.substring(0,url.indexOf("."))
      ((subject,teacher),1)
    })
    //聚合
    val reduced = word.reduceByKey(_+_)

    // val sorted = reduced.sortBy(_._2,false)
    //分组
    // val grouped = reduced.groupBy(_._1._1)
    //先将学科进行过滤，一个学科的数据放到一个RDD中
    for(sb <- subjects){
      //对所有数据进行过滤
      val filtered = reduced.filter(_._1._1 == sb)
      //在一个学科中进行排序(RDD排序是内存+磁盘)
      val sorted = filtered.sortBy(_._2,false).take(2)
      println(sorted.toBuffer)
    }

  }
}
