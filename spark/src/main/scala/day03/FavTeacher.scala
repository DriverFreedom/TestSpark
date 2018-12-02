package day03

/**
  * 求每个学科最受欢迎的两名老师
  */

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

    //reduced.foreach(println)
   // val sorted = reduced.sortBy(_._2,false)
    //分组
    val grouped = reduced.groupBy(_._1._1)
    //排序 取前两名 取到的数据是scala中进行排序的
    //先分组 然后在组内进行排序 这里的ComoactBuffer是迭代器，继承了序列，然后迭代器转换成List进行排序
    //在某种极端情况下，_表示迭代分区的数据，证明这里是将迭代器的数据一次性的来过来后进行toList,如果数据量非常大，这里肯定会出现OOM(内存溢出)
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(-_._2).take(2))


    sorted.foreach(println)

    //释放资源
    sc.stop()
  }
}
