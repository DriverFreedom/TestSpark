package TestOne

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object No_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ratingFile = sc.textFile("D:\\QQFiles\\数据\\大题一\\ml-1m\\ratings.dat")
    val userFile = sc.textFile("D:\\QQFiles\\数据\\大题一\\ml-1m\\users.dat")

    val users = userFile.map(lines => {
      val splited = lines.split("::")
      (splited(0).toInt,splited(1).toString,splited(2).toString)
    }).filter(_._2.equals("M")).filter(_._3.equals("18"))
    val user: RDD[Int] = users.map(x => x._1)

    val broadcast = sc.broadcast(user.collect())

    val ratings: RDD[(Int, Int)] = ratingFile.map(line => {
      var splited = line.split("::")
      var user = splited(0).toInt
      var movieId = splited(1).toInt;
      (user,movieId)
    })

    val rating = ratings.groupByKey().map(line => {
      var num = 0;
      for (x <- line._2){
        num = num+1;
      }
      (line._1,num)
    })

    val rate = rating.filter(x => broadcast.value.contains(x._1))
    println("年龄段在“18-24”的男性年轻人，最喜欢(次数)看哪10部电影：")
    val result = rate.sortBy(- _._2).take(10).foreach(println)



  }

}
