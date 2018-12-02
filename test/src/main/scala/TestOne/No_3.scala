package TestOne

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object No_3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ratingFile = sc.textFile("D:\\QQFiles\\数据\\大题一\\ml-1m\\ratings.dat")
    val userFile = sc.textFile("D:\\QQFiles\\数据\\大题一\\ml-1m\\users.dat")
    val movieFile = sc.textFile("D:\\QQFiles\\数据\\大题一\\ml-1m\\movies.dat")

    val users = userFile.map(lines => {
      val splited = lines.split("::")
      (splited(0).toInt,splited(1).toString)
    }).filter(_._2.equals("F"))
    val user: RDD[Int] = users.map(x => x._1)
    //使用广播
    val broadcast = sc.broadcast(user.collect())

    val movies = movieFile.map(line => {
      val splited = line.split("::")
      val movieId = splited(0).toInt
      val movieTitle = splited(1)
      (movieId,movieTitle)
    })

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

    val rate: RDD[(Int, Int)] = rating.filter(x => broadcast.value.contains(x._1))
    val a = rate.join(movies).map(x => {
      (x._2._2,x._2._1)
    }).sortBy(- _._2).take(10)
    println("最受女性欢迎的10部电影")
    a.foreach(println)
  }

}
