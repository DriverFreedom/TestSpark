package TestOne

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object No_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\QQFiles\\数据\\大题一\\ml-1m\\ratings.dat")
    val data = lines.map(line => {
      var splited = line.split("::")
      var movieId = splited(1).toInt;
      var movieRate = splited(2).toInt;
      (movieId,movieRate)
    })

   val result = data.groupByKey().map(line => {
     var num = 0;
     var total = 0.0;
     for(r <- line._2){
       num = num +1;
       total = total + r
     }
     val avg = total/num
     (line._1,avg)
   })

    result.sortBy( - _._2).take(10).foreach(println)










  }

}
