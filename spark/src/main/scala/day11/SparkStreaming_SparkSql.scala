package day11

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_SparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val lines = ssc.socketTextStream("hadoop01",1234)
    val words = lines.flatMap(_.split(" "))

    //使用sparkSql老查询SparkStreaming的流式数据
    words.foreachRDD(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val wordsDataFrame = rdd.toDF("words")
      wordsDataFrame.createOrReplaceTempView("t_words")
      val result = spark.sql("select words,count(*) as total from t_words group by words")
      result.show()
    })

    ssc.start()

    ssc.awaitTermination()

  }

}
