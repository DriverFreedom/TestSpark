package day11

/**
  * 将sparkStreaming统计结果输出到mysql
  * 存在一个缺点  就是 每次插入的数据都是新的，没有计算以前的数据
  * 将在下一个代码中实现
  */

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.typesafe.config.ConfigFactory

object SparkStreaming_Mysql {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[0]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val config = ConfigFactory.load()
    //通过tcp加载数据
    val lines = ssc.socketTextStream("hadoop01",1234)
    lines.foreachRDD(rdd => {
      //创建sparkSession
      val spark = new sql.SparkSession.Builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //rdd转换为DataFrame
      val wordsDataFrame = rdd.flatMap(_.split(" ")).toDF("words")
      //创建视图
      wordsDataFrame.createOrReplaceTempView("t_words")
      //SparkSql语句查询
      var result = spark.sql("select words,count(*) from t_words group by words")
      //封装用户名和口令
      val props = new Properties()
      props.setProperty("user",config.getString("db.user"))
      props.setProperty("password",config.getString("db.password"))
      //写入到mysql数据库
      if(result!=null){
        result.write.mode(SaveMode.Append).jdbc(config.getString("db.url"),config.getString("db.table"),props)
      }


    })

    ssc.start()

    ssc.awaitTermination()
  }
}
