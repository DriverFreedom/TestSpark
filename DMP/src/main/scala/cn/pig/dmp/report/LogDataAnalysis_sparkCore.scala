package cn.pig.dmp.report

import cn.pig.dmp.beans.OffLineReport.ReportLogDataAnalysis
import cn.pig.dmp.utils.{ConfigHandler, FileHandler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LogDataAnalysis_sparkCore {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("LogDataAnalysis")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //读取parquet 文件
    val rowDF = sqlContext.read.parquet(ConfigHandler.parquetFile)
    //处理数据
    val result: RDD[((String, String), Int)] = rowDF.map(line => {
      val provincename =  line.getAs[String]("procincename")
      val cityname = line.getAs[String]("cityname")
      ((provincename,cityname),1)
    }).reduceByKey(_+_)

    //使用隐式转换  进行模式匹配
    import sqlContext.implicits._

    val resultDF = result.map(x => ReportLogDataAnalysis(x._1._1,x._1._2,x._2)).toDF()
    FileHandler.deleteWillOutDir(sc,ConfigHandler.JsonFile)
    resultDF.coalesce(1).write.json(ConfigHandler.JsonFile)

    sc.stop()

  }

}
