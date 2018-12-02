package cn.pig.dmp.report

import cn.pig.dmp.utils.{ConfigHandler, FileHandler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object LogDataAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("LogDataAnalysis")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //读取parquet 文件
    val rowDF = sqlContext.read.parquet(ConfigHandler.parquetFile)
    rowDF.registerTempTable("log")
    val result = sqlContext.sql("select count(*) count,provincename,cityname from log group by provincename,cityname")

    //以json格式输出（设置只输出一个文件）
    FileHandler.deleteWillOutDir(sc,ConfigHandler.JsonFile)
    result.coalesce(1).write.json(ConfigHandler.JsonFile)

    //写到mysql 数据库中
    result.write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.db_url,ConfigHandler.LogDataAnalysis_table,ConfigHandler.props)
    sc.stop()

  }

}
