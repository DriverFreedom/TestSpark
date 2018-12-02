package cn.pig.dmp.report

import cn.pig.dmp.beans.OffLineReport.ReportAreaAnalysis
import cn.pig.dmp.beans.{OffLineReport, SheepString}
import cn.pig.dmp.utils.{ConfigHandler, KpiUtils, MySqlHandler}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 使用广播将appid 和 appname 广播到executor
  * 注意 toMap的使用
  * 使用sparkSql 进行媒体文件的分析
  */
object RptMediaAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("媒体数据").setMaster("local[*]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc  = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //隐式转换 (toDF使用)
    import sqlContext.implicits._
    //广播字典
    val appData = sc.textFile(ConfigHandler.appFile).map(_.split("\t")).filter(_.length>=5)
      .map(x=>(x(4),x(1))).collect().toMap  //记住toMap
    //进行广播
    val broadcast = sc.broadcast(appData)

    //读取数据 并且处理
    val data = sqlContext.read.parquet(ConfigHandler.parquetFile)
      .map(line => {
        val appid = line.getAs[String]("appid")
        var appname = line.getAs[String]("appname")
        if(StringUtils.isEmpty(appname)){
          appname = broadcast.value.getOrElse(appid,appid)
        }
        (appname,KpiUtils.Row2list(line))
      }).reduceByKey((list1,list2) => list1.zip(list2).map(x => x._1+x._2))
      .map(x=>OffLineReport.ReportMeidaAnalysis(x._1,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8)))
      .toDF()//toDF 很重要

    data.show()

    //保存到数据库
    //MySqlHandler.save2db(result,ConfigHandler.RptMediaAnalysis_table)

    sc.stop()





  }
}
