package cn.pig.dmp.report

import cn.pig.dmp.beans.OffLineReport.ReportAreaAnalysis
import cn.pig.dmp.utils.{ConfigHandler, KpiUtils, MySqlHandler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RptAreaAnalysis {
  def main(args: Array[String]): Unit = {
    //创建对象
    val conf = new SparkConf().setAppName("RptAreaAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //读取数据
    val file = sqlContext.read.parquet(ConfigHandler.parquetFile)

    val result = file.map(line => {
      val province = line.getAs[String]("provincename")
      val city = line.getAs[String]("cityname")
      ((province,city),KpiUtils.Row2list(line))
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(x => x._1+x._2)
    }).map(x=>ReportAreaAnalysis(x._1._1,x._1._2,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))).toDF()

    MySqlHandler.save2db(result,ConfigHandler.RptAreaAnalysis_table)
    sc.stop()

  }

}
