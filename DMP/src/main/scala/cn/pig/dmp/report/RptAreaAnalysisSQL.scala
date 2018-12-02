package cn.pig.dmp.report

import cn.pig.dmp.utils.ConfigHandler
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 使用sparkSql 对 区域信息进行分析
  * 难点：
  *     case when then else end 的使用
  *     自定义udf的使用
  */
object RptAreaAnalysisSQL {
  def main(args: Array[String]): Unit = {
    //创建对象
    val conf = new SparkConf()
      .setAppName("RptAreaAnalysis")
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //读取数据
    val dataDF: DataFrame = sqlContext.read.parquet(ConfigHandler.parquetFile)
    //使用sql查询原始数据
    dataDF.registerTempTable("log")
   // dataDF.show()

    /**
      * 自定义UDF
      */
      sqlContext.udf.register("area_func",(boolean:Boolean,result:Double)=>if(boolean) result else 0)

    val result = sqlContext.sql(
      """
        |select provincename,cityname,
        |sum(case when requestmode = 1 and processnode>=1 then 1 else 0 end) rawReq,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) effReq,
        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adReq,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0 then 1 else 0 end) rtbReq,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin =1 then 1 else 0 end) winReq,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) adShow,
        |sum(case when requestmode = 3 and isbilling = 1 and iswin = 1  then winprice/10000 else 0 end) adCost,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) adPayment
        |from log group by provincename,cityname
      """.stripMargin)
    result.show()
    sc.stop()
  }

}
