package cn.pig.dmp.report

import cn.pig.dmp.utils.{ConfigHandler, Jpools, KpiUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 使用SparkSql 进行分析媒体信息的分析
  * 将appname信息保存到redis中，从redis中读取数据，从而完成名字的匹配
  * 重点：
  *       自定义UDf的使用
  *       case when ** then ** else  * end  在sql查询中的使用
  */
object RptMediaAnalysis_redis {
  def main(args: Array[String]): Unit = {

    //创建对象
    val conf = new SparkConf().setAppName("媒体分析").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //读取数据
    val dataDF = sqlContext.read.parquet(ConfigHandler.parquetFile).filter("appid!=null or appname!=null or appname !=''" )
    //val appData = sc.textFile(ConfigHandler.appFile)

    dataDF.registerTempTable("log")
    // dataDF.show()

    /**
      * 自定义UDF
      */
    sqlContext.udf.register("area_func",(boolean:Boolean,result:Double)=>if(boolean) result else 0)
    sqlContext.udf.register("getAppName",KpiUtils.selectAppNameFromRedis)

    val result = sqlContext.sql(
      """
        |select getAppName(appid,appname) appname,
        |sum(area_func(requestmode = 1 and processnode >=1,1)) rawReq,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) effReq,
        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adReq,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0 then 1 else 0 end) rtbReq,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin =1 then 1 else 0 end) winReq,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) adShow,
        |sum(case when requestmode = 3 and isbilling = 1 and iswin = 1  then winprice/10000 else 0 end) adCost,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) adPayment
        |from log group by getAppName(appid,appname)
      """.stripMargin)
    result.show()
    sc.stop()



    /**
      * 将数据保存到redis
      */
    /*val appFile = appData.map(_.split("\t")).filter(_.length>=5).map(x=>(x(4),x(1))).collect()
    val redis = Jpools.getJedis
    appFile.foreach(x =>{
      redis.hset("app_dict",x._1, 4
      14jux._2)
    })
    redis.close()*/
  }

}
