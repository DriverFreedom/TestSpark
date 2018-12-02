package cn.pig.dmp.etl

import ch.hsr.geohash.GeoHash
import cn.pig.dmp.utils.{BaiduLBSHandlerV2, ConfigHandler, MySqlHandler}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

object LogLat2Business {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setAppName("将日志文件中的经纬信息抽取出来 保存到数据库")
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建 sparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val rawdataFrame = sqlContext.read.parquet(ConfigHandler.parquetFile)
    //中国的经纬度范围 纬度（3.86—53.55） 经度（73.66-135.05）
    rawdataFrame.select("long","lat").filter(
      """
        |cast(long as double) >=73 and cast(long as double)<=136 and
        |cast(long as double) >=3 and cast(long as double)<=54
      """.stripMargin).distinct()
      .foreachPartition(iter =>{
        val list = new ListBuffer[(String,String)]()
        iter.foreach(row => {
          val long = row.getAs[String]("long")
          val lat = row.getAs[String]("lat")
          //传递经度和纬度  返回商圈信息
          val bussibess = BaiduLBSHandlerV2.parseBasinessTagsBy(long,lat)
          val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
          if(StringUtils.isNotEmpty(bussibess))
            list.append((geoHashCode,bussibess))
        })

        //MySqlHandler.saveBussines(list)
        list.foreach(println(_))
      })



    sc.stop()


  }
}
