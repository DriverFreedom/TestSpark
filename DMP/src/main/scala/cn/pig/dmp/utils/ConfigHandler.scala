package cn.pig.dmp.utils

import java.util.Properties

import com.typesafe.config.ConfigFactory

object ConfigHandler {

  //读取配置文件
  val config = ConfigFactory.load()

  // Pqrquet和Json路径
  val parquetFile = config.getString("parquet_url")
  val JsonFile = config.getString("Json_url")
  //app文件路径
  val appFile = config.getString("app_file")
  val stopwords = config.getString("stopwords")

  //Mysql 的配置
  val driver = config.getString("db.driver")
  val db_url = config.getString("db.url")
  val user = config.getString("db.user")
  val password = config.getString("db.password")
  val LogDataAnalysis_table = config.getString("db.LogDataAnalysis.table")
  val RptAreaAnalysis_table = config.getString("db.RptAreaAnalysis.table")
  val RptMediaAnalysis_table = config.getString("db.RptMediaAnalysis.table")

  //封装Mysql
  val props = new Properties()
  props.setProperty("driver",driver)
  props.setProperty("user",user)
  props.setProperty("password",password)

  //封装redis
  val redis_host = config.getString("redis.host")
  val redis_port = config.getInt("redis.port")
  val redis_index = config.getInt("redis.index")

  //解析百度域名
  val lbs=config.getString("baidu.domain")
  //返回aksk
  val aksk=config.getString("baidu.aksk").split(",")

  object lbsDomai {
    def concat(strRequestParams: String) = {
      lbs+strRequestParams
    }

  }

}
