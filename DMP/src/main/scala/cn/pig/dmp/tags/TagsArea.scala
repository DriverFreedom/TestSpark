package cn.pig.dmp.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsArea extends Tags {
  /**
    * 定义一个打标签的接口
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    //获取参数
    val row = args(0).asInstanceOf[Row]
    val province = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if(StringUtils.isNotEmpty(province))
      list :+= ("ZP"+province,1)
    if(StringUtils.isNotEmpty(cityname))
      list :+= ("ZC"+cityname,1)

    list
  }
}
