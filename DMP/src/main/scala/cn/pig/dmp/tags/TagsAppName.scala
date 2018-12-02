package cn.pig.dmp.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsAppName extends Tags {
  /**
    * 定义一个打标签的接口
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //参数解析
    val row = args(0).asInstanceOf[Row]
    val appBC = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    if(StringUtils.isNotEmpty(appname))
      list :+= (appname,1)
    else if(StringUtils.isNotEmpty(appid))
      list :+= (appBC.value.getOrElse(appid,appid),1)
    list
  }
}
