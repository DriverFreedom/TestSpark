package cn.pig.dmp.tags

import org.apache.spark.sql.Row

/**
  * 广告相关
  */
object TagsAd extends Tags {
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
    //广告位类型
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v>9 => list :+= ("LC"+v,1)
      case v if v>0 && v<10 => list :+= ("LC0"+v,1)
    }
    //渠道
    val channel = row.getAs[Int]("adplatformproviderid")
    if(channel!=0) list :+= ("CN"+channel,1)

    list
  }
}
