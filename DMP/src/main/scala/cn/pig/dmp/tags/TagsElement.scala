package cn.pig.dmp.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 标签 关键字
  */
object TagsElement extends Tags {
  /**
    * 定义一个打标签的接口
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //元素解析
    val row = args(0).asInstanceOf[Row]

    val stopWord = args(1).asInstanceOf[Broadcast[Map[String, Int]]]

    val element = row.getAs[String]("keywords").split("\\|")
      .filter(word => word.length>=3 && word.length<=8 && !stopWord.value.contains(word))
        .foreach(word => list :+= ("K"+word,1))

    list

  }
}
