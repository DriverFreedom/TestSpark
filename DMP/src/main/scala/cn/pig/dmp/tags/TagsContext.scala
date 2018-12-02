package cn.pig.dmp.tags

import cn.pig.dmp.utils.{ConfigHandler, TagsHandler}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 标签数据化
  *   上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("数据进行标签化处理")
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    //读取app名称数据
    val appData: Map[String, String] = sc.textFile(ConfigHandler.appFile).map(_.split("\t")).filter(_.length>=5).map(x=>(x(4),x(1))).collect().toMap
    //读取非关键字类型
    val stopwords = sc.textFile(ConfigHandler.stopwords).map((_,0)).collect().toMap
    //广播 stopwords
    val stopWordsBC: Broadcast[Map[String, Int]] = sc.broadcast(stopwords)
    //将appData进行广播
    val appBC: Broadcast[Map[String, String]] = sc.broadcast(appData)
    val sqlContext = new SQLContext(sc)
    //读取数据
    val rawDataFrame = sqlContext.read.parquet(ConfigHandler.parquetFile)
    //过滤出来至少含有一个用户id的数据
    val filteredDF = rawDataFrame.filter(TagsHandler.hadNeedOneUserId)

    //代码逻辑实现
    val result = filteredDF.map(row =>{
      //获取用户ID
      val userId = TagsHandler.getAnyOneUserId(row)
      //广告位类型
      val adType = TagsAd.makeTags(row)
      //地域类型
      val areaType = TagsArea.makeTags(row)
      //设备类型
      val deviceType = TagsDevice.makeTags(row)
      //appName
      val appType = TagsAppName.makeTags(row,appBC)
      //关键字
      val keyword = TagsElement.makeTags(row,stopWordsBC)


      (userId,adType ++ areaType ++ deviceType ++ appType ++ keyword)
    }).reduceByKey{
      (list1,list2)=>(list1 ++ list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }

    result.map(tp=>tp._1 +"\t"+ tp._2.map(x => x._1+":"+x._2).mkString(",")).foreach(println)

    //result.foreach(println(_))

    sc.stop()
  }

}
