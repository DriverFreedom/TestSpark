package cn.pig.dmp.graphx

import cn.pig.dmp.tags._
import cn.pig.dmp.utils.{ConfigHandler, TagsHandler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 借助图计算的联通算法实现多个渠道用户身份识别问题
  */
object TagsTodayFinal {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf()
      .setAppName("图计算实现多渠道用户身份识别")
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



    //构造基础RDD
    val baseRdd: RDD[(List[String], Row)] = rawDataFrame.filter(TagsHandler.hadNeedOneUserId).map(row=>{
      //获取当前行上的所有的用户的非空id
      val allUserIds = TagsHandler.getCurrentRowAllUserId(row)

      (allUserIds,row)
    })

    //构造点集合
    val vertiesRDD: RDD[(Long, Seq[(String, Int)])] = baseRdd.flatMap(tp => {
      val userList = tp._1
      val row = tp._2

      //广告位类型
      val adType = TagsAd.makeTags(row)
      //地域类型
      val areaType = TagsArea.makeTags(row)
      //设备类型
      val deviceType = TagsDevice.makeTags(row)
      //appName
      val appType = TagsAppName.makeTags(row, appBC)
      //关键字
      val keyword = TagsElement.makeTags(row, stopWordsBC)
      //当前所打上的标签
      val currentRowTag = adType ++ areaType ++ deviceType ++ appType ++ keyword

      val VD: Seq[(String, Int)] = userList.map((_, 0)) ++ currentRowTag

      /**
        * 只有对第一个人可以携带定点ID 其他的都不需要携带
        * 如果 在同一行的多个 定点都携带了Vd 因为同一行的数据属于同一个用户 将来肯定会聚合到一起  这样会造成数据的重复累加
        */
      userList.map(uid => {
        if (userList.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })

    //构造边集合
    val edgeRdd = baseRdd.flatMap(tp=>{
      tp._1.map(uId => Edge(tp._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })

    //构造图对象
    val graph = Graph(vertiesRDD,edgeRdd)

    //调用图计算Api
    val cc = graph.connectedComponents().vertices

    cc.join(vertiesRDD).map{
      case (uId,(commonId,tags)) => (commonId,tags)
    }.reduceByKey((list1,list2)=>(list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList).foreach(println(_))

    sc.stop()
  }

}
