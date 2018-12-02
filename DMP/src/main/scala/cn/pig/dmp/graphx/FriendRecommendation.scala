package cn.pig.dmp.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求共同好友（好友推荐）
  */
object FriendRecommendation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("好友推荐")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    //step1  构建点集合和边集合
    val vertices: RDD[(VertexId, (String, Int))] = sc.makeRDD(Seq(
      (1L,("张飞",13)),
      (2L,("关羽",13)),
      (6L,("刘备",13)),

      (99L,("曹操",13)),
      (123L,("赵云",13)),
      (110L,("孙策",13)),

      (144L,("周瑜",13)),
      (250L,("大乔",13)),
      (290L,("小乔",13))
    ))

    //边集合
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1,100,0),
      Edge(2,100,0),
      Edge(6,100,0),

      Edge(99,200,0),
      Edge(110,200,0),
      Edge(123,200,0),
      Edge(6, 200, 0),

      Edge(144,300,0),
      Edge(250,300,0),
      Edge(290,300,0)

    ))

    //step2 构建图对象
    val graph = Graph(vertices,edgeRDD)

    //step3 调用图对象的APi
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //将可能认识的人放在一起
    /**
      * 存在重复的id
      * 需要输出用户名
      */
    //cc.map(tp =>(tp._2,tp._1.toString)).reduceByKey((a,b) => a.concat(",").concat(b)).foreach(println(_))
    cc.join(vertices).map(x => (x._2._1,x._2._2._1)).reduceByKey((a,b)=>a.concat(",").concat(b)).map(x =>x._2).foreach(println(_))

    sc.stop()
  }

}
