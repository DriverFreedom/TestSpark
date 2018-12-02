package cn.pig.dmp.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FriendRecommendationV2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("好友推荐")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val file: RDD[Array[String]] = sc.textFile("D:\\data\\friends.txt").map(_.split(" "))

    //构建点集合
    val vertices = file.flatMap(nameArray =>{
      nameArray.map(name => (name.hashCode().toLong,name))
    })

    //构建边集合
    val edgeRdd = file.flatMap(arrayName => {
      val head = arrayName.head.hashCode.toLong
      arrayName.map(name => Edge(head, name.hashCode.toLong, 0))
    })

    //构建图对象
    val graph = Graph(vertices,edgeRdd)
    //调用Api
    val cc = graph.connectedComponents().vertices

    cc.join(vertices).map(x=> (x._2._1,x._2._2)).reduceByKey((a,b)=>a.concat(",").concat(b)).map(x=>x._2).foreach(println(_))


  }


}
