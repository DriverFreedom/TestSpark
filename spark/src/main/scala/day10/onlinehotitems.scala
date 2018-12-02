package day10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 背景描述：
  *   在社交网络（微博），电子商务（京东）、搜索引擎（百度）、股票交易中人们关心的内容之一是我所关注的内容中，大家正在关注什么
  *   在实际企业中非常有价值
  *   例如：我们关注过去30分钟大家都在热搜什么？并且每5分钟更新一次。要求列出来搜索前三名的话题内容
  *
  *   数据格式：
  *     hadoop,201810080102
  *     spark,201810080103
  *
  *    问题：
  *       下述代码每隔20秒回重新计算之前60秒内的所有数据，如果窗口时间间隔太长，那么需要重新计算的数据就比较大，非常耗时
  *    解决：
  *       searchPair.reduceByKeyAndWindow((v1:Int, v2:Int) => v1+v2, (v1:Int, v2:Int) => v1-v2, Seconds(60), Seconds(20))
  *
  * Created by zhangjingcun on 2018/10/8 16:11.
  */
object onlinehotitems {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val hottestStream = ssc.socketTextStream("hadoop01",1234)
    /**
      * 用户搜索的格式简化为item,time  在这里我们由于要计算出热点内容，所以只需要取出item即可
      * 提取出的item然后通过map转换为(item,1)格式
      */
    val searchPair: DStream[(String, Int)] = hottestStream.map(_.split(",")(0)).filter(!_.isEmpty).map(x => (x,1))
    val hottestDStream = searchPair.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2,(v1:Int, v2:Int) => v1-v2, Seconds(60), Seconds(20))
    val result: DStream[(String, Int)] = hottestDStream.transform(hottestRDD => {
      val top3: Array[(String, Int)] = hottestRDD.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1)).take(3)
      ssc.sparkContext.makeRDD(top3)
    })

    result.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
