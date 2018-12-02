package day10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkWordCount {

  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    //创建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]")
    //定义一个采样时间 每隔2秒钟采集一次数据
    val ssc = new StreamingContext(sparkConf,Seconds(2))
    //创建一个离散流 DStream代表输入的数据流
    val lines = ssc.socketTextStream("hadoop01",1234)

    //处理数据
    val words = lines.flatMap(_.split(" "))
    val result = words.map(x => (x,1)).reduceByKey(_+_)

    //输出结果
    result.print()

    ssc.start()

    ssc.awaitTermination()



  }
}
