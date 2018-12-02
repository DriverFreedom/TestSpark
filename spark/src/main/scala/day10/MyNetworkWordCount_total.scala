package day10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkWordCount_total {
  def main(args: Array[String]): Unit = {
    //取消日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //设置环境变量
    val conf = new SparkConf().setAppName("StreamWordCountTotal").setMaster("local[2]")
    //创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(3))

    //设置标记点
    ssc.checkpoint("./cpt")
    //创建离散流 DStream代表输入的离散流
    val lines = ssc.socketTextStream("hadoop01",1234)
    //处理分词
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))

    //构建函数
    val updateStateFunc = (currValues:Seq[Int],preValues:Option[Int])=>{
      val curr = currValues.sum
      val pre = preValues.getOrElse(0)
      Some(curr+pre)

    }

    //累加
    val totalvalue = pairs.updateStateByKey(updateStateFunc)

    totalvalue.print()

    ssc.start()

    ssc.awaitTermination()


  }

}
