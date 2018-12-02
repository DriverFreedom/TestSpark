package day10

/**
  * 词频统计  获得以前运算的数据，继续运算
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkWordCount_total_2 {

  val ckp = "./ckp"

  /**
    * 函数 累计单词的数量
    */
  def updateFunction(newValues:Seq[Int],runningCount:Option[Int]) = {
    val currentTotal = newValues.sum
    //执行累加操作：如果是第一次执行（单词第一次出现，则没有之前的值）
    val totalValues = runningCount.getOrElse(0)
    Some(currentTotal + totalValues)
  }

  /**
    * 从checkpoint目录中回复上一个job的scc实例 否则创建一个新的scc
    * @return
    */
  def funcToCreateContext():StreamingContext={

    println("create new scc.......")
    //创建StreamingContext对象
    val spqrkConf = new SparkConf().setAppName("").setMaster("local[2]")
    //定义采样时间
    val ssc = new StreamingContext(spqrkConf,Seconds(2))
    //设置检查点目录
    ssc.checkpoint(ckp)
    //创建一个离散流 Dstream代表输入的数据流
    val lines = ssc.socketTextStream("hadoop01",1234)
    //设置checkpoint 默认每10秒做一次checkpoint
    lines.checkpoint(Seconds(6))
    //处理分词 ，每个单词记一次数
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x,1))
      //累加
    val totalResult = pairs.updateStateByKey(updateFunction _)
    totalResult.print()

    ssc
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val ssc = StreamingContext.getOrCreate(ckp,funcToCreateContext _)
    ssc.start()

    ssc.awaitTermination()

  }
}
