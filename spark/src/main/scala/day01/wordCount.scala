package day01

import org.apache.spark.{SparkConf, SparkContext}

object wordCount {

  def main(args: Array[String]): Unit = {
    //设置本地运行  2核
    val conf = new SparkConf().setAppName("scalaWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //读取文件
    //val rdd1 = sc.textFile(args(0))
    val rdd1 = sc.textFile("d:/data/word.txt");
    //切分后压平
    val rdd2 = rdd1.flatMap(x=>x.split(" "))
    //将数据都赋值 1
    val rdd3 = rdd2.map((_,1))
    //很据key  进行计算
    val rdd4 = rdd3.reduceByKey(_+_)
    //排序
    val rdd5 = rdd4.sortBy(_._2)
    //生成新文件 保存到hdfs
   // val rdd6 = rdd5.saveAsTextFile(args(1))
    println(rdd5.collect().toBuffer);

    sc.stop()
  }
}
