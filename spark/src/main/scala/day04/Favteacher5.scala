package day04

import java.net.URL

import day03.SubjectPartitioner
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Favteacher5 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Favteacher").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d:/data/teacher.log")
    //处理数据
    val word: RDD[((String, String), Int)] = lines.map(line => {
      val teacher = line.substring(line.lastIndexOf("/")+1)
      val url = new URL(line).getHost
      val subject = url.substring(0,url.indexOf("."))
      ((subject,teacher),1)
    })
    //聚合
    // val reduced = word.reduceByKey(_+_)

    //先计算学科的数量
    //将所有学科的名字先在集群中统计计算，然后收集回来
    val subject: Array[String] = word.map(_._1._1).distinct().collect()

    //创建一个自定义分区器，按照学科进行分区， 相同学科的数据都shuffle到一个分区
    val subjectPartitiioner = new SubjectPartitioner(subject)

    //聚合
    //对聚合后的RDD进行自定义分区
    val sbPartitioner = word.reduceByKey(_+_).partitionBy(subjectPartitiioner)
    //重新分区后，在每个分区中进行排序
    val sorted =
      sbPartitioner.mapPartitions(_.toList.sortBy(- _._2).iterator)
    //sorted.saveAsTextFile("d:/data/out/teacher")
    sorted.collect().foreach(println)
  }

}

//自定义分区器
class SubjectPartitioner(subjects:Array[String]) extends Partitioner {
  //在new的时候执行，在构造器中执行
  //String是分区(学科)，Int 是学科的位置
  val rules = new mutable.HashMap[String, Int]()

  var index = 0
  //初始化一个规则
  for (sb <- subjects) {
    rules += ((sb, index))
    index += 1
  }

  //有几个学科返回几个区
  //返回分区的数量
  override def numPartitions: Int = subjects.length

  //根据传入的key,计算返回分区的编号
  //定义一个 计算规则
  override def getPartition(key: Any): Int = {
    //key是一个元组(学科，老师) 将key强制转换成元组
    val tuple = key.asInstanceOf[(String, String)]
    val subject = tuple._1
    rules(subject)
  }
}
