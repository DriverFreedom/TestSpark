package day14

import java.util

import org.apache.kafka.common.TopicPartition
import utils.JPools

object JedisOffSet {

  def apply(groupid: String)={
    var fromDbOffset = Map[TopicPartition, Long]()
    val jedis = JPools.getJedis

    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(groupid)
    import scala.collection.JavaConversions._
    val topicPartitionOffsetList: List[(String, String)] = topicPartitionOffset.toList

    for (topicPL <- topicPartitionOffsetList){
      val split: Array[String] = topicPL._1.split("[-]")
      fromDbOffset += (new TopicPartition(split(0), split(1).toInt) -> topicPL._2.toLong)
    }
    fromDbOffset
  }
}
