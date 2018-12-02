package day15_CMCC

import org.apache.kafka.common.TopicPartition

object JedisOffSet {

  def apply(GroupId:String)={

    var fromDbOffSet = Map[TopicPartition,Long]()
    val jedis = Jpools.getJedis
    val topicPartition = jedis.hgetAll(GroupId)
    import scala.collection.JavaConversions._
    val topicPartitionList = topicPartition.toList
    for(x <- topicPartitionList){
      val split = x._1.split("-")
      fromDbOffSet += (new TopicPartition(split(0),split(1).toInt) -> x._2.toLong)
    }
    fromDbOffSet
  }
}
