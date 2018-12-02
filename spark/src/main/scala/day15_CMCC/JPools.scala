package day15_CMCC

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * 简单的Redis连接池 用于读取，存放数据
  */
object  Jpools{
  private val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxIdle(5)    //最大的空闲连接，默认为6
  poolConfig.setMaxTotal(2000)  //支持最大的连接数 默认为8

  private lazy val jedisPool = new JedisPool(poolConfig,"hadoop02")
  def getJedis = {
    val jedis = jedisPool.getResource()
    jedis.select(0)
    jedis
  }
}
