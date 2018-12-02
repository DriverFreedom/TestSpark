package utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * 一个简单的redis连接池
  */
object JPools {

  private val poolConfi = new GenericObjectPoolConfig()
  poolConfi.setMaxIdle(5)   //最大的空闲连接  连接池中最大的空闲连接数，默认为8
  poolConfi.setMaxTotal(2000)   //支持最大的连接数  默认为8

  //连接池是私有的 不能对外公开访问
  private lazy val jedisPool = new JedisPool(poolConfi,"hadoop02")

  def getJedis={
    val jedis = jedisPool.getResource
    jedis.select(0)
    jedis
  }
}
