package cn.pig.dmp.utils

import redis.clients.jedis.JedisPool

object Jpools {
  private lazy val jedisPool = new JedisPool(ConfigHandler.redis_host,ConfigHandler.redis_port)
  def getJedis = {
    val jedis = jedisPool.getResource
    jedis.select(ConfigHandler.redis_index)
    jedis
  }

}
