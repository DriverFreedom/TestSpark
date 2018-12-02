package cn.pig.utils
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool


/**
  * Redis数据库池
  */
object Jpools {
  private val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxIdle(5)    //最大的空闲连接，默认为6
  poolConfig.setMaxTotal(2000)  //支持最大的连接数 默认为8

  private lazy val jedisPool = new JedisPool(poolConfig,AppParameters.redis_host,6379)

  def getJedis = {
    val jedis = jedisPool.getResource()
    jedis.select(AppParameters.redis_index)
    jedis
  }

}
