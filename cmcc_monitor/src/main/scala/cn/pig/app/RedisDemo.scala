package cn.pig.app

import cn.pig.utils.AppParameters
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisDemo {
  def main(args: Array[String]): Unit = {
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxIdle(5)    //最大的空闲连接，默认为6
    poolConfig.setMaxTotal(2000)  //支持最大的连接数 默认为8

     lazy val jedisPool = new JedisPool(poolConfig,AppParameters.redis_host,6379)
    val jedis = jedisPool.getResource
    jedis.incr("hello");
  }

}
