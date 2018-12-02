package day11

import java.sql.DriverManager

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将sparkStreaming的数据都保存到mysql
  */
object SparkStreaming_Mysql_2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val config = ConfigFactory.load()
    val lines = ssc.socketTextStream("hadoop01",12345)
    lines.foreachRDD(rdd => {
      val curr_result = rdd.flatMap(_.split(" ")).filter(!_.isEmpty).map(x => (x,1)).reduceByKey(_+_)
      curr_result.foreachPartition(partition => {
        //创建一个连接
        val url = config.getString("db.url")
        val user = config.getString("db.user")
        val password = config.getString("db.password")
        val conn = DriverManager.getConnection(url,user,password)
        //将当前批次的计算结果都插入到mysql中
        partition.foreach(tp => {
          val word = tp._1
          //判断插入的数据是否存在
          val pstmts = conn.prepareStatement("select * from test where words = ?")
          pstmts.setString(1,word)
          var rs = pstmts.executeQuery()
          var flag = false
          while(rs.next()){
            println(s"${word}在数据库中已经存在")
            flag = true;
            val dbCurrCount = rs.getInt("total")
            val newCount = dbCurrCount + tp._2
            val update = conn.prepareStatement("update test set total = ? where word = ?")
            update.setString(2,word)
            update.setInt(1,newCount)
            update.executeUpdate()
            update.close()
          }
          rs.close()
          pstmts.close()
          if(!flag){
            val stmts = conn.prepareStatement("insert into test values(?,?)")
            stmts.setString(1,word)
            stmts.setInt(2,tp._2)
            stmts.executeUpdate()
            stmts.close()
          }
        })
        if(conn!=null)
          conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
