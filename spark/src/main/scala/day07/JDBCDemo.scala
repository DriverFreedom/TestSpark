package day07

import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCDemo {
  val conn: () => Connection = () =>{
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8","root","123456")
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val jdbcRDD = new JdbcRDD(
      sc,
      conn,
      "select * from logs where id >=? and id <=?",
      1,
      5,
      2,
      rs=>{
        val id = rs.getLong(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      }


    )
    val result = jdbcRDD.collect()
    println(result.toBuffer)
    sc.stop()

  }

}
