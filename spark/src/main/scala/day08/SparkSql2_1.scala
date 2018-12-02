package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object SparkSql2_1 {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    //val session = SparkSession.builder().appName("SparkSql2.1").master("local")
    val session = SparkSession.builder()
      .appName("SQLDemo2x1")
      .master("local[*]").getOrCreate()
    val lines = session.sparkContext.parallelize(List("1,tom,99,29", "2,marry,98,30", "3,jim,98,27"))
    val RowRDD = lines.map(line =>{
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fcValue = fields(2).toDouble
      val age = fields(3).toInt

      Row(id, name, fcValue, age)
    })
    val scheme = StructType(List(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("fv", DoubleType),
      StructField("age", IntegerType)
    ))

    val df = session.createDataFrame(RowRDD,scheme)
    df.createTempView("student")
    val result = session.sql("SELECT name,fv,age FROM student WHERE age>27 ORDER BY fv DESC,age ASC")
    result.show()

  }

}
