package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object SQLDemo_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("SQLDemo1")
    val sc = new SparkContext(conf)
    //SQLContext是对SparkContext的一个包装 (增强了功能，可以处理结构化数据)
    val sqlC = new SQLContext(sc)

    //Dataframe= RDD + Schema
    val lines = sc.parallelize(List("1,tom,99,29","2,marry,98,30","3,jim,98,27"))
    val studentRDD = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fv = fields(2).toDouble
      val age = fields(3).toInt
      Row(id,name,fv,age)
    })

    //不是用对象  创建一个scheme
    //创建一个scheme(元数据)
    val scheme = StructType(
      List(
        StructField("id",LongType),
        StructField("name",StringType),
        StructField("fv",DoubleType),
        StructField("age",IntegerType)

      )
    )
    //RDD关联scheme
    val df = sqlC.createDataFrame(studentRDD,scheme)
    //使用DSL语法 调用DataFrame
    //select是一个tarnsformation
    val selected = df.select("name","fv","age")
    //排序
    //导入隐式转换
    import sqlC.implicits._
    val result = selected.orderBy($"fv" desc ,$"age" asc)
    result.show()
    sc.stop()
  }



}
