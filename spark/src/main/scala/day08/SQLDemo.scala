package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo {

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
      Student1(id,name,fv,age)
    })

    //将RDD转换成DataFrame
    //导入隐式转换
    import sqlC.implicits._
    val df = studentRDD.toDF
    //对DataFrame 进行操作
    //使用sql风格的API
    df.registerTempTable("student")
    //sql是一个transformartion
    val result = sqlC.sql("select name,fv from student order by fv desc,age desc")
    //触发action
    result.show()
    sc.stop()


  }
}

/**
  * case class 将数据保存到case class
  * case class 的特点：不用new ；实现序列化；模式匹配
  * @param id
  * @param name
  * @param fv
  * @param age
  */
case class Student1(id:Long,name:String,fv:Double,age:Int)