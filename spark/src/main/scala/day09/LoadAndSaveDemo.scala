package day09

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
object LoadAndSaveDemo {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val jdbcDL: DataFrame = spark.read.format("jdbc").options(Map(
      "url" -> "jdbc:mysql://localhost:3306/test",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "user",
      "user" -> "root",
      "password" -> "123456"
    )).load()
    //jdbcDL.show()

    jdbcDL.select("name").show()
    //jdbcDL.write.format("json").save("d:/data/save/out1")
    //jdbcDL.write.save("")
    //存储到jdbc
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    //jdbcDL.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test","user2",prop)

    spark.stop()
  }

}
