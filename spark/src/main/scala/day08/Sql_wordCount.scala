package day08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object Sql_wordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val session = SparkSession.builder().appName("wordCount").master("local[3]").getOrCreate()
    //隐式转换
    import session.implicits._
    val words: Dataset[String] = session.read.textFile("d:\\data\\word.txt")
    val word = words.flatMap(x =>x.split(" "))

    //临时表
    word.createTempView("word")
    //sql计数
    //session.sql("select value words,count(*) counts from word group by words order by counts desc").show()
    //分组计数
    val grouped = word.groupBy("value")
    grouped.count().show()

    session.stop()
  }

}
