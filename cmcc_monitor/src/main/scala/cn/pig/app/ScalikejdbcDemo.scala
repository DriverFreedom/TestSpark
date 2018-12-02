package cn.pig.app

import scalikejdbc.config.DBs
import scalikejdbc._

object ScalikejdbcDemo {

  def main(args: Array[String]): Unit = {
    DBs.setup()
    def select() = {
      DB.readOnly { implicit session =>

        SQL("select * from wordcount").map(rs =>
          (rs.string("words"), rs.int("num"))).list().apply()
      }
    }
    for (s <- select){
      print(s)
    }

  }


}
