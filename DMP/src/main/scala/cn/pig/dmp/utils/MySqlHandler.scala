package cn.pig.dmp.utils

import org.apache.spark.sql.{DataFrame, SaveMode}
//import scalikejdbc._
//import scalikejdbc.config._

import scala.collection.mutable.ListBuffer

object MySqlHandler {

  def save2db(df:DataFrame,dbname:String,partition:Int = 1)={
    df.coalesce(partition).write.mode(SaveMode.Overwrite).jdbc(ConfigHandler.db_url,dbname,ConfigHandler.props)
  }

 /* def saveBussines(list:ListBuffer[(String,String)])={
    DBs.setup()
    DB.localTx(implicit session =>
      list.foreach(tp => {
        SQL("replace into business values(?,?)").bind(tp._1,tp._2)
          .update().apply()
      })
    )
  }*/
}
