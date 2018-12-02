package cn.pig.dmp.utils





import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/***
  * 删除已经存在的文件夹
  */
object FileHandler {

  def deleteWillOutDir(sc: SparkContext , outPutFile:String)={
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(outPutFile)
    if(fs.exists(path))
      fs.delete(path,true)

  }
}
