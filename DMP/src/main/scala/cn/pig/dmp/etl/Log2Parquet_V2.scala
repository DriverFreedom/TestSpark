package cn.pig.dmp.etl


import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import cn.pig.dmp.beans.Log
import org.apache.spark.rdd.RDD
import cn.pig.dmp.utils.FileHandler

/**
  * 读取日志 转换成parquet格式进行保存
  */
object Log2Parquet_V2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println(
      """
        |cn.pig.dmp.etl.Log2Parquet_V2
        |参数 InputFile outPutFile
      """.stripMargin)

    sys.exit()
    }

    //模式匹配
    val Array(dataInputFile, outPutFile) = args

    val sparkConf = new SparkConf()
      .setAppName("Log2Parquet_V2")
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建 sparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //设置parquet文件写出的压缩方式
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    //读取数据
    val rowData: RDD[String] = sc.textFile(dataInputFile)
    //val file: DataFrame = sqlContext.read.load(dataInputFile)

    //处理数据(如果满足85个字段则视为正确数据)
    val arrRdd = rowData.map(line=>line.split(","))
      //过滤掉不符合字段要求的数据
      .filter(_.length >= 85)
    val logRdd: RDD[Log] = arrRdd.map(Log(_))

    val dataFrame: DataFrame = sqlContext.createDataFrame(logRdd)

    FileHandler.deleteWillOutDir(sc, outPutFile)

    //保存数据
    dataFrame.write.parquet(outPutFile)

    //关闭SparkContent
    sc.stop()

  }


}
