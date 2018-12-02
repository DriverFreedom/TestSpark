package day07

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val users = Array("1,tom,18,100","2,lisa,19,500","3,lisi,30,50","4,zhangsan,25,100")
    val userLines = sc.makeRDD(users)
    val userRDD = userLines.map(line => {
      val fields = line.split(",")
      val num = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val rate = fields(3).toInt
      new User(num,name,age,rate)

    })

    //排序
    val sorted = userRDD.sortBy(x => x)
    println(sorted.collect().toBuffer)
  }


}

class User(val num:Int,val name:String,val age:Int,val rate:Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if(this.rate == that.rate){
      this.age-that.age
    }else{
      that.rate-this.rate
    }
  }

  override def toString: String = {
    s"user:$num,$name,$age,$rate"
  }
}
