package cn.pig.dmp.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object KpiUtils {
  def Row2list(row :Row)={
    val rmode = row.getAs[Int]("requestmode")
    val pnode = row.getAs[Int]("processnode")
    //原始请求数   有效请求数 广告请求数
    val adRequest =
      if(rmode == 1 && pnode==3) List[Double](1,1,1)
      else if (rmode == 1&& pnode >=2) List[Double](1,1,0)
      else if (rmode == 1 && pnode >=1) List[Double](1,0,0)
      else List[Double](0,0,0)
    val iseffect = row.getAs[Int]("iseffective")
    val isbill = row.getAs[Int]("isbilling")
    val isbid = row.getAs[Int]("isbid")
    val iswin = row.getAs[Int]("iswin")
    val adorderid = row.getAs[Int]("adorderid")

    //参与竞价与竞价成功数
    val adRTB =
      if(iseffect==1 && isbill==1 && isbid==1 && adorderid!=0) List[Double](1,0)
      else if (iseffect==1 && isbill==1 && iswin==1) List[Double](0,1)
      else List[Double](0,0)
    //展示数和点击数
    val show =
      if(rmode==2 && iseffect==1) List[Double](1,0)
      else if(rmode==3 && iseffect==1) List[Double](0,1)
      else List[Double](0,0)
    //Dsp广告消费 DSP广告成本
    val winprice = row.getAs[Double]("winprice")
    val adpayment = row.getAs[Double]("adpayment")
    val ad =
      if(iseffect==1 && isbill==1 && iswin==1) List[Double](winprice/1000,adpayment/1000)
      else List[Double](0,0)

    adRequest++adRTB++show++ad


  }

  val selectAppNameFromRedis=(appid:String,appname:String)=>{
    if(StringUtils.isEmpty(appname)){
      val redis = Jpools.getJedis
      val appname = redis.hget("app_dict",appid).toString
      appname
    }else
      appname
  }

}
