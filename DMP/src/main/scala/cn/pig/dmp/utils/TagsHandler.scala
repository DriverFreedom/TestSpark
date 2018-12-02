package cn.pig.dmp.utils

import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 选择有效的数据 打标签
  */
object TagsHandler {
  /**
    * 至少有一个id不能为空
    * @return
    */
  def hadNeedOneUserId={
    """
      |imei!='' or mac !='' or idfa!='' or openudid!='' or androidid !='' or
      |imeimd5!='' or macmd5!='' or idfamd5!='' or openudidmd5!='' or androididmd5 !='' or
      |imeisha1!='' or macsha1!='' or idfasha1!='' or openudidsha1 !='' or androididsha1!=''
    """.stripMargin
  }
  /*
  从一行数据中获取用户的某一个不为空的id
   */
  def getAnyOneUserId(row :Row)={
    row match {
      case v if StringUtils.isNotEmpty(v.getAs[String]("imei")) =>"IM:"+v.getAs[String]("imei")
      case v if StringUtils.isNotEmpty(v.getAs[String]("mac")) =>"IM:"+v.getAs[String]("mac")
      case v if StringUtils.isNotEmpty(v.getAs[String]("idfa")) =>"ID:"+v.getAs[String]("idfa")
      case v if StringUtils.isNotEmpty(v.getAs[String]("androidid")) =>"AD:"+v.getAs[String]("androidid")
      case v if StringUtils.isNotEmpty(v.getAs[String]("openudid")) =>"OU:"+v.getAs[String]("openudid")

      case v if StringUtils.isNotEmpty(v.getAs[String]("imeimd5")) =>"IMM:"+v.getAs[String]("imeimd5")
      case v if StringUtils.isNotEmpty(v.getAs[String]("macmd5")) =>"IMM:"+v.getAs[String]("macmd5")
      case v if StringUtils.isNotEmpty(v.getAs[String]("idfamd5")) =>"IDM:"+v.getAs[String]("idfamd5")
      case v if StringUtils.isNotEmpty(v.getAs[String]("openudidmd5")) =>"ADM:"+v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotEmpty(v.getAs[String]("androididmd5")) =>"OUM:"+v.getAs[String]("androididmd5")

      case v if StringUtils.isNotEmpty(v.getAs[String]("imeisha1")) =>"IMS:"+v.getAs[String]("imeisha1")
      case v if StringUtils.isNotEmpty(v.getAs[String]("macsha1")) =>"IMS:"+v.getAs[String]("macsha1")
      case v if StringUtils.isNotEmpty(v.getAs[String]("idfasha1")) =>"IDS:"+v.getAs[String]("idfasha1")
      case v if StringUtils.isNotEmpty(v.getAs[String]("openudidsha1")) =>"ADS:"+v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotEmpty(v.getAs[String]("androididsha1")) =>"OUS:"+v.getAs[String]("androididsha1")
    }

  }


  //获取全部的用户标识ID
  def getCurrentRowAllUserId(row:Row)={
    var list = List[String]()
    if(StringUtils.isNotEmpty(row.getAs[String]("imei"))) list :+= "IM:"+row.getAs[String]("imei")
    if(StringUtils.isNotEmpty(row.getAs[String]("mac"))) list :+= "IM:"+row.getAs[String]("mac")
    if(StringUtils.isNotEmpty(row.getAs[String]("idfa"))) list :+= "ID:"+row.getAs[String]("idfa")
    if(StringUtils.isNotEmpty(row.getAs[String]("androidid"))) list :+= "AD:"+row.getAs[String]("androidid")
    if(StringUtils.isNotEmpty(row.getAs[String]("openudid"))) list :+= "OU:"+row.getAs[String]("openudid")

    if(StringUtils.isNotEmpty(row.getAs[String]("imeimd5"))) list :+= "IMM:"+row.getAs[String]("imeimd5")
    if(StringUtils.isNotEmpty(row.getAs[String]("macmd5"))) list :+= "IMM:"+row.getAs[String]("macmd5")
    if(StringUtils.isNotEmpty(row.getAs[String]("idfamd5"))) list :+= "IDM:"+row.getAs[String]("idfamd5")
    if(StringUtils.isNotEmpty(row.getAs[String]("openudidmd5"))) list :+= "ADM:"+row.getAs[String]("openudidmd5")
    if(StringUtils.isNotEmpty(row.getAs[String]("androididmd5"))) list :+= "OUM:"+row.getAs[String]("androididmd5")

    if(StringUtils.isNotEmpty(row.getAs[String]("imeisha1"))) list :+= "IMS:"+row.getAs[String]("imeisha1")
    if(StringUtils.isNotEmpty(row.getAs[String]("macsha1"))) list :+= "IMS:"+row.getAs[String]("macsha1")
    if(StringUtils.isNotEmpty(row.getAs[String]("idfasha1"))) list :+= "IDS:"+row.getAs[String]("idfasha1")
    if(StringUtils.isNotEmpty(row.getAs[String]("openudidsha1"))) list :+= "ADS:"+row.getAs[String]("openudidsha1")
    if(StringUtils.isNotEmpty(row.getAs[String]("androididsha1"))) list :+= "OUS:"+row.getAs[String]("androididsha1")

    list
  }
}
