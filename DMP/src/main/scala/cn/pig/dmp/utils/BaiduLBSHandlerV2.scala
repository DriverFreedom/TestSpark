package cn.pig.dmp.utils

import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.security.NoSuchAlgorithmException
import java.util


import com.google.gson.JsonParser
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.lang.StringUtils

/**
  * Created by zhangjingcun on 2018/10/29 9:05.
  */
object BaiduLBSHandlerV2 {

  def main(args: Array[String]): Unit = {
    println(parseBasinessTagsBy("39.983424", "116.322987"))
  }

  /**
    * 解析返回内容信息
    * @param lng
    * @param lat
    */
  def parseBasinessTagsBy(lng:String, lat:String) ={
    var business:String = ""
    val strRequestParams = requestParams(lng, lat)
    val requestUrl = ConfigHandler.lbsDomai.concat(strRequestParams)
    //println(requestUrl)

    //使用HttpClient模拟浏览器发送请求
    val httpClient = new HttpClient()
    val getMethod = new GetMethod(requestUrl)
    val statusCode = httpClient.executeMethod(getMethod)

    //Http OK
    if(statusCode == 200){
      val response = getMethod.getResponseBodyAsString

      //判断是否是合法的json字符串
      var str = response.replaceAll("renderReverse&&renderReverse\\(", "")
      if(!response.startsWith("{")) {
        str = str.substring(0, str.length - 1)
      }

      //解析这个json字符串，取出business节点数据
      val jp = new JsonParser
      val responseDatas = jp.parse(str).getAsJsonObject

      //服务器返回来的json数据，status表示服务器是否正常（0）处理了我的请求
      val status = responseDatas.get("status").getAsInt
      if(status == 0){
        val resultObject = responseDatas.getAsJsonObject("result")
        business = resultObject.get("business").getAsString.replaceAll(",", ";")

        //如果business为空的话， 接着解析该坐标附近的标签信息pois
        if(StringUtils.isEmpty(business)){
          val poisDatas = resultObject.getAsJsonArray("pois")

          var tagSet = Set[String]()
          for (i<-0 until poisDatas.size()){
            val element = poisDatas.get(i).getAsJsonObject
            val tag = element.get("tag").getAsString
            if(StringUtils.isNotEmpty(tag)) tagSet += tag
          }
          business = tagSet.mkString(";")
        }
      }
    }
    business
  }

  /**
    * 构造请求参数
    * @param lng
    * @param lat
    */
  def requestParams(lng:String, lat:String) ={
    val akskArray = ConfigHandler.aksk
    val Array(ak,sk) = akskArray
    val paramsMap = new util.LinkedHashMap[String, String]();
    paramsMap.put("callback", "renderReverse")
    paramsMap.put("location", lng.concat(",").concat(lat))
    paramsMap.put("output", "json")
    paramsMap.put("pois", "1")
    paramsMap.put("ak", ak)

    // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
    val paramsStr = toQueryString(paramsMap)

    // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
    val wholeStr = new String("/geocoder/v2/?" + paramsStr + sk)
    // 对上面wholeStr再作utf8编码
    val tempStr = URLEncoder.encode(wholeStr, "UTF-8") // 调用下面的MD5方法得到最后的sn签名7de5a22212ffaa9e326444c75a58f9a0
    paramsStr.concat("&sn=").concat(MD5(tempStr))
  }

  // 对Map内所有value作utf8编码，拼接返回结果
  @throws[UnsupportedEncodingException]
  def toQueryString(data: util.LinkedHashMap[String, String]): String = {
    val queryString = new StringBuffer
    import scala.collection.JavaConversions._
    for (pair <- data.entrySet) {
      queryString.append(pair.getKey + "=")
      queryString.append(URLEncoder.encode(pair.getValue.asInstanceOf[String], "UTF-8") + "&")
    }
    if (queryString.length > 0) queryString.deleteCharAt(queryString.length - 1)
    queryString.toString
  }

  // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
  def MD5(md5: String): String = {
    try {
      val md = java.security.MessageDigest.getInstance("MD5")
      val array = md.digest(md5.getBytes)
      val sb = new StringBuffer
      var i = 0
      while ( {
        i < array.length
      }) {
        sb.append(Integer.toHexString((array(i) & 0xFF) | 0x100).substring(1, 3))

        i += 1;
        i
      }
      return sb.toString
    } catch {
      case e: NoSuchAlgorithmException =>

    }
    null
  }
}
