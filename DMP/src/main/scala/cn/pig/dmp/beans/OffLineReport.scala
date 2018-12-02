package cn.pig.dmp.beans

object OffLineReport {

  case class ReportLogDataAnalysis(provinceName:String, cityName:String, ct:Int)

  case class ReportAreaAnalysis(provinceName: String,
                                 cityName: String,
                                 rawReq: Double,
                                 effReq: Double,
                                 adReq: Double,
                                 rtbReq: Double,
                                 winReq: Double,
                                 adShow: Double,
                                 adClick: Double,
                                 adCost: Double,
                                 adPayment: Double
                               )
  case class ReportMeidaAnalysis(appname: String,
                                rawReq: Double,
                                effReq: Double,
                                adReq: Double,
                                rtbReq: Double,
                                winReq: Double,
                                adShow: Double,
                                adClick: Double,
                                adCost: Double,
                                adPayment: Double
                               )
}
